from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from hivemind.tools.discovery import (
    handle_get_partitions,
    handle_get_table_schema,
    handle_get_table_stats,
)
from hivemind.tools.explain_plan import (
    hms_rows_for_table,
    max_hms_total_rows,
    pick_focus_table,
    partition_keys_for_table,
)
from hivemind.tools.optimize import _extract_tables_from_sql
from hivemind.tools.sql_gen import (
    _is_write_operation_request,
    _parse_columns_and_partitions,
    _parse_row_counts,
    _parse_tables,
    _strip_backticks,
)

if TYPE_CHECKING:
    from hivemind.hms_client import HMSClient
    from hivemind.hs2_client import HS2Client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers — partition filter extraction
# ---------------------------------------------------------------------------

def _extract_partition_filter_values(sql: str, partition_keys: list[str]) -> dict[str, str]:
    """
    Return {partition_key: literal_value} for equality predicates found in sql.

    Handles both quoted and unquoted literals:
      WHERE ss_sold_date_sk = 2450816
      WHERE sale_date = '2026-05-11'
    """
    result: dict[str, str] = {}
    for pk in partition_keys:
        # Match  pk = value  or  pk = 'value'  (case-insensitive, spaces optional)
        pattern = re.compile(
            rf"\b{re.escape(pk)}\s*=\s*'?([^'\s,)(]+)'?",
            re.IGNORECASE,
        )
        m = pattern.search(sql)
        if m:
            result[pk] = m.group(1).strip("'\"")
    return result


def _lookup_partition_sample_rows(context: str, pkey: str, value: str) -> int | None:
    """
    Find the HMS partition sample row count for a specific partition value.

    Matches lines produced by get_partitions, e.g.:
      ss_sold_date_sk=2450816  rows=88,103  files=1  size=3.1 MB
      sale_date=2026-05-11  rows=500  files=1  size=7.7 KB
    """
    pattern = re.compile(
        rf"\b{re.escape(pkey)}={re.escape(value)}\s+rows=([\d,]+)",
        re.IGNORECASE,
    )
    m = pattern.search(context)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_COMPARE_SYSTEM_PROMPT = """\
You are a Hive query comparison expert inside HiveMind, an MCP server connected
to a live Apache Hive Metastore (HMS) on Cloudera CDP (Hive 3.x).

Your job is to compare two HiveQL queries — typically an original (slower) version
and an optimized (faster) rewrite — using their EXPLAIN plans and HMS metadata.

You never execute the queries or fabricate numbers.  Every claim must come from
the PLAN COMPARISON block or the HMS metadata provided below.

---

## HOW TO PRESENT THE COMPARISON

Use the PLAN COMPARISON block below as your primary source of truth.

### Section 1 — Overview

Write 2–3 sentences summarising:
- What both queries do (they should be logically equivalent)
- What the key structural difference is between them (partition filter added,
  join order swapped, function on partition column removed, etc.)

### Section 2 — Plan Metrics

Present the key numbers from the PLAN COMPARISON block in plain English:

- Rows scanned before and after, with the percentage change
- Whether partition pruning is active in each plan
- Join strategy change, if any
- Per-table scan breakdown when multiple tables are involved

Always cite the actual numbers from the PLAN COMPARISON block.
If CBO estimates are unavailable, say so explicitly — never fabricate.

### Section 3 — Verdict

A clear, direct judgment: is the optimized query materially better?

When TableScan row estimates are available in PLAN COMPARISON, use:
- SIGNIFICANT IMPROVEMENT : rows scanned dropped by ≥50% on the focus table
- MINOR IMPROVEMENT       : rows scanned dropped by 10–49%
- NO IMPROVEMENT          : scan rows unchanged or the plans are equivalent
- REGRESSION              : the rewrite scans more rows than the original

When row estimates are "unknown", you MUST NOT label SIGNIFICANT or MINOR
IMPROVEMENT.  Use QUALITATIVE IMPROVEMENT only if the optimized query adds a
direct partition-key filter (visible in SQL or PLAN COMPARISON pruning verdict);
otherwise use INCONCLUSIVE.  You may cite HMS partition sample row counts only
as illustrative context, not as CBO scan estimates.

State which query to use and why, in one sentence.

### Section 4 — Next Steps (optional)

Only include this section when the comparison reveals further opportunities,
for example:
- Neither plan applies a partition filter — both are full scans
- A better rewrite is still possible (e.g. replace a LIKE with an equality filter)
- HMS statistics are missing — recommend running ANALYZE TABLE for exact numbers

---

## OUTPUT FORMAT

QUERY COMPARISON REPORT
═══════════════════════════════════════════════════════
OVERVIEW
{2–3 sentences}
───────────────────────────────────────────────────────
PLAN METRICS
Original query   : {rows scanned on focus table, or "unknown"}
Optimized query  : {rows scanned on focus table, or "unknown"}
Row reduction    : {delta % or "unknown"}
Partition pruning: {Original: Yes/No  →  Optimized: Yes/No}
Join strategy    : {Original: …  →  Optimized: … (or "unchanged" / "N/A")}
───────────────────────────────────────────────────────
VERDICT
{SIGNIFICANT IMPROVEMENT / MINOR IMPROVEMENT / NO IMPROVEMENT / REGRESSION}
{One sentence: which query to use and why}
───────────────────────────────────────────────────────
NEXT STEPS (omit this section when no further action is needed)
{Bullet points with concrete recommendations}

---

## HARD RULES

- Never fabricate row counts, reduction percentages, or plan details.
  Every number must come from the PLAN COMPARISON block below.
- If CBO estimates are unavailable for either plan, say so in PLAN METRICS
  and base the VERDICT on qualitative evidence (partition filters, join type).
- Never suggest re-running or executing the queries to get results.
- Keep the OVERVIEW jargon-free — write for an analyst who knows SQL.
"""

# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


async def handle_compare_queries(
    client: "HMSClient",
    original_query: str,
    optimized_query: str,
    hs2_client: "HS2Client | None" = None,
) -> str:
    """
    Compare two HiveQL SELECT queries side-by-side using their EXPLAIN plans.

    Fetches HMS metadata for every table referenced in either query, then calls
    hs2_client.compare_explain_plans to produce a structured side-by-side report
    of CBO row estimates, partition pruning, and join strategy — before and after.

    Requires HiveServer2 (HS2) to be configured: without EXPLAIN plan data, a
    meaningful plan comparison cannot be produced.  Degrades gracefully with a
    clear message when HS2 is unavailable.
    """
    try:
        if not original_query.strip():
            return "Error: original_query cannot be empty."
        if not optimized_query.strip():
            return "Error: optimized_query cannot be empty."

        for label, q in [("original_query", original_query), ("optimized_query", optimized_query)]:
            is_write, op = _is_write_operation_request(q)
            if is_write:
                return (
                    f"Error: {label} contains a {op} operation.\n\n"
                    "compare_queries supports SELECT queries only."
                )

        if hs2_client is None or not hs2_client.is_available():
            return (
                "compare_queries requires HiveServer2 (HS2) to run EXPLAIN plans.\n\n"
                "HS2 is not configured or could not be reached. Set HS2_HOST (and "
                "optionally HS2_PORT, HS2_USER, HS2_AUTH) in your .env file, then "
                "restart HiveMind.\n\n"
                "Without EXPLAIN plans, the only comparison possible is a manual "
                "review of the query text — use explain_query on each query individually "
                "to reason from HMS metadata."
            )

        # Collect the union of tables from both queries so one HMS fetch covers both.
        all_refs: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for q in (original_query, optimized_query):
            for db, tbl in _extract_tables_from_sql(q):
                key = (db.lower(), tbl.lower())
                if key not in seen:
                    seen.add(key)
                    all_refs.append((db, tbl))

        if not all_refs:
            return (
                "Error: no fully-qualified table references (database.table) found in "
                "either query.  Make sure both queries use the form database.table in "
                "FROM and JOIN clauses."
            )

        context_parts: list[str] = []
        not_found: list[str] = []

        with client.request_cache():
            for db, tbl in all_refs:
                schema = await handle_get_table_schema(client, db, tbl)
                if schema.startswith("Error"):
                    not_found.append(f"{db}.{tbl}")
                    continue
                partitions = await handle_get_partitions(client, db, tbl)
                stats = await handle_get_table_stats(client, db, tbl)
                context_parts.extend([schema, partitions, stats])

        if not_found:
            names = ", ".join(not_found)
            return (
                f"I couldn't find the following table(s) in the Hive Metastore: {names}. "
                "Please check the table name and database, then try again."
            )

        assembled_context = "\n\n".join(context_parts)
        clean_context = _strip_backticks(assembled_context)

        tables = _parse_tables(assembled_context)
        rows = _parse_row_counts(assembled_context)
        pkeys, _ = _parse_columns_and_partitions(assembled_context)

        focus_table = pick_focus_table(tables, pkeys)
        focus_pkeys = (
            list(dict.fromkeys(partition_keys_for_table(pkeys, focus_table)))
            if focus_table
            else []
        )
        # Baseline row count for the focus (partitioned) table — not the largest
        # table in the query (e.g. customer 2M vs store_sales partition rollup).
        hms_total = (
            hms_rows_for_table(rows, focus_table) if focus_table else None
        ) or max_hms_total_rows(rows)

        # For fetch-only plans (partition filter resolved at metastore level),
        # the CBO emits no row estimate.  Resolve the partition filter value from
        # the optimized query's WHERE clause and look it up in the HMS partition
        # sample so the comparison table shows real numbers instead of "unknown".
        partition_sample_rows: int | None = None
        if focus_pkeys:
            filter_vals = _extract_partition_filter_values(optimized_query, focus_pkeys)
            for pk, val in filter_vals.items():
                n = _lookup_partition_sample_rows(assembled_context, pk, val)
                if n is not None:
                    partition_sample_rows = n
                    break

        comparison_report = hs2_client.compare_explain_plans(
            original_query=original_query,
            optimized_query=optimized_query,
            hms_total_rows=hms_total,
            table=focus_table,
            partition_keys=focus_pkeys,
            partition_sample_rows=partition_sample_rows,
        )

        prompt = "\n\n".join([
            _COMPARE_SYSTEM_PROMPT,
            f"Original query:\n```sql\n{original_query.strip()}\n```",
            f"Optimized query:\n```sql\n{optimized_query.strip()}\n```",
            "PLAN COMPARISON (from HS2 EXPLAIN — primary source of truth):\n" + comparison_report,
            "Full metastore context:\n" + clean_context.strip(),
        ])

        return prompt

    except Exception as exc:
        logger.exception("compare_queries failed")
        return f"Error preparing query comparison: {exc}"
