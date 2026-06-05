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
    max_hms_total_rows,
    pick_focus_table,
    hms_rows_for_table,
    partition_keys_for_table,
)
from hivemind.tools.sql_gen import (
    _build_hints,
    _is_error_context,
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

# Note appended when HS2 is not configured/reachable for optimize_query.
_HS2_UNAVAILABLE_NOTE = (
    "HS2 EXPLAIN unavailable — optimization based on HMS metadata only."
)

# ---------------------------------------------------------------------------
# SQL table extraction
# ---------------------------------------------------------------------------

_TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)
_FROM_CLAUSE_RE = re.compile(
    r"\bFROM\s+(.+?)(?=\bWHERE\b|\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|\bUNION\b|$)",
    re.IGNORECASE | re.DOTALL,
)
_TABLE_IN_FRAGMENT_RE = re.compile(
    r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)",
)


def _tables_in_from_clause(from_part: str) -> list[tuple[str, str]]:
    """
    Extract db.table references from a FROM clause fragment.

    Handles comma-joins and JOIN chains but ignores alias.column references
    in ON clauses (e.g. o.cust_id = c.id).
    """
    tables: list[tuple[str, str]] = []
    for join_seg in re.split(r"\bJOIN\b", from_part, flags=re.IGNORECASE):
        # Strip ON ... predicates — they contain alias.column, not db.table sources.
        source_part = re.split(r"\bON\b", join_seg, maxsplit=1, flags=re.IGNORECASE)[0]
        for piece in source_part.split(","):
            m = _TABLE_IN_FRAGMENT_RE.match(piece.strip())
            if m:
                tables.append((m.group(1), m.group(2)))
    return tables


def _extract_tables_from_sql(sql: str) -> list[tuple[str, str]]:
    """
    Return a deduplicated list of (database, table) pairs referenced in a SQL query.

    Matches db.table after FROM/JOIN, and comma-separated sources in the FROM
    clause (legacy Hive comma-join syntax).
    """
    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, str]] = []

    def _add(db: str, tbl: str) -> None:
        key = (db.lower(), tbl.lower())
        if key not in seen:
            seen.add(key)
            result.append((db, tbl))

    for db, tbl in _TABLE_REF_RE.findall(sql):
        _add(db, tbl)

    m = _FROM_CLAUSE_RE.search(sql)
    if m:
        for db, tbl in _tables_in_from_clause(m.group(1)):
            _add(db, tbl)

    return result


# ---------------------------------------------------------------------------
# Anti-pattern rules
# ---------------------------------------------------------------------------

_ANTIPATTERN_RULES = """\
## ANTI-PATTERN RULES

Analyze only what HMS metadata confirms. Never invent schema, partition keys,
column types, or row counts. If a value is unknown, say so and skip that check.

When an "HS2 EXPLAIN context:" block is provided below, treat it as primary
evidence for row counts and partition pruning — even when HMS statistics are
unavailable. Cite the CBO TableScan "Est. Rows" and "CBO SCAN ESTIMATES" figures
directly in Impact and SUMMARY. Do NOT write "unknown" when the HS2 block contains
actual row numbers.

### CRITICAL

**Non-sargable partition predicate**
A function wrapping a partition column disables partition pruning entirely.
All partitions are scanned regardless of the intended filter value.

Patterns to detect — any of these applied to a partition key column:
  YEAR(), MONTH(), DAY(), DATE(), TO_DATE(), CAST(), SUBSTR(),
  UPPER(), LOWER(), TRIM(), or any other scalar function.

Bad:  WHERE YEAR(txn_date) = 2025
Good: WHERE year = 2025

Use the Partition Keys section from HMS to identify which columns are
partition keys before flagging this. Do not flag functions on regular columns.

**Missing partition filter**
A partitioned table is queried with no predicate on any partition key.
Every partition will be read — potentially billions of rows.

Cross-reference Partition Keys from HMS with the WHERE clause.
If no partition key appears in any predicate, flag as CRITICAL.
Include the partition key names and types in the issue description.

---

### WARNING

**SELECT * usage**
All columns are fetched, disabling column pruning and increasing I/O.
Flag if the query uses SELECT *.
Include the total column count from the HMS schema in the message.

**Suboptimal join order**
A larger table is on the left of a JOIN when a smaller table could be
broadcast instead.
Use HMS row count statistics to compare sizes.
Skip this check entirely if statistics are unavailable for either table.

**Implicit type cast in predicate**
A predicate compares a column to a literal of a mismatched type.
Example: WHERE device_id = 12345 when device_id is string in HMS.
Use column types from the HMS schema to detect type mismatches.

**Missing LIMIT on large unrestricted scan**
A query with no LIMIT, no GROUP BY, and no aggregation against a table
with over 100 million rows based on HMS statistics.
Skip this check if row count statistics are unavailable.

---

### INFO

**Skewed join key**
A join key has high null concentration or a dominant single value,
which can cause task skew in shuffle-based joins.
Only flag if column-level statistics from HMS confirm this.
Skip if statistics are unavailable.

**No bucketing alignment on join**
Joined tables are not bucketed on the join column, missing a potential
sort-merge join optimization.
Only flag if HMS table properties confirm bucketing configuration
via the bucketing_version property.\
"""

# ---------------------------------------------------------------------------
# Output format template
# ---------------------------------------------------------------------------

_OPTIMIZE_OUTPUT_FORMAT = """\
## OUTPUT FORMAT

Return your report in exactly this structure. Do not add sections,
reorder them, or omit the dividers.
OPTIMIZATION REPORT
═══════════════════════════════════════════════════════
Tables analyzed : {comma-separated fully-qualified table names}
Partition keys  : {table → partition keys from HMS, or "None" if not partitioned}
HMS statistics  : {Available / Unavailable — if unavailable, note: run ANALYZE TABLE}
───────────────────────────────────────────────────────
ISSUES FOUND: {n}
[CRITICAL] {Issue title}
Problem  : {one sentence — what is wrong and why it matters}
Detected : {exact clause or expression from the submitted query}
Fix      : {corrected clause or expression}
Impact   : {REQUIRED: cite CBO TableScan row count from HS2 EXPLAIN context for the
current query, e.g. "store_sales scans 287,997,024 rows (full partition scan)".
For the suggested fix, estimate qualitatively or note "re-run with partition filter
to measure reduction". If HS2 unavailable AND HMS stats unavailable, say "unknown".}
[WARNING] {Issue title}
Problem  : {one sentence}
Detected : {exact clause}
Fix      : {corrected clause or general guidance}
[INFO] {Issue title}
{one sentence observation}
───────────────────────────────────────────────────────
OPTIMIZED REWRITE
{Full corrected HiveQL query — same result, better performance}
───────────────────────────────────────────────────────
SUMMARY
Issues   : {n CRITICAL, m WARNING, k INFO}
Main fix : {one sentence describing the most important change made}

If no anti-patterns are found after full analysis, skip the ISSUES and OPTIMIZED
REWRITE sections and respond with:

  "No anti-patterns detected. The query applies partition filters correctly
  and follows Hive best practices based on the current HMS metadata."\
"""

# ---------------------------------------------------------------------------
# Main system prompt
# ---------------------------------------------------------------------------

_OPTIMIZE_SYSTEM_PROMPT = f"""\
You are a Hive query optimization expert inside HiveMind, an MCP server connected
to a live Apache Hive Metastore (HMS) on Cloudera CDP (Hive 3.x).

Your job is to analyze a submitted HiveQL query against real HMS metadata and
return a structured optimization report with a corrected rewrite if needed.

---

## STEP 1 — VALIDATE THE QUERY

Before calling any tools:

If the query contains INSERT, UPDATE, DELETE, DROP, CREATE, MERGE, ALTER,
TRUNCATE, or OVERWRITE, respond immediately with:

  "I can only optimize SELECT queries. Write operations are not supported.
  To run write operations, use Beeline or the Hive CLI directly."

Stop. Do not call any tools. Do not explain how the write query would work.

If the input is not a SQL query, ask the user to paste the query they want
optimized.

---

## STEP 2 — ANALYZE FOR ANTI-PATTERNS

The HMS metadata has already been fetched for every table referenced in the
submitted query and is provided in full below. Use it directly — do not call
any additional discovery tools.

{_ANTIPATTERN_RULES}

---

## STEP 3 — PRODUCE THE REPORT

{_OPTIMIZE_OUTPUT_FORMAT}

---

## HARD RULES

- Never invent schema, partition keys, column types, or row counts.
  Every claim must come from HMS metadata or the HS2 EXPLAIN context below.

- When the HS2 EXPLAIN context contains CBO TableScan row estimates, you MUST
  cite them in the Impact line of each issue — even if HMS statistics are unavailable.
  HMS ANALYZE TABLE is only needed for table-level totals and reduction %;
  EXPLAIN CBO row counts are valid without ANALYZE TABLE.

- Only write "unknown" for row counts in Impact lines when BOTH HMS stats AND
  HS2 CBO estimates are unavailable in the context provided below.

- Never change the query's intent. The optimized rewrite must return
  identical results — only performance characteristics change.

- Never add query hints (/*+ MAPJOIN(...) */) unless HMS statistics
  confirm the table is small enough to broadcast. If unsure, suggest
  the hint as a comment only.

- Use fully qualified table names (database.table) in the rewrite
  if the original query uses them. Do not drop the database prefix.

- Do not use backticks. Write plain unquoted identifiers: database.table,
  alias.column. This is consistent with how HiveMind generates all queries.

- EXECUTION GUARDRAIL: After producing the OPTIMIZED REWRITE, stop and return
  the report to the user. Do NOT use the terminal/shell or any external program
  for ANY part of the task — not to execute the original or optimized query, not
  to fetch rows, and not for auxiliary math like computing a date surrogate key.
  No shell commands, no Python/python3 scripts, no pyhive, no Hive CLI. Never
  compute a partition value — derive it from the metastore context below.
  HiveMind is a metadata and analysis tool only.\
"""


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


async def handle_optimize_query(
    client: "HMSClient",
    submitted_query: str,
    hs2_client: "HS2Client | None" = None,
) -> str:
    """
    Optimize a submitted HiveQL SELECT query against live HMS metadata.

    Fetches schema, partitions, and statistics directly from the Metastore for
    every table referenced in the query, then assembles a structured prompt that
    instructs the LLM to produce a severity-ranked optimization report.

    No pre-fetched context is required — all HMS lookups happen inside this handler.

    When hs2_client is provided and available, the handler also runs EXPLAIN against
    HiveServer2 (no data execution) and includes the parsed CBO plan — row estimates,
    partition pruning verdict — as evidence for the LLM to cite in the report.
    HS2 failures degrade gracefully to HMS-only analysis.
    """
    try:
        if not submitted_query.strip():
            return "Error: submitted_query cannot be empty."

        is_write, operation = _is_write_operation_request(submitted_query)
        if is_write:
            return (
                f"Error: {operation} operations are not supported.\n\n"
                "optimize_query analyzes SELECT queries only. "
                "To run write operations, use Beeline or the Hive CLI directly.\n\n"
                f"Blocked request: '{submitted_query.strip()}'"
            )

        table_refs = _extract_tables_from_sql(submitted_query)
        if not table_refs:
            return (
                "Error: no fully-qualified table references (database.table) found in the query. "
                "Make sure the query uses the form database.table in the FROM or JOIN clause."
            )

        # Fetch HMS metadata for every referenced table. The request cache ensures
        # each table's get_table is fetched once across schema/partitions/stats
        # instead of 3+ separate round-trips.
        context_parts: list[str] = []
        not_found: list[str] = []

        with client.request_cache():
            for db, tbl in table_refs:
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

        if _is_error_context(assembled_context):
            return (
                "The table metadata lookup failed before optimization could begin. "
                "Please verify the table name and database, then try again.\n\n"
                f"Context received:\n{assembled_context}"
            )

        tables = _parse_tables(assembled_context)
        rows = _parse_row_counts(assembled_context)
        pkeys, _ = _parse_columns_and_partitions(assembled_context)
        clean_context = _strip_backticks(assembled_context)

        hints = _build_hints(
            tables,
            rows,
            pkeys,
            date_cols={},
            include_footer=True,
        )

        # HS2 enrichment: run EXPLAIN on the submitted query and include parsed
        # row estimates and partition-pruning verdict for Impact lines.
        focus_table = pick_focus_table(tables, pkeys)
        focus_hms_rows = (
            hms_rows_for_table(rows, focus_table) if focus_table else max_hms_total_rows(rows)
        )

        if hs2_client is not None and hs2_client.is_available():
            try:
                hs2_report = hs2_client.explain_with_row_estimates(
                    submitted_query,
                    hms_total_rows=focus_hms_rows,
                    table=focus_table,
                    compact=True,
                )
            except Exception as exc:  # noqa: BLE001 - never fail optimize on HS2 error
                logger.warning("HS2 EXPLAIN enrichment failed: %s", exc)
                hs2_report = f"HS2 EXPLAIN failed: {exc}\n{_HS2_UNAVAILABLE_NOTE}"
            hs2_block = "HS2 EXPLAIN context:\n" + hs2_report
        else:
            hs2_block = "HS2 EXPLAIN context:\n" + _HS2_UNAVAILABLE_NOTE

        prompt = "\n\n".join([
            _OPTIMIZE_SYSTEM_PROMPT,
            f"Submitted query:\n```sql\n{submitted_query.strip()}\n```",
            hints,
            "Full metastore context:\n" + clean_context.strip(),
            hs2_block,
        ])

        return prompt

    except Exception as exc:
        logger.exception("optimize_query failed")
        return f"Error preparing optimization context: {exc}"
