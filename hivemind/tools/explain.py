from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from hivemind.tools.discovery import (
    handle_get_partitions,
    handle_get_table_schema,
    handle_get_table_stats,
)
from hivemind.tools.sql_gen import (
    _build_hints,
    _is_error_context,
    _parse_columns_and_partitions,
    _parse_row_counts,
    _parse_tables,
    _strip_backticks,
)

if TYPE_CHECKING:
    from hivemind.hms_client import HMSClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL table extraction
# ---------------------------------------------------------------------------

_TABLE_NAME = r"`?([a-zA-Z_][a-zA-Z0-9_]*)`?(?:\.`?([a-zA-Z_][a-zA-Z0-9_]*)`?)?"

_TABLE_REF_RE = re.compile(
    rf"\b(?:FROM|JOIN|UPDATE|INTO|MERGE\s+INTO|ANALYZE\s+TABLE|ALTER\s+TABLE|"
    rf"DROP\s+TABLE|TRUNCATE\s+TABLE|DELETE\s+FROM)\s+{_TABLE_NAME}",
    re.IGNORECASE,
)
_INSERT_OVERWRITE_RE = re.compile(
    rf"\bINSERT\s+OVERWRITE\s+(?:TABLE\s+)?{_TABLE_NAME}",
    re.IGNORECASE,
)
_CTE_RE = re.compile(r"\bWITH\s+`?([a-zA-Z_][a-zA-Z0-9_]*)`?\s+AS\s*\(", re.IGNORECASE)


def _extract_tables_from_sql(sql: str) -> tuple[list[tuple[str, str]], list[str]]:
    """
    Return deduplicated fully-qualified tables and unqualified table names.

    The explain tool accepts DDL, DML, and SELECT statements, so this covers the
    common table-bearing clauses used by those statement types.
    """
    cte_names = {name.lower() for name in _CTE_RE.findall(sql)}
    seen: set[tuple[str, str]] = set()
    qualified: list[tuple[str, str]] = []
    unqualified: list[str] = []

    for pattern in (_TABLE_REF_RE, _INSERT_OVERWRITE_RE):
        for first, second in pattern.findall(sql):
            if second:
                key = (first.lower(), second.lower())
                if key not in seen:
                    seen.add(key)
                    qualified.append((first, second))
                continue

            if first.lower() not in cte_names and first not in unqualified:
                unqualified.append(first)

    return qualified, unqualified


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_EXPLAIN_SYSTEM_PROMPT = """\
You are a Hive query explanation expert inside HiveMind, an MCP server connected
to a live Apache Hive Metastore (HMS) on Cloudera CDP (Hive 3.x).

Your job is to explain what a submitted HiveQL query does in plain English,
identify where it may be slow or expensive based on real HMS metadata, and
suggest concrete optimizations without executing the query.

The HMS metadata has already been fetched for every table referenced in the
submitted query and is provided below. Use only that metadata: schema,
partition definitions, table properties, and statistics. Never guess values
that are not present in the metadata.

---

## VALIDATION RULES

If the input is not a SQL query, ask the user to paste the query they want
explained.

Write operations and DDL (INSERT, UPDATE, DELETE, DROP, CREATE, MERGE, ALTER,
TRUNCATE, OVERWRITE, ANALYZE) are valid to explain. Do not reject them. Add this
note at the top of the explanation:

  "Note: This is a write or metadata operation. HiveMind does not execute
  queries. This explanation is based on HMS metadata only."

---

## WHAT TO EXPLAIN

Return the explanation in exactly this structure:

QUERY EXPLANATION
═══════════════════════════════════════════════════════
WHAT THIS QUERY DOES
{2-4 sentences in plain English}
───────────────────────────────────────────────────────
TABLES AND DATA VOLUME
{For each table:}
{database}.{table}
Type         : {Managed / External} | {ORC / Parquet / TextFile / Avro}
Transactional: {Full ACID / Insert-only / Non-transactional}
Rows         : {from HMS stats, or "unknown - run ANALYZE TABLE"}
Size         : {from HMS stats, or "unknown"}
Partitioned  : {Yes - keys: {key list} / No}
Partitions   : {sample values from get_partitions(), or "none found"}
───────────────────────────────────────────────────────
PARTITION ANALYSIS
{For each table, describe partition pruning or state that the table is not partitioned.}
───────────────────────────────────────────────────────
PERFORMANCE ISSUES
{n issue(s) found}
[CRITICAL] {title}
{one sentence explanation}
[WARNING] {title}
{one sentence explanation}
[INFO] {title}
{one sentence observation}
───────────────────────────────────────────────────────
SUGGESTED OPTIMIZATIONS
{For each issue:}
Issue  : {description}
Fix    : {exact corrected HiveQL clause or expression}
Impact : {quantitative if stats are available, qualitative if not}
───────────────────────────────────────────────────────
STATISTICS NOTE
{State whether HMS statistics are populated. If missing, provide exact ANALYZE
TABLE statements using the real partition keys and sample partition values in
the metadata. Do not use placeholders.}

---

## ANALYSIS RULES

- Section 1 must be jargon-free and written for an analyst who understands SQL
  but not Hive internals.
- For partitioned tables, identify whether the query has a direct partition
  filter, a function-wrapped/non-sargable partition predicate, or no partition
  filter.
- A direct equality or range predicate on a partition key enables partition
  pruning.
- A function around a partition key disables partition pruning. Show the exact
  rewrite using the raw partition key.
- If no partition key appears in the WHERE clause for a partitioned table, flag
  a CRITICAL missing partition filter.
- For insert-only tables, state that DELETE, UPDATE, and MERGE are not supported
  at runtime even when transactional=true.
- For SELECT * on a wide table, include the HMS column count.
- If HMS stats are unavailable, do not provide row-count or size estimates.
- Do not suggest HiveServer2 EXPLAIN, Tez DAG inspection, or any execution-based
  analysis. HiveMind reasons from HMS metadata only.
- Never confuse transactional=true with full ACID support. Always check
  transactional_properties. insert_only means DELETE, UPDATE, and MERGE will
  fail at runtime.
- Do not repeat the same wording in PERFORMANCE ISSUES and SUGGESTED
  OPTIMIZATIONS. Section 4 identifies; Section 5 fixes.
"""


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


async def handle_explain_query(client: "HMSClient", submitted_query: str) -> str:
    """
    Explain any submitted HiveQL query against live HMS metadata.

    This is an analytical tool, not a generative or execution tool. It accepts
    SELECT, DDL, and DML statements, fetches HMS metadata for referenced tables,
    and returns a prompt that explains behavior, performance risks, and fixes.
    """
    try:
        if not submitted_query.strip():
            return "Error: submitted_query cannot be empty."

        table_refs, unqualified = _extract_tables_from_sql(submitted_query)
        if unqualified:
            names = ", ".join(sorted(unqualified))
            return (
                "Error: explain_query needs fully-qualified table names so it can "
                "fetch the correct HMS metadata. Use database.table for: "
                f"{names}."
            )

        if not table_refs:
            return (
                "Error: no fully-qualified table references (database.table) found "
                "in the query. Make sure the query includes a table such as "
                "sample.sales_transactions."
            )

        context_parts: list[str] = []
        not_found: list[str] = []

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
                "The table metadata lookup failed before the explanation could begin. "
                "Please verify the table name and database, then try again.\n\n"
                f"Context received:\n{assembled_context}"
            )

        tables = _parse_tables(assembled_context)
        rows = _parse_row_counts(assembled_context)
        pkeys, date_cols = _parse_columns_and_partitions(assembled_context)
        clean_context = _strip_backticks(assembled_context)

        hints = _build_hints(
            tables,
            rows,
            pkeys,
            date_cols,
            include_footer=True,
        )

        prompt = "\n\n".join(
            [
                _EXPLAIN_SYSTEM_PROMPT,
                f"Submitted query:\n```sql\n{submitted_query.strip()}\n```",
                hints,
                "Full metastore context:\n" + clean_context.strip(),
            ]
        )

        return prompt

    except Exception as exc:
        logger.exception("explain_query failed")
        return f"Error preparing query explanation context: {exc}"
