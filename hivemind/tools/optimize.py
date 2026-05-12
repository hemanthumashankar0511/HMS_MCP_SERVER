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
    _is_write_operation_request,
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

_TABLE_REF_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)


def _extract_tables_from_sql(sql: str) -> list[tuple[str, str]]:
    """
    Return a deduplicated list of (database, table) pairs referenced in a SQL query.
    Matches db.table patterns that follow FROM or JOIN keywords.
    """
    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, str]] = []
    for db, tbl in _TABLE_REF_RE.findall(sql):
        key = (db.lower(), tbl.lower())
        if key not in seen:
            seen.add(key)
            result.append((db, tbl))
    return result


# ---------------------------------------------------------------------------
# Anti-pattern rules
# ---------------------------------------------------------------------------

_ANTIPATTERN_RULES = """\
## ANTI-PATTERN RULES

Analyze only what HMS metadata confirms. Never invent schema, partition keys,
column types, or row counts. If a value is unknown, say so and skip that check.

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
Impact   : {estimated row reduction if HMS stats available,
otherwise "unknown — run ANALYZE TABLE ... COMPUTE STATISTICS"}
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
Partition filter applied : {Yes / No / Partial — list which partition keys are covered}
Estimated rows before    : {from HMS stats, or "unknown — run ANALYZE TABLE"}
Estimated rows after     : {calculated from partition stats, or "unknown"}
Estimated reduction      : {percentage, or "unknown"}

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
  Every claim must come from the HMS metadata provided below.

- Never change the query's intent. The optimized rewrite must return
  identical results — only performance characteristics change.

- Never add query hints (/*+ MAPJOIN(...) */) unless HMS statistics
  confirm the table is small enough to broadcast. If unsure, suggest
  the hint as a comment only.

- Use fully qualified table names (database.table) in the rewrite
  if the original query uses them. Do not drop the database prefix.

- Do not use backticks. Write plain unquoted identifiers: database.table,
  alias.column. This is consistent with how HiveMind generates all queries.

- If HMS statistics are unavailable, omit all row count estimates.
  State clearly: "run ANALYZE TABLE {{db}}.{{table}} COMPUTE STATISTICS
  PARTITION (partition_key) to enable row count estimates."\
"""


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


async def handle_optimize_query(client: "HMSClient", submitted_query: str) -> str:
    """
    Optimize a submitted HiveQL SELECT query against live HMS metadata.

    Fetches schema, partitions, and statistics directly from the Metastore for
    every table referenced in the query, then assembles a structured prompt that
    instructs the LLM to produce a severity-ranked optimization report.

    No pre-fetched context is required — all HMS lookups happen inside this handler.
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

        # Fetch HMS metadata for every referenced table.
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

        prompt = "\n\n".join([
            _OPTIMIZE_SYSTEM_PROMPT,
            f"Submitted query:\n```sql\n{submitted_query.strip()}\n```",
            hints,
            "Full metastore context:\n" + clean_context.strip(),
        ])

        return prompt

    except Exception as exc:
        logger.exception("optimize_query failed")
        return f"Error preparing optimization context: {exc}"
