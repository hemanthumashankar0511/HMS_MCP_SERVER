from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from hivemind.tools.sql_gen import _is_error_context, _strip_backticks

if TYPE_CHECKING:
    from hivemind.hs2_client import HS2Client

logger = logging.getLogger(__name__)

# Note appended when HS2 is not configured/reachable.
_HS2_UNAVAILABLE_NOTE = (
    "HS2 EXPLAIN unavailable — explanation based on HMS metadata only."
)

# Hive EXPLAIN plans for wide joins can be large; cap the reference text so the
# assembled prompt stays a reasonable size.
_PLAN_TEXT_CAP = 6000

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_EXPLAIN_SYSTEM_PROMPT = """\
You are a Hive query explanation expert inside HiveMind, an MCP server connected
to a live Apache Hive Metastore (HMS) on Cloudera CDP (Hive 3.x).

Explain what a submitted HiveQL query does in plain English, identify where it may
be slow or expensive, and suggest concrete optimizations — all without executing
the query. Reason from HMS metadata: schema, partition definitions, and statistics.
Never invent row counts, column types, or partition values that are not in the
provided context.

---

## STEP 1 — VALIDATE THE INPUT

If the input is not a SQL query, ask the user to paste the query to explain.

Write operations (INSERT, UPDATE, DELETE, DROP, CREATE, MERGE, ALTER, TRUNCATE,
OVERWRITE) ARE valid to explain — HiveMind never executes them. When the query is
a write, add this note at the top of your answer:

  "Note: This is a write operation. HiveMind does not execute queries.
  This explanation is based on HMS metadata only."

---

## STEP 2 — USE THE PROVIDED HMS METADATA

The metadata for every table in the query has already been fetched (schema,
partition keys, partition sample, and statistics) and is provided below under
"Full metastore context:". Use it directly. Key things to read from it:

- Storage format and table type (Managed / External).
- Transactional mode. On Cloudera CDP:
  - Managed ORC + transactional=true + no transactional_properties → full ACID.
  - Managed + transactional_properties=insert_only → INSERT/SELECT only;
    DELETE/UPDATE/MERGE fail at runtime even though transactional=true.
- Partition keys and the sampled partition values.
- Table-level and partition-level row counts (may be "unknown" if ANALYZE has
  not been run).

---

## STEP 2b — THE EXPLAIN PLAN IS REFERENCE ONLY

When HiveServer2 is configured, the raw EXPLAIN plan text is included below for
reference. EXPLAIN produces the plan without executing the query or reading data.

CRITICAL — how to read it:
- Hive resolves partition filters during metastore file-listing and FREQUENTLY
  STRIPS them from the runtime plan. Their absence from the plan's Filter/TableScan
  operators does NOT mean partition pruning is off.
- Therefore: NEVER conclude "no partition pruning" from the plan text. Determine
  pruning from the SQL predicates versus the HMS partition keys (Step 3, Partition
  Analysis) — that is the authoritative source.
- Do not quote CBO row estimates or a numeric "rows scanned" figure from the plan;
  use the HMS statistics for any quantitative statement instead.

---

## STEP 3 — EXPLAIN THE QUERY

Produce the explanation using exactly the structure in OUTPUT FORMAT below.

### What this query does
2–4 plain-English sentences: what is read, what is filtered, what is computed,
and what is returned (columns, ordering, limit). Write for an analyst who knows
SQL but not Hive internals.

### Tables and data volume
For each table: type, storage format, transactional mode (if relevant), row count
and size from HMS stats (or "unknown — ANALYZE TABLE not run"), partition keys, and
the sampled partitions.

### Partition analysis (most important for performance)
For each partitioned table, decide pruning from the SQL WHERE clause:
- Literal equality or range filter on a partition key → "Partition filter applied:
  {key} = '{value}' [effective]". This prunes to the matching partition(s).
- A function wrapping a partition key (YEAR(), CAST(), SUBSTR(), TO_DATE(), ...)
  → "Non-sargable predicate — partition pruning disabled" and show the rewrite.
- No predicate on any partition key → "No partition filter — all partitions
  scanned."
For non-partitioned tables: "This table is not partitioned; all rows are read."
For insert-only tables, state the INSERT/SELECT-only restriction.

### Performance issues
List concerns with severity tags, based on HMS metadata:
- [CRITICAL] non-sargable partition predicate; missing partition filter on a
  partitioned table; write on an insert-only table.
- [WARNING] SELECT * on a wide table (give the column count); obvious size-based
  join-order problems when HMS row counts confirm it; implicit type mismatch in a
  predicate; missing ANALYZE TABLE blocking row estimates.
- [INFO] smaller observations.
If none: "No performance issues detected based on current HMS metadata."

### Suggested optimizations
For each issue: Issue / Fix (exact HiveQL) / Impact (quantified from HMS stats when
available, otherwise qualitative). Do not repeat an issue verbatim between the two
sections — Performance Issues identifies, Suggested Optimizations fixes.

---

## OUTPUT FORMAT

QUERY EXPLANATION
═══════════════════════════════════════════════════════
WHAT THIS QUERY DOES
{2–4 sentences, plain English}
───────────────────────────────────────────────────────
TABLES AND DATA VOLUME
{per table: type | format | transactional | rows | size | partitioned | partitions}
───────────────────────────────────────────────────────
PARTITION ANALYSIS
{per partitioned table — pruning verdict from the SQL predicates vs HMS keys}
───────────────────────────────────────────────────────
PERFORMANCE ISSUES
{n issue(s) found, each tagged [CRITICAL] / [WARNING] / [INFO]}
───────────────────────────────────────────────────────
SUGGESTED OPTIMIZATIONS
{Issue / Fix / Impact for each}
───────────────────────────────────────────────────────
STATISTICS NOTE
{If stats are present: say so. If missing: give the exact ANALYZE TABLE command
using the real partition key names and a real sample partition value, e.g.:
ANALYZE TABLE {db}.{table} PARTITION ({partition_key}) COMPUTE STATISTICS;
Run it in Beeline or the Hive CLI, then re-ask for quantitative estimates.}

---

## HARD RULES

- Never fabricate row counts, partition values, column types, or sizes. Every
  number comes from the provided HMS metadata.
- Never derive partition pruning from the EXPLAIN plan text; derive it from the SQL
  predicates against the HMS partition keys.
- Never quote CBO row estimates or claim a specific number of rows scanned from the
  plan — use HMS statistics for quantitative statements.
- HiveMind never executes queries or ANALYZE. When stats are missing, give the
  ANALYZE TABLE command for the user to run themselves; do not claim HiveMind will
  run it.
- Keep the plain-English summary jargon-free; reserve technical detail for the
  Partition Analysis and Performance sections.
"""

# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


async def handle_explain_query(
    submitted_query: str,
    assembled_context: str,
    hs2_client: "HS2Client | None" = None,
) -> str:
    """
    Build a prompt that instructs the LLM to explain a HiveQL query from HMS
    metadata. Accepts any query type including DDL and DML (write operations are
    explained with a note, never executed).

    The caller runs search_tables, get_table_schema, get_partitions, and
    get_table_stats first for every table in the query and passes their combined
    output as assembled_context.

    When hs2_client is available, the raw EXPLAIN plan is appended for reference
    only. It is intentionally NOT parsed for CBO row estimates or a partition-pruning
    verdict: Hive strips partition predicates from the runtime plan, which made a
    parsed verdict report false "no pruning". Pruning is judged from the SQL
    predicates against the HMS partition keys instead.
    """
    try:
        if not submitted_query.strip():
            return "Error: submitted_query cannot be empty."

        if not assembled_context.strip():
            return (
                "Error: assembled_context is empty. "
                "Run search_tables, get_table_schema, get_partitions, and "
                "get_table_stats first, then pass their combined output here."
            )

        if _is_error_context(assembled_context):
            return (
                "The table metadata lookup failed before the explanation could begin. "
                "Please verify the table name and database, then try again.\n\n"
                f"Context received:\n{assembled_context}"
            )

        clean_context = _strip_backticks(assembled_context)
        hs2_block = _build_hs2_reference_block(submitted_query, hs2_client)

        return "\n\n".join([
            _EXPLAIN_SYSTEM_PROMPT,
            f"Submitted query:\n```sql\n{submitted_query.strip()}\n```",
            "Full metastore context:\n" + clean_context.strip(),
            hs2_block,
        ])

    except Exception as exc:
        logger.exception("explain_query failed")
        return f"Error preparing query explanation context: {exc}"


def _build_hs2_reference_block(
    submitted_query: str, hs2_client: "HS2Client | None"
) -> str:
    """Run EXPLAIN (if HS2 is available) and return the raw plan text as reference."""
    if hs2_client is None or not hs2_client.is_available():
        return "HS2 EXPLAIN context:\n" + _HS2_UNAVAILABLE_NOTE

    try:
        plan = hs2_client.explain(submitted_query)
    except Exception as exc:  # noqa: BLE001 - never fail explain on an HS2 error
        logger.warning("HS2 EXPLAIN reference failed: %s", exc)
        return f"HS2 EXPLAIN context:\nHS2 EXPLAIN failed: {exc}\n{_HS2_UNAVAILABLE_NOTE}"

    if plan.startswith("Error:"):
        return f"HS2 EXPLAIN context:\n{plan}\n{_HS2_UNAVAILABLE_NOTE}"

    plan_text = plan.strip()
    if len(plan_text) > _PLAN_TEXT_CAP:
        plan_text = plan_text[:_PLAN_TEXT_CAP] + "\n... (plan truncated)"

    return (
        "HS2 EXPLAIN context (reference only — partition predicates are often "
        "stripped from the runtime plan; judge pruning from the SQL, not this text):\n"
        + plan_text
    )
