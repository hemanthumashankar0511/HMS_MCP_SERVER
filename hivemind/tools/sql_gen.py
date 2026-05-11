from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompt sections — composed at call time into the final instruction block.
# ---------------------------------------------------------------------------

_SYSTEM_RULES = """\
This tool generates SELECT-only HiveQL queries for data exploration and analysis.

If the user requests a write operation (DELETE, INSERT, UPDATE, DROP, TRUNCATE, ALTER, \
CREATE, MERGE, or any data modification), do not generate the query. Instead, respond:

  "I can only generate read-only SELECT queries. For write operations, please use your \
Hive client directly."

Do not explain how the write query would work. Do not offer alternatives that involve \
data modification.

Additional rules:
1. Do not use backticks anywhere. Write plain unquoted identifiers: database.table, alias.column.
2. Use only tables and columns that appear in the metastore context below.
3. Filter on partition keys first in WHERE to avoid full table scans.
4. Use short 1–2 letter aliases for tables in JOINs.\
"""

_QUERY_HINTS = """\
Query guidance:
- Apply the most selective partition filter the question allows. If the question has no \
time scope, use the most recent available partition value from the Partitions section.
- Keep the query simple and readable. Prefer straightforward aggregations over subqueries \
where possible.
- Add LIMIT unless the question specifies a different row count.\
"""

_OUTPUT_FORMAT = """\
Output format:
- Produce a complete, runnable HiveQL SELECT query.
- Do not add explanatory prose around the query.
- When row-count stats are available, append the footer lines shown in the example below. \
When stats are unavailable, omit the footer entirely.\
"""

# ---------------------------------------------------------------------------
# Helpers for parsing assembled_context
# ---------------------------------------------------------------------------


def _parse_tables(ctx: str) -> list[str]:
    """Return fully-qualified table names from Schema: lines."""
    return re.findall(r"^Schema:\s*(\S+)", ctx, re.MULTILINE)


def _parse_row_counts(ctx: str) -> dict[str, str]:
    """
    Return {table: row_count_str} from both Statistics: blocks and Schema: blocks.

    Statistics blocks (from get_table_stats):
        Statistics: db.table
          Rows       : 1,234,567

    Schema blocks (from get_table_schema) carry inline row-count warnings but
    do NOT emit a Rows line themselves; however, the table params printed via
    handle_get_table_schema don't include numRows directly.  We therefore also
    accept a bare 'Rows :' line that appears inside any section whose last
    top-level header mentioned a table name.
    """
    counts: dict[str, str] = {}
    current: str | None = None
    for line in ctx.splitlines():
        m = re.match(r"^(?:Statistics|Schema|Partitions):\s*(\S+)", line)
        if m:
            current = m.group(1)
            continue
        if current and re.match(r"\s+Rows\s*:", line):
            raw = line.split(":", 1)[1].strip()
            if raw not in ("-1", "", "N/A"):
                counts[current] = raw
            current = None
    return counts


_DATE_COL_RE = re.compile(
    r"(?:date|_dt|_time|_ts|timestamp|created|modified|updated|loaded|processed)",
    re.IGNORECASE,
)


def _parse_columns_and_partitions(ctx: str) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """
    Return partition keys and likely date/time columns from schema and partition blocks.
    """
    pkeys: dict[str, list[str]] = {}
    date_cols: dict[str, list[str]] = {}
    current_table: str | None = None
    in_section: str | None = None

    for line in ctx.splitlines():
        line = line.strip()

        m_schema = re.match(r"^(?:Schema|Partitions):\s*(\S+)", line)
        if m_schema:
            current_table = m_schema.group(1)
            in_section = None
            continue

        if line in ("Partition Keys:", "Partition Key Structure:"):
            in_section = "partitions"
            continue
        if line == "Columns:":
            in_section = "columns"
            continue
        if not line or line.startswith(("Table Properties:", "===")):
            in_section = None
            continue

        if not in_section or not current_table:
            continue

        parts = line.split()
        if (
            not parts
            or parts[0] in ("Name", "Type", "Comment")
            or parts[0].startswith("-")
        ):
            continue

        col_name = parts[0]
        if in_section == "partitions":
            pkeys.setdefault(current_table, []).append(col_name)
        elif _DATE_COL_RE.search(col_name):
            date_cols.setdefault(current_table, []).append(col_name)

    return pkeys, date_cols


def _format_row_count_abbreviated(raw: str) -> str:
    """Convert a possibly comma-formatted row count string into a ~NM/K shorthand."""
    try:
        n = int(raw.replace(",", ""))
    except (ValueError, TypeError):
        return raw
    if n >= 1_000_000_000:
        return f"~{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"~{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"~{n / 1_000:.1f}K"
    return str(n)


def _build_hints(
    tables: list[str],
    rows: dict[str, str],
    pkeys: dict[str, list[str]],
    date_cols: dict[str, list[str]],
    include_footer: bool,
) -> str:
    """Produce the metastore-derived hint block injected into the prompt."""
    out: list[str] = [
        "=== METASTORE HINTS ===",
    ]

    if include_footer and tables:
        out.append("Tables in context: " + ", ".join(tables))
        out.append(
            "→ 'Tables used:' must list only the tables your query actually reads (FROM / JOIN)."
        )

    if rows:
        out.append("Row counts from HMS stats:")
        for t, r in sorted(rows.items()):
            out.append(f"  {t}: {_format_row_count_abbreviated(r)} rows (raw: {r})")
        if include_footer:
            out.append(
                "→ 'Estimated rows:' must cite one of the numbers above; "
                "when a WHERE clause filters rows, show both sides, e.g. ~5K (vs ~100K without filter)."
            )
    else:
        out.append("Row counts unavailable in HMS stats.")
        if not include_footer:
            out.append("→ Footer lines must be omitted when stats are unknown.")

    if include_footer and pkeys:
        out.append("Partition columns (always filter on these to avoid full scans):")
        for t, ks in sorted(pkeys.items()):
            out.append(f"  {t}: {', '.join(ks)}")
        out.append(
            "→ 'Partition filter:' must show the exact filter applied, "
            "e.g. 'year=2025, month=1 [applied]'. If the question has no time scope, "
            "pick the most recent available partition value from the Partitions section."
        )
    elif include_footer:
        out.append(
            "→ 'Partition filter:' write 'None [not partitioned]' — "
            "no partition pruning is possible for these tables."
        )

    all_date_cols = {col for cols in date_cols.values() for col in cols}
    if all_date_cols:
        out.append(
            "Date/time columns detected (prefer these in ORDER BY / WHERE for time-based queries): "
            + ", ".join(sorted(all_date_cols))
        )

    out.append("=== END HINTS ===")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# Query-type detection
# ---------------------------------------------------------------------------

# Order matters — first match wins.
_QUERY_TYPE_RULES: list[tuple[list[str], str]] = [
    (
        ["top", "rank", "ranking", "most", "highest", "best", "least", "lowest", "bottom",
         "largest", "smallest", "greatest", "first", "last"],
        "aggregation with ORDER BY … DESC/ASC LIMIT N",
    ),
    (
        ["trend", "over time", "per day", "per month", "per year", "daily", "monthly",
         "weekly", "yearly", "quarterly", "by date", "by month", "by year", "by week",
         "time series", "historical", "growth"],
        "time-series aggregation grouped by date/period column",
    ),
    (
        ["join", "related", "match", "combine", "link", "across", "between.*table",
         "from.*and.*table"],
        "multi-table join",
    ),
    (
        ["count", "how many", "total", "sum", "average", "avg", "mean", "distinct",
         "unique", "number of", "percentage", "ratio", "breakdown", "distribution"],
        "aggregation or count",
    ),
    (
        ["find", "where", "filter", "lookup", "search", "specific", "record", "row",
         "show me", "get me", "give me", "list", "fetch", "retrieve"],
        "filtered SELECT",
    ),
]


def _detect_query_type(query: str) -> str:
    """
    Return a short Hive-aware hint based on keywords/phrases in the user's question.
    Uses ordered rules so more specific patterns take priority over generic ones.
    """
    q = query.lower()
    for keywords, label in _QUERY_TYPE_RULES:
        if any(re.search(kw, q) for kw in keywords):
            return label
    return "general SELECT"


def _default_limit(query: str, query_type: str) -> str:
    """
    Derive a sensible default LIMIT.
    - Top-N / ranking queries default to 10.
    - Time-series queries default to 30 periods.
    - Everything else defaults to 100.
    """
    q = query.lower()

    m = re.search(r"\b(?:top|first|last|bottom)\s+(\d+)\b", q)
    if m:
        return m.group(1)

    if "aggregation with ORDER BY" in query_type:
        return "10"
    if "time-series" in query_type:
        return "30"
    return "100"


# ---------------------------------------------------------------------------
# Output-format example generator
# ---------------------------------------------------------------------------

_SQL_FENCE_OPEN = "```sql\n-- NO BACKTICKS: use plain identifiers\n"
_SQL_FENCE_CLOSE = "```\n"


def _generate_example_query(
    query_type: str,
    pkeys: dict[str, list[str]],
    limit: str,
    include_footer: bool,
) -> str:
    """Return a generic output-format example that illustrates the required structure."""
    partition_footer = (
        "Partition filter: <key>=<value> [applied]"
        if pkeys
        else "Partition filter: None [not partitioned]"
    )

    if "time-series" in query_type:
        sql = (
            f"SELECT t.<date_col>, COUNT(*) AS <metric>\n"
            f"FROM <db>.<table> t\n"
            f"WHERE t.<partition_key> = <value>\n"
            f"GROUP BY t.<date_col>\n"
            f"ORDER BY t.<date_col> DESC\n"
            f"LIMIT {limit};\n"
        )
        footer = "Tables used: <db>.<table>\n{{pf}}\nEstimated rows: ~<N> (vs ~<M> without filter)"
    elif "multi-table join" in query_type:
        sql = (
            f"SELECT a.<col1>, b.<col2>\n"
            f"FROM <db>.<table1> a\n"
            f"JOIN <db>.<table2> b ON a.<key> = b.<key>\n"
            f"WHERE a.<partition_key> = <value>\n"
            f"ORDER BY <col> DESC\n"
            f"LIMIT {limit};\n"
        )
        footer = "Tables used: <db>.<table1>, <db>.<table2>\n{{pf}}\nEstimated rows: ~<N> (vs ~<M> without filter)"
    elif "aggregation" in query_type:
        sql = (
            f"SELECT <group_col>, COUNT(*) AS <cnt_alias>, SUM(<metric_col>) AS <sum_alias>\n"
            f"FROM <db>.<table> t\n"
            f"WHERE t.<partition_key> = <value>\n"
            f"GROUP BY <group_col>\n"
            f"ORDER BY <sum_alias> DESC\n"
            f"LIMIT {limit};\n"
        )
        footer = "Tables used: <db>.<table>\n{{pf}}\nEstimated rows: ~<N> (vs ~<M> without filter)"
    else:
        sql = (
            f"SELECT t.<col1>, t.<col2>\n"
            f"FROM <db>.<table> t\n"
            f"WHERE t.<filter_col> = '<value>'\n"
            f"LIMIT {limit};\n"
        )
        footer = "Tables used: <db>.<table>\n{{pf}}\nEstimated rows: ~<N> (vs ~<M> without filter)"

    output = _SQL_FENCE_OPEN + sql + _SQL_FENCE_CLOSE
    if include_footer:
        output += footer.format(pf=partition_footer)
    return output


# ---------------------------------------------------------------------------
# Safety guards
# ---------------------------------------------------------------------------

# Broad write-verb check — matched against the raw user input before any tools are called.
# A single bare keyword is enough to refuse: requiring a second structural word (table,
# record, etc.) caused legitimate write requests to slip through when phrased naturally
# (e.g. "delete inactive users", "insert the missing rows").
_WRITE_VERB_RE = re.compile(
    r"\b(delete|insert|update|drop|truncate|alter|create|merge|overwrite)\b",
    re.IGNORECASE,
)


def _strip_backticks(text: str) -> str:
    """Remove backtick quoting so the LLM writes plain identifiers."""
    return text.replace("`", "")


def _is_error_context(ctx: str) -> bool:
    """Return True when assembled context is just an upstream discovery failure."""
    for line in ctx.splitlines():
        line = line.strip()
        if line.startswith(("No tables found", "Error ", "Error:")):
            return True
    return False


def _is_write_operation_request(query: str) -> tuple[bool, str]:
    """
    Detect whether the natural language query is requesting a write operation.

    Uses a broad single-verb regex so that naturally phrased requests
    ("delete inactive users", "insert the missing rows") are caught without
    requiring a second structural keyword nearby.

    Returns:
        (is_write, operation_type): True if a write verb is detected, with the
        matched verb in upper case.
    """
    m = _WRITE_VERB_RE.search(query)
    if m:
        return True, m.group(1).upper()
    return False, ""


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------


async def handle_text_to_hiveql(natural_query: str, assembled_context: str) -> str:
    """
    Format schema/partition context and a natural language query into a structured
    prompt that instructs the LLM to produce a safe, well-formed HiveQL SELECT query.

    This tool only generates SELECT queries. Write operations are rejected before the
    prompt is assembled.
    """
    try:
        if not natural_query.strip():
            return "Error: natural_query cannot be empty."
        if not assembled_context.strip():
            return (
                "Error: assembled_context is empty. "
                "Run search_tables and get_table_schema first, then pass that output here."
            )
        if _is_error_context(assembled_context):
            return (
                "It looks like the previous table search or metadata lookup failed. "
                "Please clarify the table name or search with different keywords before writing SQL. "
                f"Context received: {assembled_context}"
            )

        is_write, operation = _is_write_operation_request(natural_query)
        if is_write:
            return (
                f"Error: {operation} operations are not supported.\n\n"
                "This tool generates SELECT queries only. To modify data, use Beeline or Hive CLI directly.\n\n"
                f"Blocked request: '{natural_query.strip()}'"
            )

        tables = _parse_tables(assembled_context)
        rows = _parse_row_counts(assembled_context)
        pkeys, date_cols = _parse_columns_and_partitions(assembled_context)

        # Parse before stripping so regex anchors match the original format
        clean_context = _strip_backticks(assembled_context)

        query_type = _detect_query_type(natural_query)
        limit = _default_limit(natural_query, query_type)
        include_footer = bool(rows)
        hints = _build_hints(tables, rows, pkeys, date_cols, include_footer)
        example = _generate_example_query(query_type, pkeys, limit, include_footer)

        output_instruction = (
            "Output ONLY the SQL query in a ```sql block. Do not include footer lines."
            if not include_footer
            else "Output ONLY the SQL and the required footer lines."
        )

        prompt = "\n\n".join([
            _SYSTEM_RULES,
            _QUERY_HINTS,
            _OUTPUT_FORMAT,
            output_instruction,
            f"Request: {natural_query.strip()}",
            f"Detected query pattern: {query_type}",
            hints,
            # TODO: semantic column prioritization — replace full schema pass-through with
            # a selection step that surfaces partition keys, numeric metrics, low-cardinality
            # dimensions, and join keys in priority order. This is the highest-impact future
            # improvement for SQL quality.
            "Full metastore context:\n" + clean_context.strip(),
            f"Add LIMIT {limit} unless the question specifies a different row count.\n\n"
            "=== OUTPUT FORMAT ===\n" + example,
        ])

        return prompt

    except Exception as exc:
        logger.exception("text_to_hiveql failed")
        return f"Error preparing SQL generation context: {exc}"


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
  and follows Hive best practices based on the current HMS metadata."
\
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

## STEP 2 — DISCOVER AND VALIDATE ALL TABLES

Extract every table referenced in the query (FROM clause, JOINs, subqueries).

For each table, in this exact order:

1. Call search_tables() to confirm the table exists in HMS.

   If search_tables() returns no match for a table, respond with:

     "I couldn't find the table '{{db}}.{{table}}' in the Hive Metastore.
     Please check the table name and database, then try again."

   Stop. Do not attempt to optimize a query against a table that does not
   exist in HMS. Do not guess the schema.

2. Call get_table_schema() to retrieve:
   - Column names and types (used for type mismatch and SELECT * checks)
   - Partition key definitions (used for partition predicate checks)
   - Storage format and table properties including bucketing_version
     (used for bucketing alignment check)
   - Transactional mode: transactional + transactional_properties
     (insert_only = no DELETE/UPDATE support even if transactional=true)

3. Call get_partitions() to retrieve:
   - Partition key structure and sample partition values
   - Used to verify partition filter coverage and suggest correct values

4. Call get_table_stats() to retrieve:
   - Row count, total file size, file count
   - If stats return unknown, note this explicitly in the report.
     Do not estimate or fabricate row counts.
   - Note: on Cloudera CDP, managed Parquet tables default to insert_only
     transactional mode. HMS stats are only populated after
     ANALYZE TABLE ... COMPUTE STATISTICS is run explicitly.

---

## STEP 3 — ANALYZE FOR ANTI-PATTERNS

Using only the metadata returned by the tools in Step 2:

{_ANTIPATTERN_RULES}

---

## STEP 4 — PRODUCE THE REPORT

{_OPTIMIZE_OUTPUT_FORMAT}

---

## HARD RULES

- Never invent schema, partition keys, column types, or row counts.
  Every claim must come from an HMS tool result.

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
  PARTITION (partition_key) to enable row count estimates."
\
"""


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


async def handle_optimize_query(submitted_query: str, assembled_context: str) -> str:
    """
    Format HMS metadata context and a submitted HiveQL query into a structured
    prompt that instructs the LLM to produce an optimization report.

    Workflow mirrors handle_text_to_hiveql:
      - write operations are rejected before the prompt is assembled
      - error contexts (upstream discovery failures) are surfaced clearly
      - HMS metadata is parsed and injected as structured hints

    The caller is responsible for running search_tables, get_table_schema,
    get_partitions, and get_table_stats before calling this handler, and
    passing their combined output as assembled_context.
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
                "The table metadata lookup failed before optimization could begin. "
                "Please verify the table name and database, then try again.\n\n"
                f"Context received:\n{assembled_context}"
            )

        is_write, operation = _is_write_operation_request(submitted_query)
        if is_write:
            return (
                f"Error: {operation} operations are not supported.\n\n"
                "optimize_query analyzes SELECT queries only. "
                "To run write operations, use Beeline or the Hive CLI directly.\n\n"
                f"Blocked request: '{submitted_query.strip()}'"
            )

        tables   = _parse_tables(assembled_context)
        rows     = _parse_row_counts(assembled_context)
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
