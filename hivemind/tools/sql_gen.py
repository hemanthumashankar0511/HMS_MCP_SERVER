from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

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
        # Top-level section header — track current table for both block types
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
) -> str:
    """Produce the metastore-derived hint block injected into the prompt."""
    out: list[str] = [
        "=== METASTORE HINTS (use these to fill the three footer lines) ===",
    ]

    if tables:
        out.append("Tables in context: " + ", ".join(tables))
        out.append(
            "→ 'Tables used:' must list only the tables your query actually reads (FROM / JOIN)."
        )

    if rows:
        out.append("Row counts from HMS stats:")
        for t, r in sorted(rows.items()):
            out.append(f"  {t}: {_format_row_count_abbreviated(r)} rows (raw: {r})")
        out.append(
            "→ 'Estimated rows:' must cite one of the numbers above; "
            "when a WHERE clause filters rows, show both sides, e.g. ~5K (vs ~100K without filter)."
        )
    else:
        out.append("→ 'Estimated rows:' write 'unknown (no HMS stats)' if row counts are absent.")

    if pkeys:
        out.append("Partition columns (always filter on these to avoid full scans):")
        for t, ks in sorted(pkeys.items()):
            out.append(f"  {t}: {', '.join(ks)}")
        out.append(
            "→ 'Partition filter:' must show the exact filter applied, "
            "e.g. 'year=2025, month=1 [applied]'. If the question has no time scope, "
            "pick the most recent available partition value from the Partitions section."
        )
    else:
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


# Query-type keyword groups — order matters (first match wins)
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
    - Top-N / ranking queries default to 10 (more useful than 5 in most cases).
    - Time-series queries don't need a hard cap (30 periods is a reasonable default).
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


_SQL_FENCE_OPEN = "```sql\n-- NO BACKTICKS: use plain identifiers\n"
_SQL_FENCE_CLOSE = "```\n"


def _generate_example_query(query_type: str, pkeys: dict[str, list[str]], limit: str) -> str:
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

    return _SQL_FENCE_OPEN + sql + _SQL_FENCE_CLOSE + footer.format(pf=partition_footer)


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


async def handle_text_to_hiveql(natural_query: str, assembled_context: str) -> str:
    """
    Formats the schema/partition context and user query into a structured prompt
    that instructs the LLM to produce a safe, well-formed HiveQL query.
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

        # Parse before stripping so regex anchors match the original format
        tables = _parse_tables(assembled_context)
        rows = _parse_row_counts(assembled_context)
        pkeys, date_cols = _parse_columns_and_partitions(assembled_context)

        clean_context = _strip_backticks(assembled_context)

        query_type = _detect_query_type(natural_query)
        limit = _default_limit(natural_query, query_type)
        hints = _build_hints(tables, rows, pkeys, date_cols)
        example = _generate_example_query(query_type, pkeys, limit)

        prompt = "\n".join([
            "CRITICAL: Do NOT use backticks anywhere in the SQL. "
            "Write plain unquoted identifiers: database.table, alias.column.",
            "",
            "You MUST write a complete, runnable HiveQL query. No hedging. No refusal.",
            "The user already has a cluster and knows how to run it.",
            "Output ONLY the SQL and the required footer. Do NOT add conversational filler like 'Run this on your cluster' or 'I only have metadata'.",
            "",
            f"Request: {natural_query.strip()}",
            f"Detected query pattern: {query_type}",
            "",
            hints,
            "",
            "Full metastore context:",
            clean_context.strip(),
            "",
            "Rules (keep the query simple and easy to read):",
            "1. NO backticks — plain identifiers only: database.table, alias.column.",
            "2. Filter on partition keys first in WHERE to avoid full table scans.",
            "3. Use short 1-2 letter aliases for tables in JOINs.",
            f"4. Add LIMIT {limit} unless the question asks for a different number.",
            "5. SELECT only — no INSERT, UPDATE, DROP, CREATE, or any other write statements.",
            "6. Use only tables and columns from the metastore context above.",
            "",
            "=== OUTPUT FORMAT ===",
            example,
        ])

        return prompt

    except Exception as exc:
        logger.exception("text_to_hiveql failed")
        return f"Error preparing SQL generation context: {exc}"
