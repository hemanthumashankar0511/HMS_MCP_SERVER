from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Pure parsing + formatting helpers for HiveServer2 EXPLAIN plan text.
#
# These functions take the raw plan text returned by HS2 and turn it into a
# structured dict (parse_explain_plan) and a human-readable report block
# (build_hs2_report). They have no PyHive dependency so they can be unit-tested
# without a live cluster.
# ---------------------------------------------------------------------------

_NUM_ROWS_RE = re.compile(r"Num rows:\s*([\d,]+)")
_ALIAS_RE = re.compile(r"^alias:\s*(\S+)")
_FILTEREXPR_RE = re.compile(r"^filterExpr:\s*(.+)$")
_PREDICATE_RE = re.compile(r"^predicate:\s*(.+)$")
_PARTITION_FILTER_RE = re.compile(r"Partition Filters?:\s*(.+)", re.IGNORECASE)
_TYPE_SUFFIX_RE = re.compile(r"\s*\(type:[^)]*\)\s*$")

# Hive 3 CBO EXPLAIN (single-line operators) — e.g.:
#   TableScan [TS_2] (rows=287997024 width=117) db@store_sales,store_sales
#   Filter Operator [FIL_28] (rows=82764 width=114) predicate:((x > 1) and y)
_CBO_TABLESCAN_RE = re.compile(
    r"TableScan\s+\[[^\]]+\]\s+\(rows=(\d+)\s+width=\d+\)\s+[\w.]+\@(\w+)",
)
_CBO_FILTER_RE = re.compile(
    r"Filter Operator\s+\[[^\]]+\]\s+\(rows=(\d+)\s+width=\d+\)\s+predicate:(\([^)]*(?:\([^)]*\)[^)]*)*\))",
)
_CBO_JOIN_BROADCAST_RE = re.compile(r"Map Join Operator.*BROADCAST", re.IGNORECASE)
_CBO_JOIN_SHUFFLE_RE = re.compile(r"Map Join Operator.*(?:PARTITION_ONLY_SHUFFLE|SHUFFLE)", re.IGNORECASE)

# Operator header lines in a Hive plan are either the literal "TableScan" or end
# with "Operator" (e.g. "Filter Operator", "Group By Operator", "Map Join Operator").
_OPERATOR_HEADERS = frozenset({"TableScan"})

# How much raw plan text to keep in the report. Hive plans for wide joins can be
# very large; we cap to keep the assembled prompt within a sane size.
_PLAN_TEXT_CAP = 6000


def _clean_expr(expr: str) -> str:
    """Strip the trailing '(type: ...)' annotation Hive appends to predicates."""
    return _TYPE_SUFFIX_RE.sub("", expr).strip()


def _is_operator_header(line: str) -> bool:
    return line in _OPERATOR_HEADERS or line.endswith("Operator")


def _parse_tree_format(plan_text: str) -> tuple[list[dict], list[dict], list[str]]:
    """Parse legacy multi-line EXPLAIN tree (Statistics / filterExpr lines)."""
    operators: list[dict] = []
    table_scans: list[dict] = []
    partition_filters: list[str] = []
    current: dict | None = None

    for raw in plan_text.splitlines():
        line = raw.strip()
        if not line:
            continue

        if _is_operator_header(line):
            current = {"operator": line, "table": None, "rows": None, "filter": None}
            operators.append(current)
            if line == "TableScan":
                table_scans.append(current)
            continue

        m = _ALIAS_RE.match(line)
        if m and current is not None:
            current["table"] = m.group(1)
            continue

        m = _FILTEREXPR_RE.match(line)
        if m and current is not None:
            current["filter"] = _clean_expr(m.group(1))
            continue

        m = _PREDICATE_RE.match(line)
        if m and current is not None and current.get("filter") is None:
            current["filter"] = _clean_expr(m.group(1))
            continue

        m = _PARTITION_FILTER_RE.search(line)
        if m:
            partition_filters.append(_clean_expr(m.group(1)))
            continue

        m = _NUM_ROWS_RE.search(line)
        if m and current is not None and current["rows"] is None:
            current["rows"] = int(m.group(1).replace(",", ""))
            continue

    return operators, table_scans, partition_filters


def _parse_cbo_format(plan_text: str) -> tuple[list[dict], list[str]]:
    """
    Parse Hive 3 CBO EXPLAIN (inline operators with (rows=N) and db@table).

    Returns table_scans and filter predicates found in the plan text.
    """
    table_scans: list[dict] = []
    for m in _CBO_TABLESCAN_RE.finditer(plan_text):
        table_scans.append({
            "operator": "TableScan",
            "table": m.group(2),
            "rows": int(m.group(1)),
            "filter": None,
        })

    filters: list[str] = []
    for m in _CBO_FILTER_RE.finditer(plan_text):
        filters.append(_clean_expr(m.group(2)))

    return table_scans, filters


def _detect_join_strategy(plan_text: str) -> str | None:
    if _CBO_JOIN_BROADCAST_RE.search(plan_text):
        return "Broadcast join"
    if _CBO_JOIN_SHUFFLE_RE.search(plan_text):
        return "Shuffle / map join"
    return None


def _finalize_parsed(
    operators: list[dict],
    table_scans: list[dict],
    partition_filters: list[str],
    plan_text: str,
    partition_keys: list[str] | None = None,
) -> dict:
    """Compute pruning verdict and dominant scan row count from merged scan list."""
    pkeys = partition_keys or []

    ts_with_filter = [t for t in table_scans if t.get("filter")]
    detected = bool(partition_filters) or bool(ts_with_filter)

    filters: list[str] = list(partition_filters)
    for t in ts_with_filter:
        if t["filter"] and t["filter"] not in filters:
            filters.append(t["filter"])

    # Partition key referenced in a filter predicate counts as pruning evidence.
    for pred in filters:
        for pk in pkeys:
            if pk and pk in pred:
                detected = True
                break

    # Metastore-level partition pruning detection.
    #
    # In Hive 3 on CDP, when a query has direct equality filters on partition keys,
    # Hive resolves them during metastore file-listing (before plan generation).
    # The partition predicates are stripped from the runtime FilterOperator — they
    # never appear in the plan text even though pruning *is* active.
    #
    # Heuristic: if the caller provided partition keys, at least one non-partition
    # runtime predicate exists in an operator, and none of those predicates mention
    # any partition key, the partition predicates were consumed at the metastore
    # level, which means pruning was applied.
    if pkeys and not detected:
        all_op_predicates = [op["filter"] for op in operators if op.get("filter")]
        if all_op_predicates:
            pk_in_runtime = any(
                pk in pred
                for pred in all_op_predicates
                for pk in pkeys
                if pk
            )
            if not pk_in_runtime:
                detected = True
                filters.append(
                    "(inferred: partition predicates absent from runtime filter "
                    "— applied at metastore level before plan generation)"
                )

    ts_rows = [t["rows"] for t in table_scans if isinstance(t["rows"], int) and t["rows"] >= 0]
    # Dominant scan = largest TableScan (fact table in joins), not smallest.
    cbo_est_rows = max(ts_rows) if ts_rows else None

    return {
        "operators": operators,
        "table_scans": table_scans,
        "partition_pruning_detected": detected,
        "partition_filters": filters,
        "cbo_est_rows": cbo_est_rows,
        "join_strategy": _detect_join_strategy(plan_text),
    }


def parse_explain_plan(plan_text: str, partition_keys: list[str] | None = None) -> dict:
    """
    Extract row estimates and partition-pruning evidence from a Hive EXPLAIN plan.

    Supports both formats:
      - Legacy tree EXPLAIN (Statistics: Num rows / filterExpr per operator)
      - Hive 3 CBO EXPLAIN (TableScan [TS_n] (rows=N) db@table inline)

    Returns a dict with operators, table_scans, partition_pruning_detected,
    partition_filters, cbo_est_rows (largest TableScan — dominant fact scan),
    and join_strategy when detectable.
    """
    if plan_text.startswith("Error:") or "[EXPLAIN failed:" in plan_text:
        return _finalize_parsed([], [], [], plan_text, partition_keys)

    tree_ops, tree_scans, tree_part_filters = _parse_tree_format(plan_text)
    cbo_scans, cbo_filters = _parse_cbo_format(plan_text)

    # CBO inline scans are authoritative when present; merge tree scans otherwise.
    if cbo_scans:
        table_scans = cbo_scans
        operators = tree_ops if tree_ops else cbo_scans
    else:
        table_scans = tree_scans
        operators = tree_ops

    partition_filters = list(dict.fromkeys(tree_part_filters + cbo_filters))

    return _finalize_parsed(
        operators, table_scans, partition_filters, plan_text, partition_keys
    )


def pick_focus_table(tables: list[str], pkeys: dict[str, list[str]]) -> str | None:
    """
    Choose the table whose scan matters most for partition-pruning comparison.

    Prefers the first partitioned table in the query; falls back to the first
    table in context when none are partitioned.
    """
    for t in tables:
        if pkeys.get(t):
            return t
    return tables[0] if tables else None


def table_scan_rows_for(parsed: dict, qualified_table: str) -> int | None:
    """Return CBO row estimate for a TableScan matching qualified_table (by suffix)."""
    suffix = qualified_table.split(".")[-1].lower()
    for ts in parsed.get("table_scans", []):
        name = (ts.get("table") or "").lower()
        if name == suffix:
            return ts.get("rows")
    return None


def hms_rows_for_table(rows: dict[str, str], qualified_table: str) -> int | None:
    """Parse HMS row count string for one fully-qualified table."""
    raw = rows.get(qualified_table)
    if raw is None:
        return None
    try:
        n = int(str(raw).replace(",", ""))
        return n if n >= 0 else None
    except (TypeError, ValueError):
        return None


def partition_keys_for_table(pkeys: dict[str, list[str]], qualified_table: str) -> list[str]:
    return pkeys.get(qualified_table, [])


def max_hms_total_rows(rows: dict[str, str]) -> int | None:
    """
    Pick the largest HMS row count among a query's tables as the reduction baseline.

    The CBO scan estimate compares most meaningfully against the dominant (largest)
    table's total. Returns None when no usable count is present. Accepts the
    {table: row_count_str} mapping produced by sql_gen._parse_row_counts.
    """
    best: int | None = None
    for raw in rows.values():
        try:
            n = int(str(raw).replace(",", ""))
        except (TypeError, ValueError):
            continue
        if n >= 0 and (best is None or n > best):
            best = n
    return best


def compute_row_reduction(hms_total_rows: int | None, cbo_est_rows: int | None) -> str:
    """
    Describe the row reduction between an HMS total row count and the CBO scan estimate.

    Never fabricates numbers: if either input is missing it says so explicitly.
    """
    if cbo_est_rows is None:
        return "CBO row estimate unavailable — cannot compute row reduction."

    if hms_total_rows is None or hms_total_rows <= 0:
        return (
            f"Estimated rows scanned (CBO): {cbo_est_rows:,}\n"
            "Total rows (HMS stats): unavailable — cannot compute reduction %."
        )

    if cbo_est_rows >= hms_total_rows:
        return (
            f"Total rows (HMS stats): {hms_total_rows:,}\n"
            f"Estimated rows scanned (CBO): {cbo_est_rows:,}\n"
            "Reduction: ~0% (plan scans the full table — no effective pruning)."
        )

    reduction = (1 - cbo_est_rows / hms_total_rows) * 100
    return (
        f"Total rows (HMS stats): {hms_total_rows:,}\n"
        f"Estimated rows scanned (CBO): {cbo_est_rows:,}\n"
        f"Reduction: ~{reduction:.1f}%"
    )


def _format_operator_table(parsed: dict) -> str:
    """Render parsed TableScan row estimates into a fixed-width table."""
    scans = parsed.get("table_scans") or []
    if not scans:
        scans = [
            op for op in parsed.get("operators", [])
            if op.get("operator") == "TableScan" or op.get("rows") is not None
        ]

    rows: list[tuple[str, str, str]] = []
    for op in scans:
        name = op.get("operator", "TableScan")
        if op.get("table"):
            name = f"{name} {op['table']}"
        est = f"{op['rows']:,}" if isinstance(op.get("rows"), int) else "unknown"
        note = f"filter: {op['filter']}" if op.get("filter") else ""
        rows.append((name, est, note))
        if len(rows) >= 20:
            break

    if not rows:
        return "No operator row estimates found in the plan."

    name_w = max(len("Operator"), *(len(r[0]) for r in rows))
    est_w = max(len("Est. Rows"), *(len(r[1]) for r in rows))

    header = f"{'Operator':<{name_w}} | {'Est. Rows':>{est_w}} | Notes"
    divider = f"{'-' * name_w}-+-{'-' * est_w}-+------"
    lines = [header, divider]
    for name, est, note in rows:
        lines.append(f"{name:<{name_w}} | {est:>{est_w}} | {note}")
    return "\n".join(lines)


def _row_delta_pct(before: int | None, after: int | None) -> str:
    """Express the row count change as a percentage string, or 'unknown'."""
    if before is None or after is None:
        return "unknown"
    if before == 0:
        return "N/A (0 rows before)"
    delta = (before - after) / before * 100
    if delta > 0:
        return f"-{delta:.1f}% ({after:,} vs {before:,})"
    if delta < 0:
        return f"+{abs(delta):.1f}% MORE rows ({after:,} vs {before:,})"
    return "0% (no change)"


def build_comparison_report(
    original_parsed: dict,
    optimized_parsed: dict,
    hms_total_rows: int | None = None,
    table: str | None = None,
    partition_keys: list[str] | None = None,
) -> str:
    """
    Produce a structured side-by-side comparison of two EXPLAIN plans.

    Uses the focus table (partitioned table when available) for row metrics rather
    than the first table in the query or the smallest scan in a join.
    """
    pkeys = partition_keys or []
    focus = table or ""

    rows_before = table_scan_rows_for(original_parsed, focus) if focus else original_parsed.get("cbo_est_rows")
    rows_after = table_scan_rows_for(optimized_parsed, focus) if focus else optimized_parsed.get("cbo_est_rows")

    # Infer pruning on focus table: scan rows dropped materially after rewrite.
    pruning_before = original_parsed.get("partition_pruning_detected", False)
    pruning_after = optimized_parsed.get("partition_pruning_detected", False)
    if (
        rows_before is not None
        and rows_after is not None
        and rows_before > 0
        and rows_after < rows_before * 0.5
    ):
        pruning_after = True

    join_before = original_parsed.get("join_strategy")
    join_after = optimized_parsed.get("join_strategy")

    lines = [
        "PLAN COMPARISON",
        "═══════════════",
    ]
    if focus:
        pk_line = f" (partition key: {', '.join(pkeys)})" if pkeys else ""
        lines.append(f"Focus table : {focus}{pk_line}")
    if hms_total_rows is not None:
        lines.append(f"HMS total rows (table-level stats): {hms_total_rows:,}")
    lines.append("")

    def _rows_str(r: int | None) -> str:
        return f"{r:,}" if r is not None else "unknown"

    lines += [
        f"{'Metric':<22} {'Before':>16}   {'After':>16}   Change",
        "─" * 62,
        f"{'TableScan rows':<22} {_rows_str(rows_before):>16}   {_rows_str(rows_after):>16}   {_row_delta_pct(rows_before, rows_after)}",
        f"{'Partition pruning':<22} {'Yes' if pruning_before else 'No':>16}   {'Yes' if pruning_after else 'No':>16}",
    ]

    if join_before or join_after:
        lines.append(
            f"{'Join strategy':<22} {(join_before or 'unknown'):>16}   {(join_after or 'unknown'):>16}"
        )

    if hms_total_rows and rows_before is not None and rows_after is not None:
        pct_b = (1 - rows_before / hms_total_rows) * 100 if hms_total_rows > rows_before else 0.0
        pct_a = (1 - rows_after / hms_total_rows) * 100 if hms_total_rows > rows_after else 0.0
        lines.append(
            f"{'vs HMS total saved':<22} {pct_b:>15.1f}%   {pct_a:>15.1f}%"
        )

    # Per-table TableScan breakdown (compact).
    all_tables = {
        *(ts.get("table") for ts in original_parsed.get("table_scans", []) if ts.get("table")),
        *(ts.get("table") for ts in optimized_parsed.get("table_scans", []) if ts.get("table")),
    }
    if all_tables:
        lines += ["", "TableScan breakdown:"]
        for tname in sorted(all_tables):
            b = next(
                (ts["rows"] for ts in original_parsed.get("table_scans", []) if ts.get("table") == tname),
                None,
            )
            a = next(
                (ts["rows"] for ts in optimized_parsed.get("table_scans", []) if ts.get("table") == tname),
                None,
            )
            if b is not None or a is not None:
                lines.append(f"  {tname:<20} {_rows_str(b):>12}  →  {_rows_str(a):>12}")

    lines += ["", "VERDICT", "───────"]

    if rows_before is not None and rows_after is not None:
        delta = _row_delta_pct(rows_before, rows_after)
        if rows_after < rows_before:
            lines.append(
                f"The optimized plan scans {_rows_str(rows_after)} rows on the focus table "
                f"vs {_rows_str(rows_before)} before ({delta})."
            )
        elif rows_after == rows_before:
            lines.append("Both plans scan the same number of rows — the rewrite did not reduce I/O.")
        else:
            lines.append(
                f"The optimized plan scans MORE rows ({delta}). Review the rewrite."
            )
    else:
        lines.append(
            "Optimizer row estimates could not be parsed from the EXPLAIN plan. "
            "TableScan Statistics lines may be missing for this statement shape."
        )

    if not pruning_before and pruning_after:
        lines.append("Partition pruning: activated in the optimized plan.")
    elif pruning_before and pruning_after:
        lines.append("Partition pruning: active in both plans.")
    elif not pruning_before and not pruning_after:
        if rows_before and rows_after and rows_after < rows_before:
            lines.append(
                "Scan rows dropped significantly — partition pruning likely applied at the "
                "metastore level even if not shown explicitly in filter predicates."
            )
        else:
            lines.append(
                "No partition pruning detected. Add a direct equality or range filter "
                "on the partition key."
            )

    if join_before and join_after and join_before != join_after:
        lines.append(f"Join improved: {join_before} → {join_after}.")

    return "\n".join(lines)


def format_cbo_scan_summary(parsed: dict, focus_table: str | None = None) -> str:
    """
    Compact, LLM-friendly summary of CBO row estimates from a parsed EXPLAIN plan.

    Designed to be cited directly in optimize_query Impact/Summary lines.
    """
    scans = parsed.get("table_scans") or []
    if not scans:
        return "CBO scan estimates: unavailable (could not parse TableScan rows from EXPLAIN)."

    focus_suffix = focus_table.split(".")[-1].lower() if focus_table else ""
    lines = ["CBO SCAN ESTIMATES (from EXPLAIN — cite these in Impact/Summary):"]
    for ts in scans:
        name = ts.get("table") or "unknown"
        rows = ts.get("rows")
        est = f"{rows:,}" if isinstance(rows, int) else "unknown"
        marker = " ← focus table (partitioned)" if focus_suffix and name.lower() == focus_suffix else ""
        lines.append(f"  {name}: {est} rows{marker}")

    pruning = "yes" if parsed.get("partition_pruning_detected") else "no"
    lines.append(f"Partition pruning: {pruning}")
    if parsed.get("join_strategy"):
        lines.append(f"Join strategy: {parsed['join_strategy']}")
    return "\n".join(lines)


def build_hs2_report(
    plan_text: str,
    parsed: dict,
    hms_total_rows: int | None = None,
    table: str | None = None,
    compact: bool = False,
) -> str:
    """
    Assemble the structured HS2 EXPLAIN report block from a parsed plan.

    compact=True  — used by optimize_query: leads with a concise CBO scan summary
                    for the LLM to cite in Impact lines; omits ROW REDUCTION ESTIMATE
                    and the raw plan dump (the comparison feature handles reduction).
    compact=False — used by explain_query: includes the full ROW REDUCTION ESTIMATE
                    block and the raw plan text (capped at _PLAN_TEXT_CAP chars).
    """
    summary = format_cbo_scan_summary(parsed, table)
    detected = parsed.get("partition_pruning_detected", False)
    filters = parsed.get("partition_filters") or []
    filters_line = "; ".join(filters) if filters else "none"

    sections = [
        "HS2 EXPLAIN PLAN",
        "================",
        "",
        summary,
        "",
        "PARSED ROW ESTIMATES",
        "====================",
        _format_operator_table(parsed),
        "",
        "PARTITION PRUNING",
        "=================",
        f"Detected: {'yes' if detected else 'no'}",
        f"Filters: {filters_line}",
    ]

    if not compact:
        sections += [
            "",
            "ROW REDUCTION ESTIMATE",
            "======================",
        ]
        if table:
            sections.append(f"Table: {table}")
        sections.append(compute_row_reduction(hms_total_rows, parsed.get("cbo_est_rows")))

        plan_display = plan_text.strip()
        if len(plan_display) > _PLAN_TEXT_CAP:
            plan_display = plan_display[:_PLAN_TEXT_CAP] + "\n... (plan truncated)"
        sections += ["", "RAW PLAN", "========", plan_display]

    return "\n".join(sections)
