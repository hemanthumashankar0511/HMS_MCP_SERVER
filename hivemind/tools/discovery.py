from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from hivemind.hms_client import (
    _FORMAT_ALIASES,
    _PARTITION_ROLLUP_BATCH,
    PARTITION_ROLLUP_CAP,
    PARTITION_SAMPLE_CAP,
)

if TYPE_CHECKING:
    from hivemind.hms_client import HMSClient

logger = logging.getLogger(__name__)


def _is_missing_stat(raw: str | None) -> bool:
    if raw in (None, "", "N/A"):
        return True
    text = str(raw).strip()
    if not text:
        return True
    try:
        return int(float(text)) < 0
    except (ValueError, TypeError):
        return text.lower() in {"unknown", "na"}


def _stat_int(raw: str | None) -> int:
    """Parse HMS numRows/numFiles/totalSize-style string; -1 means unknown."""
    if raw is None:
        return -1
    text = str(raw).strip().replace(",", "")
    if text in {"", "-1", "N/A"}:
        return -1
    try:
        n = int(float(text))
        return n if n >= 0 else -1
    except (ValueError, TypeError):
        return -1


def _format_bytes(raw: str) -> str:
    if _is_missing_stat(raw):
        return "unknown"
    try:
        n = int(float(str(raw).strip().replace(",", "")))
    except (ValueError, TypeError):
        return raw
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def _format_count(raw: str) -> str:
    if _is_missing_stat(raw):
        return "unknown"
    try:
        return f"{int(raw):,}"
    except (ValueError, TypeError):
        return raw


def _format_table_type(table_type: str) -> str:
    return {
        "MANAGED_TABLE": "Managed",
        "EXTERNAL_TABLE": "External",
        "VIRTUAL_VIEW": "View",
        "MATERIALIZED_VIEW": "Materialized View",
    }.get(table_type, table_type)


def _collect_partition_stats(
    client: "HMSClient",
    database: str,
    table: str,
    part_names: list[str],
    part_key_names: list[str],
) -> dict[str, dict[str, str]]:
    """
    Fetch BASIC_STATS for many partitions, batched, preserving input order.

    Issues one get_partitions_by_names round-trip per _PARTITION_ROLLUP_BATCH chunk
    and falls back to a per-partition lookup only for names the bulk call did not
    return (e.g. values HMS escapes in partition-name form). Keeping the result
    insertion-ordered lets the caller both sum every partition and slice a sample
    from the front.
    """
    out: dict[str, dict[str, str]] = {}
    for i in range(0, len(part_names), _PARTITION_ROLLUP_BATCH):
        chunk = part_names[i:i + _PARTITION_ROLLUP_BATCH]
        try:
            batch = client.get_partition_basic_stats_bulk(
                database, table, chunk, part_key_names
            )
        except Exception:
            batch = {}
        for pn in chunk:
            if pn in batch:
                out[pn] = batch[pn]
            else:
                try:
                    out[pn] = client.get_partition_basic_stats(database, table, pn)
                except Exception:
                    pass
    return out


def _parse_partition_name(pn: str) -> dict[str, str]:
    """Parse 'key=value/key2=value2' into {'key': 'value', 'key2': 'value2'}."""
    result: dict[str, str] = {}
    for segment in pn.split("/"):
        if "=" in segment:
            k, _, v = segment.partition("=")
            result[k] = v
    return result


def _partition_key_ranges(part_names: list[str]) -> dict[str, dict[str, str]]:
    """Return min/max value per partition key across all fetched partition names."""
    ranges: dict[str, dict[str, str]] = {}
    for pn in part_names:
        for k, v in _parse_partition_name(pn).items():
            if k not in ranges:
                ranges[k] = {"min": v, "max": v}
            else:
                if v < ranges[k]["min"]:
                    ranges[k]["min"] = v
                if v > ranges[k]["max"]:
                    ranges[k]["max"] = v
    return ranges


def _compute_partition_rollup(
    client: "HMSClient",
    database: str,
    table: str,
    part_key_names: list[str],
) -> dict:
    """
    Derive table-level BASIC_STATS for a partitioned table by aggregating partitions.

    Hive does not store a table-level numRows/totalSize/numFiles for partitioned
    tables and no HMS API computes it, so we enumerate partitions (up to
    PARTITION_ROLLUP_CAP) and sum each metric. This mirrors how Hive's own optimizer
    derives the row count for a partitioned table.

    Returns a dict with:
      stats_map  - {partition_name: {num_rows, num_files, total_size}}, insertion-ordered
      scanned    - number of partitions actually scanned
      truncated  - True if the table likely has more partitions than the cap
      sum_rows / sum_files / sum_size       - aggregated totals (ints)
      rows_cov / files_cov / size_cov       - partitions that contributed each metric
    On a listing failure an {"error": str} dict is returned instead.
    """
    try:
        part_names = client.get_partition_names(
            database, table, max_parts=PARTITION_ROLLUP_CAP
        )
    except Exception as exc:
        return {"error": str(exc)}

    truncated = len(part_names) >= PARTITION_ROLLUP_CAP
    stats_map = _collect_partition_stats(
        client, database, table, part_names, part_key_names
    )

    sum_rows = sum_files = sum_size = 0
    rows_cov = files_cov = size_cov = 0
    for pst in stats_map.values():
        r = _stat_int(pst["num_rows"])
        f = _stat_int(pst["num_files"])
        s = _stat_int(pst["total_size"])
        if r >= 0:
            sum_rows += r
            rows_cov += 1
        if f >= 0:
            sum_files += f
            files_cov += 1
        if s >= 0:
            sum_size += s
            size_cov += 1

    return {
        "stats_map": stats_map,
        "part_names": part_names,
        "key_ranges": _partition_key_ranges(part_names),
        "scanned": len(part_names),
        "truncated": truncated,
        "sum_rows": sum_rows,
        "sum_files": sum_files,
        "sum_size": sum_size,
        "rows_cov": rows_cov,
        "files_cov": files_cov,
        "size_cov": size_cov,
    }


def _append_derived_table_stats(lines: list[str], rollup: dict) -> bool:
    """
    Append table-level totals derived by aggregating partition BASIC_STATS.

    Returns True if any derived total was emitted, False otherwise (e.g. no
    partitions have stats, or the partition listing failed).
    """
    if "error" in rollup or not rollup.get("part_names"):
        return False
    if not (rollup["rows_cov"] or rollup["size_cov"] or rollup["files_cov"]):
        return False

    scanned = rollup["scanned"]
    if rollup["truncated"]:
        note = (
            f" [partial: first {scanned} partitions scanned; table has more — "
            "treat totals as a lower bound]"
        )
    else:
        note = f" (aggregated from all {scanned} partition(s))"
    lines.append(
        "Derived table-level totals (Hive does not roll partition BASIC_STATS "
        f"into table-level stats for partitioned tables){note}:"
    )

    def _line(label: str, value: str, cov: int) -> str:
        if cov:
            return f"  {label}: {value}  (from {cov}/{scanned} partition(s))"
        return f"  {label}: unknown"

    lines.append(_line("Rows      ", f"{rollup['sum_rows']:,}", rollup["rows_cov"]))
    lines.append(
        _line("Total size", _format_bytes(str(rollup["sum_size"])), rollup["size_cov"])
    )
    lines.append(_line("Files     ", f"{rollup['sum_files']:,}", rollup["files_cov"]))
    return True


def _append_partition_basic_stats(
    lines: list[str],
    rollup: dict,
    sample_cap: int = PARTITION_SAMPLE_CAP,
) -> None:
    """Append partition key ranges and a head/tail BASIC_STATS sample from a rollup.

    Shows up to sample_cap partition lines split evenly between the first and last
    partitions in HMS order, with an omission line in between when the total exceeds
    the cap.  A key-range summary block always precedes the per-partition lines so
    the LLM can reason about the covered date/value range without reading every row.
    """
    if "error" in rollup:
        lines.append(f"Partition-level BASIC_STATS: could not list partitions: {rollup['error']}")
        return

    part_names: list[str] = rollup.get("part_names", [])
    stats_map: dict[str, dict[str, str]] = rollup.get("stats_map", {})
    if not part_names:
        lines.append(
            "Partition-level BASIC_STATS: no partition data found "
            "(table may be empty or not yet populated)."
        )
        return

    scanned = rollup["scanned"]

    # --- key ranges block ---
    key_ranges = rollup.get("key_ranges", {})
    if key_ranges:
        lines.append(f"Partition key ranges (from {scanned} partition(s)):")
        for k, rng in key_ranges.items():
            lines.append(f"  {k}: min={rng['min']}  max={rng['max']}")

    # --- head / tail sample ---
    half = max(1, sample_cap // 2)
    if scanned <= sample_cap:
        sample_head = part_names
        sample_tail: list[str] = []
        omitted = 0
    else:
        sample_head = part_names[:half]
        sample_tail = part_names[-half:]
        omitted = scanned - 2 * half

    lines.append(
        f"Partition-level BASIC_STATS sample ({min(scanned, sample_cap)} of "
        f"{scanned} partition(s); totals above cover all):"
    )

    def _stat_line(pn: str) -> str:
        pst = stats_map.get(pn)
        if not pst:
            return f"  {pn}  (stats not fetched)"
        nr, nf, sz = pst["num_rows"], pst["num_files"], pst["total_size"]
        if nr == "-1" and nf == "-1" and sz == "-1":
            return f"  {pn}  (no BASIC_STATS — run ANALYZE TABLE ... PARTITION)"
        return (
            f"  {pn}  rows={_format_count(nr)}  "
            f"files={_format_count(nf)}  size={_format_bytes(sz)}"
        )

    for pn in sample_head:
        lines.append(_stat_line(pn))
    if omitted > 0:
        lines.append(f"  ... {omitted} partition(s) omitted ...")
    for pn in sample_tail:
        lines.append(_stat_line(pn))


async def handle_list_databases(client: "HMSClient") -> str:
    try:
        dbs = client.get_all_databases()
    except Exception as exc:
        logger.exception("list_databases failed")
        return f"Error listing databases: {exc}"

    if not dbs:
        return "No databases found in HMS."

    lines = [f"Databases in Hive Metastore ({len(dbs)} total)", "=" * 40]
    for db in dbs:
        lines.append(f"  {db}")
    lines.append("")
    lines.append("Tip: use list_tables with a database name to see its tables.")
    return "\n".join(lines)


async def handle_list_tables(client: "HMSClient", database: str) -> str:
    try:
        # TODO: add output cap; very large databases can return thousands of table names
        tables = client.get_all_tables(database)
    except Exception as exc:
        logger.exception("list_tables failed for %s", database)
        return f"Error listing tables in '{database}': {exc}"

    if not tables:
        return f"No tables found in database '{database}'."

    lines = [f"Tables in '{database}' ({len(tables)} total)", "=" * 40]
    for t in tables:
        lines.append(f"  {t}")
    lines.append("")
    lines.append(f"Tip: use get_table_schema with database='{database}' and table=<name> to see columns.")
    return "\n".join(lines)


async def handle_search_tables(
    client: "HMSClient", keyword: str, database: str | None = None
) -> str:
    try:
        results = client.search_tables(keyword, database)
    except Exception as exc:
        logger.exception("search_tables failed for keyword=%s", keyword)
        return f"Error searching for '{keyword}': {exc}"

    scope = f"database '{database}'" if database else "all databases"

    if not results:
        return (
            f"No tables found matching '{keyword}'.\nSearched {scope}.\n"
            "Try a shorter keyword or check the spelling."
        )

    lines = [
        f"Search results for '{keyword}' in {scope} ({len(results)} match(es))",
        "=" * 55,
        f"{'Database':<20} {'Table':<30} Match reason",
        "-" * 65,
    ]
    for r in results:
        lines.append(f"{r['database']:<20} {r['table']:<30} {r['match_reason']}")
    if len(results) >= 30:
        lines.append("")
        lines.append("Note: results capped at 30. Narrow your search if needed.")
    return "\n".join(lines)


async def handle_get_table_schema(
    client: "HMSClient", database: str, table: str
) -> str:
    try:
        info = client.get_table(database, table)
    except Exception as exc:
        logger.exception("get_table_schema failed for %s.%s", database, table)
        return f"Error fetching schema for '{database}.{table}': {exc}"

    fmt = _FORMAT_ALIASES.get(info["input_format"], info["input_format"].split(".")[-1] if info["input_format"] else "Unknown")
    tbl_type = _format_table_type(info["table_type"])

    lines = [
        f"Schema: {database}.{table}",
        "=" * 50,
        f"  Type    : {tbl_type}",
        f"  Format  : {fmt}",
        f"  Location: {info['location']}",
        "Columns:",
        f"  {'Name':<30} {'Type':<20} Comment",
        "  " + "-" * 65,
    ]
    for col in info["columns"]:
        comment = col["comment"] or ""
        lines.append(f"  {col['name']:<30} {col['type']:<20} {comment}")

    if info["partition_keys"]:
        lines.append("")
        lines.append("Partition Keys:")
        lines.append(f"  {'Name':<30} {'Type':<20} Comment")
        lines.append("  " + "-" * 65)
        for pk in info["partition_keys"]:
            comment = pk["comment"] or ""
            lines.append(f"  {pk['name']:<30} {pk['type']:<20} {comment}")

    if info["parameters"]:
        lines.append("")
        # TODO: add output cap; tables with many custom properties can produce very long output
        lines.append("Table Properties:")
        for k, v in sorted(info["parameters"].items()):
            lines.append(f"  {k} = {v}")

    return "\n".join(lines)


async def handle_get_table_stats(
    client: "HMSClient", database: str, table: str
) -> str:
    """
    Return HMS statistics for a table: table-level row count / size / file count,
    plus a per-partition BASIC_STATS sample for partitioned tables.

    When statistics are missing, the exact ANALYZE TABLE command needed to populate
    them is shown. HiveMind never runs ANALYZE itself — it is a read-only discovery
    tool, and ANALYZE launches a data-reading job that belongs in the user's Hive
    client (Beeline / Hive CLI).
    """
    # request_cache() coalesces the two get_table lookups (stats + partition info)
    # into a single HMS round-trip for the duration of this call.
    with client.request_cache():
        try:
            stats = client.get_table_stats(database, table)
        except Exception as exc:
            logger.exception("get_table_stats failed for %s.%s", database, table)
            return f"Error fetching stats for '{database}.{table}': {exc}"

        lines = [f"Statistics: {database}.{table}", "=" * 50, "Table-level BASIC_STATS:"]
        lines += [
            f"  Rows       : {_format_count(stats['num_rows'])}",
            f"  Total size : {_format_bytes(stats['total_size'])}",
            f"  Files      : {_format_count(stats['num_files'])}",
        ]
        if stats["last_modified"]:
            lines.append(f"  Last DDL   : {stats['last_modified']}")

        try:
            info = client.get_table(database, table)
        except Exception as exc:
            logger.debug("get_table for partition stats failed: %s", exc)
            return "\n".join(lines)

        part_keys = info.get("partition_keys", [])

        # Non-partitioned table: table-level stats are the whole story.
        if not part_keys:
            if not stats["stats_available"]:
                lines.append("")
                lines.append(
                    "Table-level numRows absent in HMS. "
                    f"Run ANALYZE TABLE {database}.{table} COMPUTE STATISTICS."
                )
            return "\n".join(lines)

        # Partitioned table: Hive writes BASIC_STATS per partition and never rolls
        # them into a table-level numRows, so derive the totals by aggregating
        # partitions (this is what Hive's optimizer does internally).
        part_key_names = [pk["name"] for pk in part_keys]
        rollup = _compute_partition_rollup(client, database, table, part_key_names)

        lines.append("")
        derived = _append_derived_table_stats(lines, rollup)

        lines.append("")
        pk_clause = ", ".join(part_key_names)
        if not stats["stats_available"]:
            if derived:
                lines.append(
                    "Note: Hive stores BASIC_STATS per partition and never rolls them "
                    "into a table-level numRows, so the table-level line above stays "
                    "'unknown' even after ANALYZE. The derived totals are the effective "
                    "table-level figures. To (re)compute partition stats, run: "
                    f"ANALYZE TABLE {database}.{table} PARTITION ({pk_clause}) COMPUTE STATISTICS."
                )
            else:
                lines.append(
                    "Table-level numRows absent and no partition BASIC_STATS found in HMS. "
                    f"Run: ANALYZE TABLE {database}.{table} PARTITION ({pk_clause}) "
                    "COMPUTE STATISTICS."
                )

        lines.append("")
        _append_partition_basic_stats(lines, rollup)

        return "\n".join(lines)


async def handle_get_partitions(
    client: "HMSClient", database: str, table: str
) -> str:
    try:
        info = client.get_table(database, table)
    except Exception as exc:
        logger.exception("get_partitions failed for %s.%s", database, table)
        return f"Error fetching table metadata for '{database}.{table}': {exc}"

    part_keys = info.get("partition_keys", [])
    if not part_keys:
        return f"Table '{database}.{table}' is not partitioned."

    lines = [f"Partitions: {database}.{table}", "=" * 50]
    lines.append("Partition Key Structure:")
    lines.append(f"  {'Name':<25} Type")
    lines.append("  " + "-" * 40)
    for pk in part_keys:
        lines.append(f"  {pk['name']:<25} {pk['type']}")

    part_key_names = [pk["name"] for pk in part_keys]
    rollup = _compute_partition_rollup(client, database, table, part_key_names)

    lines.append("")
    if _append_derived_table_stats(lines, rollup):
        lines.append("")

    _append_partition_basic_stats(lines, rollup)

    return "\n".join(lines)


async def handle_get_table_ddl(
    client: "HMSClient", database: str, table: str
) -> str:
    try:
        ddl = client.get_table_ddl(database, table)
    except Exception as exc:
        logger.exception("get_table_ddl failed for %s.%s", database, table)
        return f"Error reconstructing DDL for '{database}.{table}': {exc}"

    return ddl
