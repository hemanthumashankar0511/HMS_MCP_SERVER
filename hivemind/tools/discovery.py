from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from hivemind.hms_client import _FORMAT_ALIASES

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


def _bulk_partition_stats(
    client: "HMSClient",
    database: str,
    table: str,
    part_names: list[str],
    part_key_names: list[str],
) -> dict[str, dict[str, str]]:
    """
    Fetch BASIC_STATS for the sampled partitions in a single round-trip when possible.

    Falls back to a per-partition lookup only for names the bulk call did not return
    (e.g. values HMS escapes in partition-name form), so the result is identical to
    the previous per-partition behavior while collapsing the common case to one call.
    """
    try:
        stats = client.get_partition_basic_stats_bulk(
            database, table, part_names, part_key_names
        )
    except Exception:
        stats = {}
    for pn in part_names:
        if pn not in stats:
            try:
                stats[pn] = client.get_partition_basic_stats(database, table, pn)
            except Exception:
                pass
    return stats


def _append_partition_basic_stats(
    lines: list[str],
    client: "HMSClient",
    database: str,
    table: str,
    part_key_names: list[str],
    max_parts: int = 20,
) -> None:
    """Append sampled partition BASIC_STATS without treating them as table-level stats."""
    lines.append("Partition-level BASIC_STATS sample (same cap as get_partitions):")
    try:
        part_names = client.get_partition_names(database, table, max_parts=max_parts)
    except Exception as exc:
        lines.append(f"  (could not list partitions: {exc})")
        return

    if not part_names:
        lines.append("  No partition data found (table may be empty or not yet populated).")
        return

    bulk_stats = _bulk_partition_stats(client, database, table, part_names, part_key_names)

    sum_rows = 0
    parts_with_rows = 0
    detail: list[str] = []
    for pn in part_names:
        pst = bulk_stats.get(pn)
        if pst is None:
            detail.append(f"  {pn} (metadata error: stats unavailable)")
            continue

        nr = pst["num_rows"]
        nf = pst["num_files"]
        sz = pst["total_size"]
        rows_i = _stat_int(nr)
        if rows_i >= 0:
            sum_rows += rows_i
            parts_with_rows += 1

        if nr == "-1" and nf == "-1" and sz == "-1":
            detail.append(f"  {pn}  (no BASIC_STATS — run ANALYZE TABLE ... PARTITION)")
        else:
            detail.append(
                f"  {pn}  rows={_format_count(nr)}  "
                f"files={_format_count(nf)}  size={_format_bytes(sz)}"
            )

    lines.extend(detail)
    suffix = ""
    if len(part_names) >= max_parts:
        suffix = " Partial sum — up to 20 partitions sampled; analyze all partitions separately if needed."
    if parts_with_rows:
        lines.append("")
        lines.append(
            f"  Sum(rows) over partitions with usable numRows ({parts_with_rows} partition(s)): "
            f"{sum_rows:,}{suffix}"
        )
    else:
        lines.append("")
        lines.append(
            "  No partition-level numRows in HMS sample. "
            "Run ANALYZE TABLE ... PARTITION(...) COMPUTE STATISTICS."
        )


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
    if len(results) == 20:
        lines.append("")
        lines.append("Note: results capped at 20. Narrow your search if needed.")
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

        if not stats["stats_available"]:
            lines.append("")
            if part_keys:
                lines.append(
                    "Table-level numRows absent in HMS. "
                    "Use the partition sample below for visibility, or run "
                    f"ANALYZE TABLE {database}.{table} COMPUTE STATISTICS for table-level totals."
                )
            else:
                lines.append(
                    "Table-level numRows absent in HMS. "
                    f"Run ANALYZE TABLE {database}.{table} COMPUTE STATISTICS."
                )

        # Partitioned tables can carry table-level and partition-level stats
        # independently, so always show the partition sample when keys exist.
        if part_keys:
            lines.append("")
            part_key_names = [pk["name"] for pk in part_keys]
            _append_partition_basic_stats(lines, client, database, table, part_key_names)

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

    try:
        # TODO: add output cap; tables with many partitions can return unbounded partition lists
        part_names = client.get_partition_names(database, table, max_parts=20)
    except Exception as exc:
        lines.append(f"\nCould not fetch partition values: {exc}")
        return "\n".join(lines)

    lines.append("")
    if part_names:
        lines.append(
            f"Showing {len(part_names)} sample partition(s); HMS BASIC_STATS per partition:"
        )
        part_key_names = [pk["name"] for pk in part_keys]
        bulk_stats = _bulk_partition_stats(client, database, table, part_names, part_key_names)
        for p in part_names:
            lines.append(f"  {p}")
            pst = bulk_stats.get(p)
            if pst is None:
                lines.append("    (could not load stats: stats unavailable)")
                continue
            nr, nf, ts = pst["num_rows"], pst["num_files"], pst["total_size"]
            if nr == "-1" and nf == "-1" and ts == "-1":
                lines.append(
                    "    rows=unknown  files=unknown  size=unknown  "
                    "(run ANALYZE TABLE ... PARTITION (...) COMPUTE STATISTICS)"
                )
            else:
                lines.append(
                    f"    rows={_format_count(nr)}  "
                    f"files={_format_count(nf)}  "
                    f"size={_format_bytes(ts)}"
                )
    else:
        lines.append("No partition data found (table may be empty or not yet populated).")

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
