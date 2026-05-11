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
        n = int(raw)
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
    try:
        stats = client.get_table_stats(database, table)
    except Exception as exc:
        logger.exception("get_table_stats failed for %s.%s", database, table)
        return f"Error fetching stats for '{database}.{table}': {exc}"

    lines = [f"Statistics: {database}.{table}", "=" * 50]

    lines += [
        f"  Rows       : {_format_count(stats['num_rows'])}",
        f"  Total size : {_format_bytes(stats['total_size'])}",
        f"  Files      : {_format_count(stats['num_files'])}",
    ]
    if stats["last_modified"]:
        lines.append(f"  Last DDL   : {stats['last_modified']}")

    # Table-level HMS stats are often absent for partitioned tables; roll up sampled partitions.
    if not stats["stats_available"]:
        try:
            info = client.get_table(database, table)
        except Exception as exc:
            logger.debug("get_table for partition rollup failed: %s", exc)
            return "\n".join(lines)
        if info.get("partition_keys"):
            lines.append("")
            lines.append(
                "Table-level numRows absent in HMS. Sample partition stats (same cap as get_partitions):"
            )
            try:
                part_names = client.get_partition_names(database, table, max_parts=20)
            except Exception as exc:
                lines.append(f"  (could not list partitions: {exc})")
                return "\n".join(lines)
            sum_rows = 0
            parts_with_rows = 0
            detail: list[str] = []
            for pn in part_names:
                try:
                    pst = client.get_partition_basic_stats(database, table, pn)
                except Exception as exc:
                    detail.append(f"  {pn} (metadata error: {exc})")
                    continue
                nr = pst["num_rows"]
                nf = pst["num_files"]
                sz = pst["total_size"]
                rows_i = _stat_int(nr)
                if rows_i >= 0:
                    sum_rows += rows_i
                    parts_with_rows += 1
                detail.append(
                    f"  {pn}  rows={nr}  files={nf}  size={sz}"
                    if nr != "-1" or nf != "-1" or sz != "-1"
                    else f"  {pn}  (no BASIC_STATS — run ANALYZE TABLE ... PARTITION)"
                )
            lines.extend(detail)
            suffix = ""
            if len(part_names) >= 20:
                suffix = " Partial sum — up to 20 partitions sampled; analyze all partitions separately if needed."
            if parts_with_rows:
                lines.append("")
                lines.append(
                    f"  Sum(rows) over partitions with usable numRows ({parts_with_rows} partition(s)): {sum_rows:,}{suffix}"
                )
            elif part_names:
                lines.append("")
                lines.append(
                    "  No partition-level numRows in HMS sample. Run ANALYZE TABLE ... PARTITION(...) COMPUTE STATISTICS."
                )

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
        for p in part_names:
            lines.append(f"  {p}")
            try:
                pst = client.get_partition_basic_stats(database, table, p)
            except Exception as exc:
                lines.append(f"    (could not load stats: {exc})")
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
