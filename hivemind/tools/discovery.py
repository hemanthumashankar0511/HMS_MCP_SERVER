"""
HiveMind Phase 1 — discovery tool handlers.

Each handler:
- Accepts an HMSClient instance plus tool-specific arguments
- Returns a plain-text string formatted for display in Cursor Agent
- Never raises — all errors are caught and returned as readable messages
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hivemind.hms_client import HMSClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_bytes(raw: str) -> str:
    """Convert a byte-count string to a human-readable size."""
    try:
        n = int(raw)
    except (ValueError, TypeError):
        return raw
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024  # type: ignore[assignment]
    return f"{n:.1f} PB"


def _fmt_count(raw: str) -> str:
    try:
        return f"{int(raw):,}"
    except (ValueError, TypeError):
        return raw


def _short_format(input_format: str) -> str:
    _map = {
        "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat": "ORC",
        "org.apache.hadoop.mapred.TextInputFormat": "TextFile",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat": "Parquet",
        "org.apache.hadoop.mapred.SequenceFileInputFormat": "SequenceFile",
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat": "Avro",
        "org.apache.hadoop.hive.ql.io.RCFileInputFormat": "RCFile",
    }
    return _map.get(input_format, input_format.split(".")[-1] if input_format else "Unknown")


def _short_type(table_type: str) -> str:
    return {
        "MANAGED_TABLE": "Managed",
        "EXTERNAL_TABLE": "External",
        "VIRTUAL_VIEW": "View",
        "MATERIALIZED_VIEW": "Materialized View",
    }.get(table_type, table_type)


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

async def handle_list_databases(client: "HMSClient") -> str:
    """Lists all databases in HMS."""
    try:
        dbs = client.get_all_databases()
    except Exception as exc:
        logger.exception("list_databases failed")
        return f"Error listing databases: {exc}"

    if not dbs:
        return "No databases found in HMS."

    lines = [f"Databases in Hive Metastore  ({len(dbs)} total)", "=" * 40]
    for db in dbs:
        lines.append(f"  {db}")
    lines.append("")
    lines.append("Tip: use list_tables with a database name to see its tables.")
    return "\n".join(lines)


async def handle_list_tables(client: "HMSClient", database: str) -> str:
    """Lists all tables in a database."""
    try:
        tables = client.get_all_tables(database)
    except Exception as exc:
        logger.exception("list_tables failed for %s", database)
        return f"Error listing tables in '{database}': {exc}"

    if not tables:
        return f"No tables found in database '{database}'."

    lines = [f"Tables in '{database}'  ({len(tables)} total)", "=" * 40]
    for t in tables:
        lines.append(f"  {t}")
    lines.append("")
    lines.append(f"Tip: use get_table_schema with database='{database}' and table=<name> to see columns.")
    return "\n".join(lines)


async def handle_search_tables(
    client: "HMSClient", keyword: str, database: str | None = None
) -> str:
    """Search for tables by keyword across table names and column names."""
    try:
        results = client.search_tables(keyword, database)
    except Exception as exc:
        logger.exception("search_tables failed for keyword=%s", keyword)
        return f"Error searching for '{keyword}': {exc}"

    scope = f"database '{database}'" if database else "all databases"

    if not results:
        searched = f"Searched {scope}."
        return (
            f"No tables found matching '{keyword}'.\n{searched}\n"
            "Try a shorter keyword or check the spelling."
        )

    lines = [
        f"Search results for '{keyword}' in {scope}  ({len(results)} match(es))",
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
    """Returns full schema: columns, partition keys, storage format, table type."""
    try:
        info = client.get_table(database, table)
    except Exception as exc:
        logger.exception("get_table_schema failed for %s.%s", database, table)
        return f"Error fetching schema for '{database}.{table}': {exc}"

    fmt = _short_format(info["input_format"])
    tbl_type = _short_type(info["table_type"])
    num_rows = info.get("num_rows", "-1")
    stats_warn = (
        "\n  [!] Statistics missing — run: ANALYZE TABLE "
        f"{database}.{table} COMPUTE STATISTICS\n"
        if num_rows in ("-1", "", None) or int(num_rows) < 0
        else ""
    )

    lines = [
        f"Schema: {database}.{table}",
        "=" * 50,
        f"  Type    : {tbl_type}",
        f"  Format  : {fmt}",
        f"  Location: {info['location']}",
        stats_warn,
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
        lines.append("Table Properties:")
        for k, v in sorted(info["parameters"].items()):
            lines.append(f"  {k} = {v}")

    return "\n".join(lines)


async def handle_get_table_stats(
    client: "HMSClient", database: str, table: str
) -> str:
    """Returns table statistics with a warning when they haven't been computed."""
    try:
        stats = client.get_table_stats(database, table)
    except Exception as exc:
        logger.exception("get_table_stats failed for %s.%s", database, table)
        return f"Error fetching stats for '{database}.{table}': {exc}"

    lines = [f"Statistics: {database}.{table}", "=" * 50]

    if not stats["stats_available"]:
        lines.append("")
        lines.append(
            "  [!] Statistics missing — run ANALYZE TABLE "
            f"{database}.{table} COMPUTE STATISTICS for accurate analysis."
        )
        lines.append("")

    lines += [
        f"  Rows       : {_fmt_count(stats['num_rows'])}",
        f"  Total size : {_fmt_bytes(stats['total_size'])}",
        f"  Files      : {_fmt_count(stats['num_files'])}",
    ]
    if stats["last_modified"]:
        lines.append(f"  Last DDL   : {stats['last_modified']}")

    return "\n".join(lines)


async def handle_get_partitions(
    client: "HMSClient", database: str, table: str
) -> str:
    """Returns partition key structure and up to 20 sample partition values."""
    try:
        info = client.get_table(database, table)
    except Exception as exc:
        logger.exception("get_partitions failed (table fetch) for %s.%s", database, table)
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
        part_names = client.get_partition_names(database, table, max_parts=20)
    except Exception as exc:
        lines.append(f"\nCould not fetch partition values: {exc}")
        return "\n".join(lines)

    lines.append("")
    if part_names:
        lines.append(f"Showing {len(part_names)} most recent partitions (sample — not the full list):")
        for p in part_names:
            lines.append(f"  {p}")
    else:
        lines.append("No partition data found (table may be empty or not yet populated).")

    return "\n".join(lines)


async def handle_get_table_ddl(
    client: "HMSClient", database: str, table: str
) -> str:
    """Returns a reconstructed CREATE TABLE statement from HMS metadata."""
    try:
        ddl = client.get_table_ddl(database, table)
    except Exception as exc:
        logger.exception("get_table_ddl failed for %s.%s", database, table)
        return f"Error reconstructing DDL for '{database}.{table}': {exc}"

    return ddl
