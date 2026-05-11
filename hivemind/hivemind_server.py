from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from fastmcp import FastMCP
from dotenv import load_dotenv

_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env")

from hivemind.hms_client import HMSClient  # noqa: E402
from hivemind.tools.discovery import (  # noqa: E402
    handle_get_partitions,
    handle_get_table_ddl,
    handle_get_table_schema,
    handle_get_table_stats,
    handle_list_databases,
    handle_list_tables,
    handle_search_tables,
)
from hivemind.tools.sql_gen import handle_text_to_hiveql, handle_optimize_query  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("hivemind.server")

_HMS_HOST = (os.environ.get("HMS_HOST") or "").strip()
_HMS_PORT = int((os.environ.get("HMS_PORT") or "9083").strip())
_TIMEOUT_MS = int(os.environ.get("HMS_THRIFT_TIMEOUT_MS", "10000"))

if not _HMS_HOST:
    logger.error(
        "HMS_HOST is not set. Add it to %s or export it in the environment (see README).",
        _ROOT / ".env",
    )
    sys.exit(1)

_client: HMSClient | None = None

mcp = FastMCP(
    name="hivemind",
    instructions=(
        "HiveMind provides read-only Hive Metastore discovery tools. "
        f"Connected to HMS at {_HMS_HOST}:{_HMS_PORT}. "
        "This server generates SELECT-only HiveQL queries. "
        "If the user asks for a write operation (DELETE, INSERT, UPDATE, DROP, TRUNCATE, "
        "ALTER, CREATE, MERGE, or any data modification), refuse immediately without "
        "calling any tools or explaining how the operation would work. "
        "For all other requests, fetch schema metadata before generating SQL queries."
    ),
)


def _require_client() -> HMSClient:
    if _client is None:
        raise RuntimeError(
            f"HMS client unavailable — connection to {_HMS_HOST}:{_HMS_PORT} failed at startup."
        )
    return _client


@mcp.tool(
    name="list_databases",
    description=(
        "List all databases available in the Hive Metastore. "
        "Use this first to understand what databases exist before searching for tables."
    ),
)
async def _tool_list_databases() -> str:
    return await handle_list_databases(_require_client())


@mcp.tool(
    name="list_tables",
    description="List all tables in a specific Hive Metastore database.",
)
async def _tool_list_tables(database: str) -> str:
    return await handle_list_tables(_require_client(), database)


@mcp.tool(
    name="search_tables",
    description=(
        "Search for tables in the Hive Metastore whose name or column names contain "
        "a keyword. If database is not specified, searches all databases."
    ),
)
async def _tool_search_tables(keyword: str, database: str = "") -> str:
    db = database.strip() or None
    return await handle_search_tables(_require_client(), keyword, db)


@mcp.tool(
    name="get_table_schema",
    description=(
        "Fetch the full schema of a Hive table including columns, types, partition keys, "
        "storage format, and table properties."
    ),
)
async def _tool_get_table_schema(database: str, table: str) -> str:
    return await handle_get_table_schema(_require_client(), database, table)


@mcp.tool(
    name="get_table_stats",
    description=(
        "Fetch table statistics from HMS: row count, total size, and number of files. "
        "Returns a warning if statistics have not been computed."
    ),
)
async def _tool_get_table_stats(database: str, table: str) -> str:
    return await handle_get_table_stats(_require_client(), database, table)


@mcp.tool(
    name="get_partitions",
    description=(
        "Fetch partition key definitions and a sample of the 20 most recent partition "
        "values for a Hive table."
    ),
)
async def _tool_get_partitions(database: str, table: str) -> str:
    return await handle_get_partitions(_require_client(), database, table)


@mcp.tool(
    name="get_table_ddl",
    description=(
        "Get a reconstructed CREATE TABLE statement for a Hive table based on HMS metadata."
    ),
)
async def _tool_get_table_ddl(database: str, table: str) -> str:
    return await handle_get_table_ddl(_require_client(), database, table)


@mcp.tool(
    name="text_to_hiveql",
    description=(
        "Final step in SELECT query generation. Takes a natural language question and the "
        "schema context from get_table_schema / get_partitions and produces a HiveQL SELECT "
        "query. Always call get_table_schema (and get_partitions if partitioned) first, then "
        "pass those outputs as assembled_context. Do NOT call this tool for write operations "
        "(DELETE, INSERT, UPDATE, DROP, TRUNCATE, ALTER, CREATE, MERGE) — refuse those "
        "requests directly without fetching any schema."
    ),
)
async def _tool_text_to_hiveql(natural_query: str, assembled_context: str) -> str:
    return await handle_text_to_hiveql(natural_query, assembled_context)


@mcp.tool(
    name="optimize_query",
    description=(
        "Analyze a HiveQL SELECT query for performance anti-patterns and return "
        "a structured report with severity-ranked issues and a corrected rewrite. "
        "Always call get_table_schema, get_partitions, and get_table_stats first "
        "for every table in the query, then pass their combined output as "
        "assembled_context. "
        "Only SELECT queries are supported. Refuse write operations "
        "(DELETE, INSERT, UPDATE, DROP, TRUNCATE, ALTER, CREATE, MERGE) "
        "without calling any tools."
    ),
)
async def _tool_optimize_query(submitted_query: str, assembled_context: str) -> str:
    return await handle_optimize_query(submitted_query, assembled_context)


def main() -> None:
    global _client
    logger.info("Connecting to HMS at %s:%d", _HMS_HOST, _HMS_PORT)
    try:
        _client = HMSClient(_HMS_HOST, _HMS_PORT, _TIMEOUT_MS)
        logger.info("HMS connection established.")
    except Exception as exc:
        logger.error("Failed to connect to HMS: %s", exc)
        sys.exit(1)
    mcp.run()


if __name__ == "__main__":
    main()
