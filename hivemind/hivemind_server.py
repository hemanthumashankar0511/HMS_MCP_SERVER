from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_GEN = _ROOT / "gen-py"
if _GEN.is_dir() and str(_GEN) not in sys.path:
    sys.path.insert(0, str(_GEN))

from fastmcp import FastMCP  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

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

logger.info("Connecting to HMS at %s:%d", _HMS_HOST, _HMS_PORT)

try:
    _client = HMSClient(_HMS_HOST, _HMS_PORT, _TIMEOUT_MS)
    logger.info("HMS connection established.")
except Exception as _conn_exc:
    logger.error("Could not connect to HMS: %s", _conn_exc)

mcp = FastMCP(
    name="hivemind",
    instructions=(
        "HiveMind - Hive Metastore discovery tools (read-only). "
        f"Connected to HMS at {_HMS_HOST}:{_HMS_PORT}. "
        "Use list_databases first, then list_tables, then get_table_schema / "
        "get_table_stats / get_partitions / get_table_ddl. "
        "Use search_tables to find relevant tables before generating SQL."
    ),
)


def _require_client() -> HMSClient:
    if _client is None:
        raise RuntimeError(
            f"HMS client unavailable - connection to {_HMS_HOST}:{_HMS_PORT} failed at startup."
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


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
