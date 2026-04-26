"""
HiveMind Phase 1 — MCP server entry point.

Registers 7 HMS discovery tools via FastMCP and runs on stdio transport
so Cursor can launch it as a subprocess.

Start:
  PYTHONPATH=. HMS_HOST=<ip> python hivemind/hivemind_server.py
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Ensure the project root is on sys.path when run directly
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Also ensure gen-py Thrift bindings are importable
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
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("hivemind.server")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_HMS_HOST = os.environ.get("HMS_HOST", "10.140.143.128")
_HMS_PORT = int(os.environ.get("HMS_PORT", "9083"))
_TIMEOUT_MS = int(os.environ.get("HMS_THRIFT_TIMEOUT_MS", "10000"))

# ---------------------------------------------------------------------------
# HMS client (module-level singleton; errors at startup are non-fatal)
# ---------------------------------------------------------------------------
_client: HMSClient | None = None

logger.info("HiveMind Phase 1 — MCP server started. Connecting to HMS at %s:%d", _HMS_HOST, _HMS_PORT)

try:
    _client = HMSClient(_HMS_HOST, _HMS_PORT, _TIMEOUT_MS)
    logger.info("HMS connection established.")
except Exception as _conn_exc:
    logger.error("Could not connect to HMS at startup: %s", _conn_exc)
    logger.error("Server will start anyway — individual tool calls will surface this error.")

# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------
mcp = FastMCP(
    name="hivemind",
    instructions=(
        "HiveMind — Hive Metastore discovery tools (Phase 1, read-only). "
        f"Connected to HMS at {_HMS_HOST}:{_HMS_PORT}. "
        "Use list_databases first to explore available databases, then list_tables, "
        "then get_table_schema / get_table_stats / get_partitions / get_table_ddl. "
        "Use search_tables to find relevant tables before generating SQL."
    ),
)


def _require_client() -> HMSClient:
    if _client is None:
        raise RuntimeError(
            f"HMS client is not available — connection to {_HMS_HOST}:{_HMS_PORT} failed at startup."
        )
    return _client


# ---------------------------------------------------------------------------
# Tool: list_databases
# ---------------------------------------------------------------------------
@mcp.tool(
    name="list_databases",
    description=(
        "List all databases available in the Hive Metastore. "
        "Use this first to understand what databases exist before searching for tables."
    ),
)
async def _tool_list_databases() -> str:
    return await handle_list_databases(_require_client())


# ---------------------------------------------------------------------------
# Tool: list_tables
# ---------------------------------------------------------------------------
@mcp.tool(
    name="list_tables",
    description="List all tables in a specific Hive Metastore database.",
)
async def _tool_list_tables(database: str) -> str:
    """
    Args:
        database: Database name to list tables from.
    """
    return await handle_list_tables(_require_client(), database)


# ---------------------------------------------------------------------------
# Tool: search_tables
# ---------------------------------------------------------------------------
@mcp.tool(
    name="search_tables",
    description=(
        "Search for tables in the Hive Metastore whose name or column names contain "
        "a keyword. Use this to discover relevant tables before generating SQL. "
        "If database is not specified, searches all databases."
    ),
)
async def _tool_search_tables(keyword: str, database: str = "") -> str:
    """
    Args:
        keyword: Search keyword (case-insensitive substring match).
        database: Optional database name to limit the search scope.
    """
    db = database.strip() or None
    return await handle_search_tables(_require_client(), keyword, db)


# ---------------------------------------------------------------------------
# Tool: get_table_schema
# ---------------------------------------------------------------------------
@mcp.tool(
    name="get_table_schema",
    description=(
        "Fetch the full schema of a Hive table including columns, types, partition keys, "
        "storage format, and table properties. Always call this before generating SQL."
    ),
)
async def _tool_get_table_schema(database: str, table: str) -> str:
    """
    Args:
        database: Database name.
        table: Table name.
    """
    return await handle_get_table_schema(_require_client(), database, table)


# ---------------------------------------------------------------------------
# Tool: get_table_stats
# ---------------------------------------------------------------------------
@mcp.tool(
    name="get_table_stats",
    description=(
        "Fetch table statistics from HMS: row count, total size in bytes, "
        "and number of files. Returns a warning if statistics have not been computed."
    ),
)
async def _tool_get_table_stats(database: str, table: str) -> str:
    """
    Args:
        database: Database name.
        table: Table name.
    """
    return await handle_get_table_stats(_require_client(), database, table)


# ---------------------------------------------------------------------------
# Tool: get_partitions
# ---------------------------------------------------------------------------
@mcp.tool(
    name="get_partitions",
    description=(
        "Fetch partition key definitions and a sample of the 20 most recent partition "
        "values for a Hive table. Use this to understand partitioning before generating "
        "partition-filtered SQL."
    ),
)
async def _tool_get_partitions(database: str, table: str) -> str:
    """
    Args:
        database: Database name.
        table: Table name.
    """
    return await handle_get_partitions(_require_client(), database, table)


# ---------------------------------------------------------------------------
# Tool: get_table_ddl
# ---------------------------------------------------------------------------
@mcp.tool(
    name="get_table_ddl",
    description=(
        "Get a reconstructed CREATE TABLE statement for a Hive table based on HMS "
        "metadata. Note: this is reconstructed from metadata, not the original DDL."
    ),
)
async def _tool_get_table_ddl(database: str, table: str) -> str:
    """
    Args:
        database: Database name.
        table: Table name.
    """
    return await handle_get_table_ddl(_require_client(), database, table)


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
