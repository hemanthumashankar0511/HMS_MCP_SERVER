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
from hivemind.hs2_client import HS2Client  # noqa: E402
from hivemind.tools.discovery import (  # noqa: E402
    handle_get_partitions,
    handle_get_table_ddl,
    handle_get_table_schema,
    handle_get_table_stats,
    handle_list_databases,
    handle_list_tables,
    handle_search_tables,
)
from hivemind.tools.sql_gen import handle_text_to_hiveql  # noqa: E402
from hivemind.tools.optimize import handle_optimize_query  # noqa: E402
from hivemind.tools.explain import handle_explain_query  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("hivemind.server")

_HMS_HOST = (os.environ.get("HMS_HOST") or "").strip()
_HMS_PORT = int((os.environ.get("HMS_PORT") or "9083").strip())
_TIMEOUT_MS = int(os.environ.get("HMS_THRIFT_TIMEOUT_MS", "10000"))

# HS2 is optional. When HS2_HOST is unset the server runs in HMS-only mode.
_HS2_HOST = (os.environ.get("HS2_HOST") or "").strip()
_HS2_PORT = int((os.environ.get("HS2_PORT") or "10000").strip())
_HS2_USER = (os.environ.get("HS2_USER") or "").strip()
_HS2_PASSWORD = os.environ.get("HS2_PASSWORD") or ""
_HS2_DATABASE = (os.environ.get("HS2_DATABASE") or "default").strip()
_HS2_AUTH = (os.environ.get("HS2_AUTH") or "NONE").strip().upper()
_HS2_QUERY_TIMEOUT_S = int((os.environ.get("HS2_QUERY_TIMEOUT_S") or "60").strip())

if not _HMS_HOST:
    logger.error(
        "HMS_HOST is not set. Add it to %s or export it in the environment (see README).",
        _ROOT / ".env",
    )
    sys.exit(1)

_client: HMSClient | None = None
_hs2_client: HS2Client | None = None

_hs2_instruction = (
    f"HiveServer2 EXPLAIN is enabled (HS2 at {_HS2_HOST}:{_HS2_PORT}). "
    "explain_query and optimize_query run EXPLAIN / EXPLAIN CBO via HS2 to obtain "
    "the optimizer plan, CBO row estimates, and partition-pruning verification. "
    "EXPLAIN only produces plan text — user data is never read or returned. "
    if _HS2_HOST
    else "HiveServer2 is not configured; analysis uses HMS metadata only. "
)

mcp = FastMCP(
    name="hivemind",
    instructions=(
        "HiveMind provides read-only Hive Metastore discovery tools. "
        f"Connected to HMS at {_HMS_HOST}:{_HMS_PORT}. "
        + _hs2_instruction
        + "For query generation, this server generates SELECT-only HiveQL queries. "
        "If the user asks to generate a write operation (DELETE, INSERT, UPDATE, DROP, "
        "TRUNCATE, ALTER, CREATE, MERGE, or any data modification), refuse immediately "
        "without calling tools or explaining how the operation would work. "
        "For query optimization, only SELECT queries are supported. "
        "For query explanation, always use explain_query with HMS metadata. "
        "explain_query may explain SELECT, DDL, and DML statements, including write "
        "operations, but HiveMind never executes them — only EXPLAIN plans are run."
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
        "a keyword. "
        "STRONGLY PREFER passing the 'database' argument to scope the search: an "
        "unscoped search reads the schema of every table in every database and can "
        "take many seconds on a large metastore. First call list_databases to find "
        "the relevant database, then search within it. "
        "If you already know the table name, skip search entirely and call "
        "list_tables then get_table_schema directly — that is far faster than an "
        "unscoped search."
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
        "For partitioned tables, returns table-level BASIC_STATS plus a sampled "
        "partition-level BASIC_STATS section. Returns a warning if statistics have not "
        "been computed."
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
        "HMS metadata (schema, partitions, statistics) is fetched automatically "
        "for every table in the query — no pre-fetched context is required. "
        "When HiveServer2 is configured, EXPLAIN is run automatically on the "
        "submitted query — the parsed CBO row estimates and partition-pruning "
        "verdict are included as evidence for the LLM to cite in Impact and "
        "Summary lines (EXPLAIN only — no data is executed or returned). "
        "Only SELECT queries are supported. Refuse write operations "
        "(DELETE, INSERT, UPDATE, DROP, TRUNCATE, ALTER, CREATE, MERGE) "
        "without calling any tools."
    ),
)
async def _tool_optimize_query(submitted_query: str) -> str:
    return await handle_optimize_query(_require_client(), submitted_query, _hs2_client)


@mcp.tool(
    name="explain_query",
    description=(
        "Explain what a HiveQL query does in plain English, identify where it may "
        "be slow or expensive, and suggest concrete optimizations — all without "
        "executing the query. "
        "Works on any query type including SELECT, DDL, and DML. "
        "Always call get_table_schema, get_partitions, and get_table_stats first "
        "for every table in the query, then pass their combined output as assembled_context. "
        "Reasons from HMS metadata: schema, partition definitions, and statistics. "
        "When HiveServer2 is configured, EXPLAIN CBO is run automatically for plan "
        "analysis — CBO row estimates and partition-pruning verification (EXPLAIN "
        "only — no data is executed or returned); the agent does not need to "
        "pre-fetch any plan."
    ),
)
async def _tool_explain_query(submitted_query: str, assembled_context: str) -> str:
    return await handle_explain_query(submitted_query, assembled_context, _hs2_client)


def main() -> None:
    global _client, _hs2_client
    logger.info("Connecting to HMS at %s:%d", _HMS_HOST, _HMS_PORT)
    try:
        _client = HMSClient(_HMS_HOST, _HMS_PORT, _TIMEOUT_MS)
        logger.info("HMS connection established.")
    except Exception as exc:
        logger.error("Failed to connect to HMS: %s", exc)
        sys.exit(1)

    # HS2 is an optional enrichment layer. A missing host or a failed connection
    # must NOT stop the server — it simply falls back to HMS-only analysis.
    if _HS2_HOST:
        logger.info("Connecting to HS2 at %s:%d", _HS2_HOST, _HS2_PORT)
        try:
            _hs2_client = HS2Client(
                host=_HS2_HOST,
                port=_HS2_PORT,
                user=_HS2_USER,
                password=_HS2_PASSWORD,
                database=_HS2_DATABASE,
                auth=_HS2_AUTH,
                timeout_s=_HS2_QUERY_TIMEOUT_S,
            )
            logger.info("HS2 connection established — EXPLAIN enrichment enabled.")
        except Exception as exc:
            logger.warning(
                "HS2 connection failed (%s). Continuing in HMS-only mode.", exc
            )
            _hs2_client = None
    else:
        logger.info("HS2_HOST not set — running in HMS-only mode (no EXPLAIN plans).")

    try:
        mcp.run()
    finally:
        if _client is not None:
            _client.close()
        if _hs2_client is not None:
            _hs2_client.close()


if __name__ == "__main__":
    main()
