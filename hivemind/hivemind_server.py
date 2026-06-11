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
from hivemind.tools.compare import handle_compare_queries  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("hivemind.server")

def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


# ── Cluster preset ─────────────────────────────────────────────────────────────
# CLUSTER_PRESET=cloudera_kerberos turns on all Cloudera CDH/CDP secure defaults
# (Auto-TLS, Kerberos/GSSAPI, HTTP transport on port 10001) so the only values
# a user needs to supply individually are HMS_HOST, HS2_HOST, and
# HIVE_KERBEROS_PRINCIPAL. Every preset default can be overridden by an explicit
# env var — the preset only changes what the _absent_ variable resolves to.
#
#   plain (default) — no TLS, no Kerberos, Thrift binary, HS2 port 10000
#   cloudera_kerberos — Auto-TLS, AD Kerberos, HTTP transport, HS2 port 10001
_CLUSTER_PRESET = (os.environ.get("CLUSTER_PRESET") or "plain").strip().lower()
_IS_CLOUDERA = _CLUSTER_PRESET in ("cloudera_kerberos", "cloudera")

# ── HMS ────────────────────────────────────────────────────────────────────────
_HMS_HOST = (os.environ.get("HMS_HOST") or "").strip()
_HMS_PORT = int((os.environ.get("HMS_PORT") or "9083").strip())
_TIMEOUT_MS = int(os.environ.get("HMS_THRIFT_TIMEOUT_MS", "10000"))

# ── Security ──────────────────────────────────────────────────────────────────
# TLS is split into two independent flags because on Cloudera CDH/CDP:
#   • HMS (port 9083) uses plain Thrift + SASL/Kerberos — NO TLS on the wire.
#     TLS on the metastore Thrift port causes an immediate SSL EOF.
#   • HS2 (port 10001) uses HTTPS — TLS IS required.
#
# HMS_USE_TLS  — controls the HMSClient Thrift socket. Defaults to false even
#                for the cloudera_kerberos preset.
# HS2_USE_TLS  — controls the HS2Client HTTPS connection. Defaults to true for
#                the cloudera_kerberos preset.
# HIVE_USE_TLS — legacy single flag; when set it overrides BOTH of the above so
#                existing configs that explicitly set it keep working.
_HIVE_USE_TLS_OVERRIDE = os.environ.get("HIVE_USE_TLS")
if _HIVE_USE_TLS_OVERRIDE is not None:
    _HMS_USE_TLS = _env_bool("HIVE_USE_TLS")
    _HS2_USE_TLS = _env_bool("HIVE_USE_TLS")
else:
    _HMS_USE_TLS = _env_bool("HMS_USE_TLS", default=False)
    _HS2_USE_TLS = _env_bool("HS2_USE_TLS", default=_IS_CLOUDERA)

# Kerberos principal. Setting this automatically enables Kerberos authentication
# so HIVE_USE_KERBEROS=true is not required as a separate line.
_HIVE_KERBEROS_PRINCIPAL = (os.environ.get("HIVE_KERBEROS_PRINCIPAL") or "").strip()

# Auto-enable Kerberos when:
#   • HIVE_USE_KERBEROS=true is explicitly set, OR
#   • HIVE_KERBEROS_PRINCIPAL is non-empty (no need to set both), OR
#   • preset is cloudera_kerberos
_HIVE_USE_KERBEROS = _env_bool(
    "HIVE_USE_KERBEROS",
    default=bool(_HIVE_KERBEROS_PRINCIPAL) or _IS_CLOUDERA,
)

_HIVE_HS2_SERVICE_NAME = (os.environ.get("HIVE_HS2_SERVICE_NAME") or "hive").strip()
_KRB5_CCACHE = (os.environ.get("KRB5_CCACHE") or "").strip()

# Surface an optional ccache override to the GSSAPI layer via the standard env
# var it reads (KRB5CCNAME), so a non-default kinit cache is picked up cleanly.
if _KRB5_CCACHE:
    os.environ["KRB5CCNAME"] = _KRB5_CCACHE

# ── HS2 (optional EXPLAIN enrichment) ─────────────────────────────────────────
# When HS2_HOST is unset the server runs in HMS-only mode — all discovery tools
# still work; EXPLAIN-based features (optimize_query, compare_queries) are
# skipped with a clear message.
_HS2_HOST = (os.environ.get("HS2_HOST") or "").strip()
# Default port: 10001 for Cloudera HTTP transport, 10000 for plain binary Thrift.
_HS2_PORT = int((os.environ.get("HS2_PORT") or ("10001" if _IS_CLOUDERA else "10000")).strip())
_HS2_USER = (os.environ.get("HS2_USER") or "").strip()
_HS2_PASSWORD = os.environ.get("HS2_PASSWORD") or ""
_HS2_DATABASE = (os.environ.get("HS2_DATABASE") or "default").strip()
_HS2_AUTH = (os.environ.get("HS2_AUTH") or "NONE").strip().upper()
_HS2_QUERY_TIMEOUT_S = int((os.environ.get("HS2_QUERY_TIMEOUT_S") or "60").strip())
# Transport mode: Cloudera CDH/CDP uses HTTP on port 10001; standard clusters use
# Thrift binary on port 10000.  Preset cloudera_kerberos defaults to 'http'.
_HS2_TRANSPORT_MODE = (
    os.environ.get("HS2_TRANSPORT_MODE") or ("http" if _IS_CLOUDERA else "binary")
).strip().lower()
_HS2_USE_HTTP = _HS2_TRANSPORT_MODE == "http"

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
    "explain_query and optimize_query run EXPLAIN via HS2 to obtain "
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
        "operations, but HiveMind never executes them — only EXPLAIN plans are run. "
        "When the user asks to compare two queries or wants to see before/after plan "
        "metrics, use compare_queries — it runs EXPLAIN on both queries and returns "
        "a side-by-side plan comparison with row reduction and partition pruning verdict. "
        # ── Execution & terminal guardrail ───────────────────────────────────
        # HiveMind tools return text (generated SQL, analysis reports). The agent
        # MUST return that text to the user as produced. It MUST NOT use the
        # terminal/shell or any external program for ANY part of answering — not to
        # execute a query, not to fetch rows, and not for auxiliary computation such
        # as date-to-surrogate-key conversion or partition-value derivation. All
        # required values come from the metastore (get_partitions, date_dim, etc.).
        "CRITICAL: HiveMind is a read-only metadata and query-generation tool. After "
        "any tool returns, return its output directly to the user. Do NOT use the "
        "terminal, shell, Python/python3, pyhive, or a Hive CLI for ANY part of the "
        "task — not to execute queries, not to fetch results, and not for auxiliary "
        "math like computing a date surrogate key. Never compute or guess a partition "
        "key value: derive it from the metastore (a get_partitions sample value, or a "
        "JOIN to date_dim filtered on its human-readable date column). Fetching live "
        "results or running shell commands is outside HiveMind's scope and will fail "
        "in sandboxed environments."
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
        "partition-level BASIC_STATS section. Returns the ANALYZE TABLE command to run "
        "if statistics have not been computed."
    ),
)
async def _tool_get_table_stats(database: str, table: str) -> str:
    return await handle_get_table_stats(_require_client(), database, table)


@mcp.tool(
    name="get_partitions",
    description=(
        "Fetch partition key definitions for a Hive table. Returns partition key ranges "
        "(min/max per key), a representative sample (~30 partitions, head + tail), and "
        "derived table-level totals aggregated over all partitions (up to HMS i16 max)."
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
        "requests directly without fetching any schema. "
        "IMPORTANT: This tool returns generated HiveQL text. Return it directly to the user "
        "as-is. Do NOT execute it and do NOT use the terminal for any part of the task — "
        "no shell commands, no Python/python3 scripts, no pyhive, no Hive CLI — neither to "
        "fetch rows nor for auxiliary math such as converting a calendar date into a "
        "surrogate _date_sk partition key. Never compute a partition value: take it from a "
        "get_partitions sample, or JOIN to date_dim and filter on its human-readable date "
        "column. Query generation and query execution are separate concerns; this tool only "
        "handles generation."
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
        "without calling any tools. "
        "IMPORTANT: This tool returns an analysis report and an optimized query rewrite. "
        "Return the report directly to the user. Do NOT execute the original or "
        "optimized query through any means — no shell, no Python, no pyhive."
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
        "When HiveServer2 is configured, EXPLAIN is run automatically for plan "
        "analysis — CBO row estimates and partition-pruning verification (EXPLAIN "
        "only — no data is executed or returned); the agent does not need to "
        "pre-fetch any plan."
    ),
)
async def _tool_explain_query(submitted_query: str, assembled_context: str) -> str:
    return await handle_explain_query(submitted_query, assembled_context, _hs2_client)


@mcp.tool(
    name="compare_queries",
    description=(
        "Compare two HiveQL SELECT queries side-by-side using their EXPLAIN plans. "
        "Runs EXPLAIN on both queries via HiveServer2, then produces a structured "
        "report showing rows scanned before and after, partition pruning verdict, join "
        "strategy changes, and a plain-English verdict on which query is more efficient. "
        "HMS metadata (schema, partitions, statistics) is fetched automatically — "
        "no pre-fetched context is required. "
        "Use this when the user asks to compare two queries, wants to verify that an "
        "optimized rewrite actually reduces I/O, or asks for a before/after plan analysis. "
        "Requires HiveServer2 to be configured; degrades gracefully with a clear message "
        "when HS2 is unavailable. "
        "Only SELECT queries are supported."
    ),
)
async def _tool_compare_queries(original_query: str, optimized_query: str) -> str:
    return await handle_compare_queries(_require_client(), original_query, optimized_query, _hs2_client)


def main() -> None:
    global _client, _hs2_client
    logger.info(
        "Cluster preset: %s | HMS-TLS=%s | HS2-TLS=%s | Kerberos=%s%s",
        _CLUSTER_PRESET,
        _HMS_USE_TLS,
        _HS2_USE_TLS,
        _HIVE_USE_KERBEROS,
        f" (principal={_HIVE_KERBEROS_PRINCIPAL})" if _HIVE_KERBEROS_PRINCIPAL else "",
    )
    if _HIVE_USE_KERBEROS:
        logger.info(
            "Kerberos enabled — a valid TGT must be present. "
            "Ensure you have run 'kinit' with a valid keytab/principal before starting the server."
        )
    logger.info("Connecting to HMS at %s:%d", _HMS_HOST, _HMS_PORT)
    try:
        _client = HMSClient(
            _HMS_HOST,
            _HMS_PORT,
            _TIMEOUT_MS,
            use_tls=_HMS_USE_TLS,
            use_kerberos=_HIVE_USE_KERBEROS,
            kerberos_principal=_HIVE_KERBEROS_PRINCIPAL,
            kerberos_service_name=_HIVE_HS2_SERVICE_NAME,
        )
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
                use_tls=_HS2_USE_TLS,
                use_kerberos=_HIVE_USE_KERBEROS,
                kerberos_principal=_HIVE_KERBEROS_PRINCIPAL,
                kerberos_service_name=_HIVE_HS2_SERVICE_NAME,
                use_http_transport=_HS2_USE_HTTP,
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
