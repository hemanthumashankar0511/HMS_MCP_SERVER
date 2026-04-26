# HiveMind: Hive Metastore MCP Server

HiveMind is an MCP (Model Context Protocol) server that exposes Apache Hive Metastore (HMS) as a suite of AI-callable discovery tools. It allows AI agents (like Cursor) to securely explore and query HMS metadata.

## Prerequisites

- Python **3.14.4** (only version supported for this release)
- Access to HMS on port 9083 (no Kerberos required for dev cluster)
- Cursor with Agent mode enabled
- Thrift bindings already generated under `gen-py/`

## Setup

1. **Create a virtual environment and install dependencies**

   ```bash
   cd /path/to/HMS_MCP_Server
   python3 -m venv .venv && source .venv/bin/activate
   pip install -e .
   pip install -r hivemind/requirements.txt
   ```

2. **Thrift Python client** 

   If you haven't generated the Thrift bindings yet, you can do so from a Hive source tree (that includes `fb303` in the Thrift include path):

   ```bash
   cd standalone-metastore/metastore-common/src/main/thrift
   thrift --gen py -I <path-to-fb303-if-needed> hive_metastore.thrift
   ```

   Copy the generated `hive_metastore` package into this repo:

   ```text
   HMS_MCP_Server/gen-py/hive_metastore/...
   ```

3. **Configure Environment**

   Create a `.env` file in the project root with your cluster details:

   ```env
   HMS_HOST=10.140.143.128
   HMS_PORT=9083
   ```

## Running manually

Test the server before registering it with Cursor:

```bash
cd /path/to/HMS_MCP_Server
PYTHONPATH=.:gen-py python hivemind/hivemind_server.py
```

You should see:
```text
HiveMind Phase 1 — MCP server started. Connecting to HMS at <YOUR_HOST>:9083
HMS connection established.
```

## Registering with Cursor

1. Ensure `.cursor/mcp.json` has the correct absolute paths for your machine.
2. Open Cursor.
3. Open Command Palette → **"Cursor: Restart MCP Servers"**
4. Open the Agent mode chat panel and verify the tools appear in the tool list:
   - `list_databases`
   - `list_tables`
   - `search_tables`
   - `get_table_schema`
   - `get_table_stats`
   - `get_partitions`
   - `get_table_ddl`

## Running the tests

Unit tests use mocks and run without a live HMS connection:

```bash
pytest tests/ -v
```

Integration tests require `HMS_HOST` to be set and reachable:

```bash
HMS_HOST=10.140.143.128 pytest tests/test_integration.py -v
```

## Security notes

- The client redacts any table parameter whose key contains `key`, `secret`, `password`, `token`, `credential`, or `access`.
- Partition fetches are hard-capped at 20 rows — `get_all_partitions()` is never called.
- Column-level search is capped at 50 tables per database.
- All tool implementations are module-private; nothing is importable and callable without going through the MCP protocol.
