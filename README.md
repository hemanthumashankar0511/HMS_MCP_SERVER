# HiveMind: Hive Metastore MCP Server

HiveMind is an MCP (Model Context Protocol) server that exposes Apache Hive Metastore (HMS) as a suite of AI-callable discovery tools. It allows AI agents (like Cursor) to securely explore and query HMS metadata.

It also has an **optional** HiveServer2 (HS2) enrichment layer that runs `EXPLAIN` /
`EXPLAIN CBO` to verify partition pruning and obtain CBO row estimates. HS2 is
used for **plan analysis only** — HiveMind never executes a query or returns any
table data through HS2. If HS2 is not configured, HiveMind runs in HMS-only mode.

## HMS vs HS2 — two different services

| Service | Default port | Role in HiveMind |
| --- | --- | --- |
| **HMS** (Hive Metastore) | `9083` | Required. Read-only metadata: databases, tables, schema, partitions, statistics. |
| **HS2** (HiveServer2 / Beeline) | `10000` | Optional. Runs `EXPLAIN` plans for partition-pruning verification and CBO row estimates. No data is read or returned. |

## Prerequisites

- Python **3.11+**
- Access to HMS on port 9083 (no Kerberos required for dev cluster)
- *(Optional)* Access to HS2 on port 10000 for EXPLAIN plan analysis
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
   # Required — Hive Metastore (metadata only)
   HMS_HOST=<YOUR_HMS_HOST>
   HMS_PORT=9083

   # Optional — HiveServer2 EXPLAIN enrichment.
   # If HS2_HOST is unset, HiveMind runs in HMS-only mode (no EXPLAIN plans).
   HS2_HOST=<HS2_HOST>
   HS2_PORT=10000
   HS2_USER=<username>      # the user you log in as (e.g. the cluster login user)
   HS2_PASSWORD=            # leave empty if the dev cluster has no auth
   HS2_DATABASE=sample      # default database for EXPLAIN sessions
   HS2_AUTH=NONE            # NONE | LDAP | KERBEROS (NONE is supported in Phase 1)
   HS2_QUERY_TIMEOUT_S=60
   ```

   > **Never commit `.env`** — it is already in `.gitignore`. Keep credentials out
   > of version control.

   **HS2 EXPLAIN — safety notes**

   - HiveMind only ever sends `EXPLAIN`, `EXPLAIN CBO`, or `EXPLAIN EXTENDED`
     statements to HS2. These produce plan text and CBO estimates **without
     reading or returning table data**.
   - For DML/DDL, `explain_query` runs `EXPLAIN <query>` so the plan is generated
     without performing any write.
   - HS2 is optional and fault-tolerant: if `HS2_HOST` is unset or the connection
     fails at startup, the server logs a warning and continues in HMS-only mode.
   - This cluster is **non-Kerberos**; use `HS2_AUTH=NONE`. LDAP/Kerberos are
     out of scope for Phase 1.

## Running manually

Test the server before registering it with Cursor:

```bash
cd /path/to/HMS_MCP_Server
PYTHONPATH=.:gen-py python hivemind/hivemind_server.py
```

You should see (HMS-only mode):
```text
Connecting to HMS at <YOUR_HOST>:9083
HMS connection established.
HS2_HOST not set — running in HMS-only mode (no EXPLAIN plans).
```

With HS2 configured and reachable:
```text
Connecting to HMS at <YOUR_HOST>:9083
HMS connection established.
Connecting to HS2 at <YOUR_HOST>:10000
HS2 connection established — EXPLAIN enrichment enabled.
```

If HS2 is configured but unreachable, the server keeps running:
```text
HS2 connection failed (...). Continuing in HMS-only mode.
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
   - `text_to_hiveql`
   - `optimize_query` — when HS2 is configured, automatically runs `EXPLAIN CBO` for row estimates and partition-pruning verification
   - `explain_query` — when HS2 is configured, automatically runs `EXPLAIN` for plan analysis (CBO row estimates, partition pruning, row-reduction %)

## Security notes

- The client redacts any table parameter whose key contains `key`, `secret`, `password`, `token`, `credential`, or `access`.
- Partition fetches are hard-capped at 20 rows — `get_all_partitions()` is never called.
- Column-level search is capped at 50 tables per database.
- All tool implementations are module-private; nothing is importable and callable without going through the MCP protocol.
- HS2 access is **EXPLAIN-only**: HiveMind never runs a query that scans or returns table data through HiveServer2. Only `EXPLAIN` / `EXPLAIN CBO` / `EXPLAIN EXTENDED` statements are issued.
- Secrets (`.env`) are git-ignored and never committed.
