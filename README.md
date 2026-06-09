# HiveMind — Hive Metastore MCP Server

HiveMind is an [MCP (Model Context Protocol)](https://modelcontextprotocol.io) server
that turns an Apache Hive Metastore into a suite of **read-only, AI-callable tools**.
It lets agents (Cursor, Claude Desktop, or any MCP client) explore Hive metadata,
generate SELECT queries, and analyze query performance — all without ever reading or
mutating table data.

It also ships an **optional** HiveServer2 (HS2) enrichment layer that runs plain
`EXPLAIN <query>` statements for plan analysis. On Hive 3 those plans often carry CBO
row estimates, which HiveMind parses to verify partition pruning. If HS2 is not
configured, HiveMind runs fully in HMS-only mode.

> **Read-only by design.** HiveMind never executes a query, never returns table rows,
> and refuses to generate write operations. HS2 is used for `EXPLAIN` only.

---

## Table of contents

- [Architecture](#architecture)
- [HMS vs HS2](#hms-vs-hs2--two-different-services)
- [Available tools](#available-tools)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the server](#running-the-server)
- [Registering with an MCP client](#registering-with-an-mcp-client)
- [Usage examples](#usage-examples)
- [Statistics & partition limits](#statistics--partition-limits)
- [Security model](#security-model)
- [Development & testing](#development--testing)
- [Troubleshooting](#troubleshooting)
- [Project layout](#project-layout)

---

## Architecture

```
┌──────────────┐     MCP (stdio)      ┌──────────────────────────┐
│  MCP client  │ ───────────────────► │      HiveMind server     │
│  (Cursor…)   │ ◄─────────────────── │     (fastmcp tools)      │
└──────────────┘                      └────────────┬─────────────┘
                                                    │
                         ┌──────────────────────────┼───────────────────────┐
                         │                                                   │
                         ▼ Thrift (required)                       ▼ PyHive (optional)
                ┌──────────────────┐                       ┌──────────────────────┐
                │  Hive Metastore  │                       │     HiveServer2      │
                │   (port 9083)    │                       │     (port 10000)     │
                │  metadata only   │                       │   EXPLAIN plans only │
                └──────────────────┘                       └──────────────────────┘
```

- **HMS layer** (`hivemind/hms_client.py`) — thin, thread-safe Thrift wrapper, auto-reconnects once on transport failure.
- **HS2 layer** (`hivemind/hs2_client.py`) — optional PyHive wrapper that issues `EXPLAIN` only.
- **Tools** (`hivemind/tools/`) — discovery, SQL generation, optimization, explanation, and comparison handlers.
- **Server** (`hivemind/hivemind_server.py`) — registers every tool with `fastmcp` and wires up the clients.

---

## HMS vs HS2 — two different services

| Service | Default port | Required? | Role in HiveMind |
| --- | --- | --- | --- |
| **HMS** (Hive Metastore) | `9083` | **Yes** | Read-only metadata: databases, tables, schema, partitions, statistics, DDL. |
| **HS2** (HiveServer2 / Beeline) | `10000` | No | Runs `EXPLAIN` plans for partition-pruning verification and CBO row estimates. No data is read or returned. |

If `HS2_HOST` is unset (or the connection fails at startup), the server logs a warning
and continues in HMS-only mode — all discovery tools still work.

---

## Available tools

### Discovery (HMS only)

| Tool | Description |
| --- | --- |
| `list_databases` | List all databases in the metastore. |
| `list_tables` | List all tables in a given database. |
| `search_tables` | Find tables by name or column name. Scope with a `database` argument for speed. |
| `get_table_schema` | Columns, types, partition keys, storage format, and table properties. |
| `get_table_stats` | Row count / size / file count. For partitioned tables, derives table totals by aggregating partition BASIC_STATS. |
| `get_partitions` | Partition key definitions plus per-partition BASIC_STATS (rows, size, files). |
| `get_table_ddl` | Reconstructed `CREATE TABLE` statement from metastore metadata. |

### Query tools

| Tool | Query types | HS2 usage | Notes |
| --- | --- | --- | --- |
| `text_to_hiveql` | SELECT only | — | Turns a natural-language question + schema context into a safe HiveQL SELECT. |
| `optimize_query` | SELECT only | runs `EXPLAIN` on the **submitted** query | Auto-fetches HMS metadata for every `database.table`; returns a severity-ranked anti-pattern report + optimized rewrite. Falls back to HMS-only if HS2 is down. |
| `explain_query` | SELECT, DDL, DML | appends raw `EXPLAIN` text as reference | Plain-English explanation; requires pre-fetched HMS context from the discovery tools. |
| `compare_queries` | SELECT only | runs `EXPLAIN` on **both** queries | Side-by-side before/after plan comparison: row estimates, partition pruning, join strategy. Requires HS2. |

> **Write operations** (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `TRUNCATE`, `ALTER`,
> `CREATE`, `MERGE`, `OVERWRITE`) are refused by `text_to_hiveql`, `optimize_query`,
> and `compare_queries`. `explain_query` will *explain* a write statement (via
> `EXPLAIN`) but never executes it.

---

## Prerequisites

- **Python 3.14**
- Network access to a **Hive Metastore** on its Thrift port (default `9083`)
- *(Optional)* Network access to **HiveServer2** (default `10000`) for `EXPLAIN` enrichment
- Generated Thrift bindings under `gen-py/` (see [Installation](#installation))
- An MCP client (e.g. Cursor with Agent mode)

---

## Installation

```bash
# 1. Clone, then create an isolated environment
git clone https://github.com/hemanthumashankar0511/HMS_MCP_SERVER.git
python3 -m venv .venv && source .venv/bin/activate

# 2. Install the package and its dependencies
pip install -e .

# (optional) install dev/test extras
pip install -e ".[dev]"
```

### Thrift Python bindings

HiveMind talks to HMS using generated Thrift stubs that live in `gen-py/`. If they are
not already present, generate them from a Hive source tree (the include path must
contain `fb303`):

```bash
cd path_to_hive/standalone-metastore/src/main/thrift
mkdir -p share/fb303/if/
curl -sL https://raw.githubusercontent.com/apache/thrift/master/contrib/fb303/if/fb303.thrift -o share/fb303/if/fb303.thrift
thrift -r -gen py -I . hive_metastore.thrift
```

Copy the generated package into this repo:

```text
cp -r /path/to/generated/gen-py/{hive_metastore,fb303} /path/to/HMS_MCP_Server/gen-py/
```

The server adds `gen-py/` to `sys.path` automatically at startup.

### NOTE: This has already been done for you

---

## Configuration

A ready-to-use template lives in `.env.example` — copy it and fill in the marked lines:

```bash
cp .env.example .env
```

Then edit `.env`. Pick the preset that matches your cluster.

---

### Preset A — Plain / dev cluster (no TLS, no Kerberos)

Minimum change: **1 line** (`HMS_HOST`). Optionally add `HS2_HOST` for EXPLAIN plans.

```env
CLUSTER_PRESET=plain

HMS_HOST=10.x.x.x          # ← your Hive Metastore IP or hostname

HS2_HOST=10.x.x.x          # ← optional; leave blank for metadata-only mode
```

---

### Preset B — CDP cluster (Auto-TLS + AD Kerberos)

Minimum change: **3 lines** (host × 2 + principal). TLS, Kerberos, port 10001, and HTTP transport are switched on automatically by the preset.

```env
CLUSTER_PRESET=cloudera_kerberos

HMS_HOST=ccycloud-1.example.site          # ← your cluster node FQDN

HS2_HOST=ccycloud-1.example.site          # ← same node (optional, leave blank to disable)

# Copy from the 'principal=...' field in your Beeline/JDBC URL.
# Setting this automatically enables Kerberos — no other auth flag needed.
HIVE_KERBEROS_PRINCIPAL=hive/ccycloud-1.example.site@YOUR-AD-REALM.COM  # ← from JDBC URL
```

Before starting the server, obtain a Kerberos ticket:

```bash
kinit hive -kt /path/to/hive.keytab
klist   # confirm ticket is valid
pip install kerberos   # one-time: needed for HTTP SPNEGO auth
```

---

### Connecting to a new Kerberized cluster — one-time setup

> Follow these steps once per cluster. The result is a dedicated `krb5` config and
> keytab on your Mac that keeps credentials isolated per cluster. After completing
> them, update `.env` following [Preset B](#preset-b--kerberized-cluster-auto-tls--kerberos).

#### Step 1 — Collect details from the cluster

SSH into the new cluster from your Mac:

```bash
# Run on Mac
ssh root@<NEW-CLUSTER-HOSTNAME>
```

Now inside the cluster, run each of these and copy the output to a notes file on your Mac:

```bash
# Run INSIDE the cluster

# 1. Get the exact FQDN of the HMS node
hostname -f

# 2. Get the full krb5.conf (you will need realm, KDC, and domain_realm sections)
cat /etc/krb5.conf

# 3. Confirm the keytab path and that it has a hive principal
ls -la /path/to/hive.keytab
klist -kt /path/to/hive.keytab

# 4. Get the Kerberos principal from the running HiveServer2 config
grep -i "kerberos.principal" /etc/hive/conf/hive-site.xml | head -5

# 5. Confirm HMS is listening on 9083
netstat -tlnp 2>/dev/null | grep 9083 || ss -tlnp | grep 9083

# 6. Done on cluster
exit
```

From the `klist -kt` output you will see something like:

```
hive/node-2.newcluster.example.com@NEW-REALM.EXAMPLE.COM
```

That full string is your `HIVE_KERBEROS_PRINCIPAL`. Note it exactly.

#### Step 2 — Copy files from cluster to Mac

Back on your Mac, copy both the config and keytab:

```bash
# Run on Mac

# Copy the krb5.conf from the new cluster into its own named file
scp root@<NEW-CLUSTER-HOSTNAME>:/etc/krb5.conf ~/krb5_newcluster.conf

# Copy the hive keytab from the new cluster into its own named file
scp root@<NEW-CLUSTER-HOSTNAME>:/path/to/hive.keytab ~/hive-newcluster.keytab

# Lock down the keytab permissions
chmod 600 ~/hive-newcluster.keytab

# Verify both files landed
ls -la ~/krb5_newcluster.conf ~/hive-newcluster.keytab
cat ~/krb5_newcluster.conf | grep -A3 "\[realms\]"
```

#### Step 3 — Install Kerberos tools on Mac (if not already done)

```bash
# Run on Mac (one time, skip if already done)
brew install krb5

# Add the Homebrew krb5 binaries to your PATH permanently
echo 'export PATH="/opt/homebrew/opt/krb5/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Confirm kinit is from Homebrew, not macOS built-in
which kinit
# Should show: /opt/homebrew/opt/krb5/bin/kinit
```

Also install the Python Kerberos package in your venv if not done yet:

```bash
# Run on Mac
cd /path/to/HMS_MCP_Server
source .venv/bin/activate
pip install kerberos
```

#### Step 4 — Test `kinit` with the new cluster config

```bash
# Run on Mac

# Get a ticket using the new cluster's config and keytab
KRB5_CONFIG=~/krb5_newcluster.conf kinit -kt ~/hive-newcluster.keytab hive

# Verify the ticket (must show a valid expiry, no errors)
KRB5_CONFIG=~/krb5_newcluster.conf klist
```

Expected output:

```
Credentials cache: API:...
        Principal: hive@NEW-REALM.EXAMPLE.COM
  Issued                Expires               Principal
Jun  8 14:00:00 2026   Jun  9 14:00:00 2026  krbtgt/NEW-REALM.EXAMPLE.COM@NEW-REALM.EXAMPLE.COM
```

> Do not proceed if `kinit` fails or `klist` shows warnings.

#### Step 5 — `kinit` before each session (every time you switch to this cluster)

```bash
# Run on Mac
KRB5_CONFIG=~/krb5_newcluster.conf kinit -kt ~/hive-newcluster.keytab hive
KRB5_CONFIG=~/krb5_newcluster.conf klist
```

#### Step 6 — Update `.env`

Comment out the currently active preset block and replace it. Your `.env` should look
exactly like this (following the same Preset B layout):

```env
CLUSTER_PRESET=kerberized

HMS_HOST=node-1.newcluster.example.com          # ← FQDN from hostname -f

HS2_HOST=node-1.newcluster.example.com          # ← same node (optional, leave blank to disable)

HIVE_KERBEROS_PRINCIPAL=hive/node-1.newcluster.example.com@NEW-REALM.EXAMPLE.COM  # ← from klist -kt output
```

---

### All available variables

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `CLUSTER_PRESET` | | `plain` | `plain` or `cloudera_kerberos` — sets all secure defaults for Cloudera clusters. |
| `HMS_HOST` | ✅ | — | Hive Metastore host. Server exits if unset. |
| `HMS_PORT` | | `9083` | Metastore Thrift port. |
| `HMS_THRIFT_TIMEOUT_MS` | | `10000` | Thrift socket timeout (ms). |
| `HS2_HOST` | | *(empty)* | HiveServer2 host. Empty ⇒ HMS-only mode. |
| `HS2_PORT` | | `10000` / `10001`* | HiveServer2 port. (*`10001` when preset is `cloudera_kerberos`.) |
| `HS2_USER` | | *(empty)* | HS2 login user. |
| `HS2_PASSWORD` | | *(empty)* | HS2 password (LDAP/CUSTOM only). |
| `HS2_DATABASE` | | `default` | Default database for EXPLAIN sessions. |
| `HS2_AUTH` | | `NONE` | `NONE`, `LDAP`, or `CUSTOM` (overridden automatically when Kerberos is on). |
| `HS2_TRANSPORT_MODE` | | `binary` / `http`* | `binary` = Thrift (port 10000); `http` = Cloudera HTTP (port 10001). (*`http` when preset is `cloudera_kerberos`.) |
| `HS2_QUERY_TIMEOUT_S` | | `60` | EXPLAIN statement timeout (s). |
| `HIVE_USE_TLS` | | `false` / `true`* | Wrap sockets in TLS. (*`true` when preset is `cloudera_kerberos`.) |
| `HIVE_USE_KERBEROS` | | auto | Auto-enabled when `HIVE_KERBEROS_PRINCIPAL` is set, or when preset is `cloudera_kerberos`. |
| `HIVE_KERBEROS_PRINCIPAL` | | *(empty)* | Service principal, e.g. `hive/host@REALM`. **Setting this enables Kerberos automatically.** |
| `HIVE_HS2_SERVICE_NAME` | | `hive` | SASL service name (fallback when principal is not set). |
| `KRB5_CCACHE` | | *(empty)* | Kerberos credential-cache path override (exported as `KRB5CCNAME`). |

> **Do not hard-code hosts, IPs, or credentials anywhere in source or docs.** Keep them
> in `.env` only (already covered by `.gitignore`).
>
> **TLS note.** For Cloudera Auto-TLS clusters, certificate chain validation is relaxed
> (the wire is still encrypted) so the internal self-signed CA works out of the box — no
> truststore configuration is needed on the client side.

---

## Running the server

```bash
cd /path_to_HMS_MCP_Server
source .venv/bin/activate

# Either entry point works:
PYTHONPATH=.:gen-py python hivemind/hivemind_server.py
```

Expected startup logs (HMS-only mode):

```text
Connecting to HMS at <host>:9083
HMS connection established.
HS2_HOST not set — running in HMS-only mode (no EXPLAIN plans).
```

With HS2 configured and reachable:

```text
Connecting to HMS at <host>:9083
HMS connection established.
Connecting to HS2 at <host>:10000
HS2 connection established — EXPLAIN enrichment enabled.
```

If HS2 is configured but unreachable, the server keeps running:

```text
HS2 connection failed (...). Continuing in HMS-only mode.
```

---

## Registering with an MCP client

HiveMind runs over stdio. Point your MCP client at the entry point and pass the
environment through. Example client config (e.g. `.cursor/mcp.json`):

NOTE: This is the format for preset A cluster

```json
{
  "mcpServers": {
    "hivemind": {
      "command": "path_to_HMS_MCP_Server/.venv/bin/python",
      "args": ["path_to_HMS_MCP_Server/hivemind/hivemind_server.py"],
      "env": {
        "PYTHONPATH": "path_to_HMS_MCP_Server:path_to_HMS_MCP_Server/gen-py"
      }
    }
  }
}
```

NOTE: This is the format for preset B cluster

```json
{
  "mcpServers": {
    "hivemind": {
      "command": "path_to_HMS_MCP_Server/.venv/bin/python",
      "args": ["path_to_HMS_MCP_Server/hivemind/hivemind_server.py"],
      "env": {
        "PYTHONPATH": "path_to_HMS_MCP_Server:path_to_HMS_MCP_Server/gen-py",
        "KRB5_CONFIG": "path_to_config_file/krb5_cluster.conf"
      }
    }
  }
}
```

> The server also reads a `.env` file in the project root, so you can keep secrets out
> of the client config entirely and only set what you need here.

In Cursor:

1. Add the config above to `.cursor/mcp.json` with absolute paths for your machine.
   (Example: Cursor → Settings → Tools & MCP → add a custom MCP server.)
2. Confirm the HiveMind tools appear in the tool list.

---

## Usage examples

These are natural-language prompts an agent can satisfy by chaining the tools:

- **"What databases are available?"** → `list_databases`
- **"Find tables related to sales in the `analytics` database"** → `search_tables(keyword="sales", database="analytics")`
- **"Show me the schema and partitions for `analytics.orders`"** → `get_table_schema` + `get_partitions`
- **"How many rows are in `analytics.orders`?"** → `get_table_stats` (derives totals from partitions if needed)
- **"Write a query for the top 10 customers by revenue last month"** → `get_table_schema` → `get_partitions` → `text_to_hiveql`
- **"Why is this query slow?"** → `optimize_query(submitted_query=...)`
- **"Did my rewrite actually reduce the scan?"** → `compare_queries(original_query=..., optimized_query=...)`

### How `optimize_query` works

1. Validates the input (SELECT only; rejects writes; requires `database.table` refs).
2. Auto-fetches schema, partitions, and stats for every referenced table (one cached round-trip per table).
3. If HS2 is available, runs `EXPLAIN` on the **submitted** query and parses CBO TableScan row estimates, partition-pruning evidence, and join strategy.
4. Assembles a structured prompt — HMS context + parsed EXPLAIN evidence + anti-pattern rules — for the agent to produce a severity-ranked report and an optimized rewrite.

> `optimize_query` does **not** EXPLAIN the rewrite. To verify a rewrite before/after,
> use `compare_queries`, which EXPLAINs both queries.

---

## Statistics & partition limits

- **Non-partitioned tables** — `get_table_stats` reads table-level BASIC_STATS (`numRows`, `totalSize`, `numFiles`) directly from HMS table parameters.
- **Partitioned tables** — Hive stores BASIC_STATS *per partition* and usually leaves the table-level `numRows` at `-1` (unknown), even after `ANALYZE TABLE … PARTITION`. HiveMind therefore **derives** effective table totals by summing partition stats and shows a per-partition sample. This mirrors how Hive's own optimizer derives a partitioned table's row count.
- **Partition cap** — partition enumeration is capped at **500** (`PARTITION_ROLLUP_CAP`); `get_all_partitions()` is never called. Tables with more partitions get derived totals flagged as a *lower bound*.
- **Search cap** — `search_tables` returns at most **30** results globally; unscoped multi-database searches cap column matches at **20** per database so other databases can still contribute.

---

## Security model

- **Read-only.** No tool executes a query or returns table rows. HS2 is restricted to `EXPLAIN <query>` only.
- **No writes generated.** Write verbs are rejected before any tool runs.
- **Credential redaction.** Any table parameter whose key contains `key`, `secret`, `password`, `token`, `credential`, or `access` is replaced with `[REDACTED]` in tool output (and known S3/Azure/GCS key properties are always redacted).
- **No secrets in source.** Hosts and credentials live only in `.env`, which is git-ignored. Keep them out of code, docs, and client configs you commit.
- **Encapsulated.** Tool implementations are module-private and only reachable through the MCP protocol.

---

## Development & testing

```bash
# install dev extras
pip install -e ".[dev]"

# run the test suite
python3 -m pytest tests/ -q
```

- Tests use fakes/stubs and need **no live cluster** or Thrift connection.
- `pytest` is configured for `asyncio_mode = auto` (see `pyproject.toml`), so async handler tests run without extra decorators.

---

## Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| `HMS_HOST is not set` then exit | Add `HMS_HOST` to `.env` or the client `env` block. |
| `Thrift bindings not found` | Generate stubs into `gen-py/hive_metastore/` (see [Installation](#installation)). |
| `PyHive is not installed` | `pip install 'pyhive[hive]'` — only needed for HS2 EXPLAIN. |
| HS2 features missing | `HS2_HOST` unset or unreachable; server runs HMS-only. Check logs for the warning. |
| Table-level row count shows `unknown` | Expected for partitioned tables — use the **derived totals** in the output, or run `ANALYZE TABLE … PARTITION … COMPUTE STATISTICS`. |
| `no fully-qualified table references found` | Use `database.table` form in `FROM`/`JOIN` for `optimize_query` / `compare_queries`. |

---

## Project layout

```
HMS_MCP_Server/
├── hivemind/
│   ├── hivemind_server.py     # MCP server entry point; registers all tools
│   ├── hms_client.py          # Thrift HMS client (read-only, thread-safe)
│   ├── hs2_client.py          # Optional PyHive HS2 client (EXPLAIN only)
│   └── tools/
│       ├── discovery.py       # list/search/schema/stats/partitions/ddl
│       ├── sql_gen.py         # text_to_hiveql + shared parsing helpers
│       ├── optimize.py        # optimize_query
│       ├── explain.py         # explain_query
│       ├── explain_plan.py    # EXPLAIN plan parsing + report builders
│       └── compare.py         # compare_queries
├── gen-py/                    # generated Thrift bindings (hive_metastore, fb303)
├── tests/                     # pytest suite (no live cluster required)
├── pyproject.toml             # package metadata + dependencies
└── README.md
```

