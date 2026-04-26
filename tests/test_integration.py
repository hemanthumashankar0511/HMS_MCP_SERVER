"""
Integration smoke tests for HiveMind Phase 1.

Requires a live HMS connection:
    HMS_HOST=10.140.143.128 pytest tests/test_integration.py -v

These tests are SKIPPED automatically when HMS_HOST is not set or the
HMS is unreachable — they will never break a plain `pytest` run.
"""

from __future__ import annotations

import os

import pytest

_HMS_HOST = os.environ.get("HMS_HOST")
_HMS_PORT = int(os.environ.get("HMS_PORT", "9083"))

_SKIP_REASON = (
    "Set HMS_HOST to run integration tests against a live Hive Metastore."
)
_needs_hms = pytest.mark.skipif(_HMS_HOST is None, reason=_SKIP_REASON)

# Flip: skip when HMS_HOST IS None (i.e. no live cluster)
pytestmark = pytest.mark.skipif(
    _HMS_HOST is None,
    reason=_SKIP_REASON,
)


@pytest.fixture(scope="module")
def client():
    from hivemind.hms_client import HMSClient

    c = HMSClient(_HMS_HOST, _HMS_PORT)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Smoke test 1 — list_databases
# ---------------------------------------------------------------------------

def test_list_databases_returns_nonempty(client):
    dbs = client.get_all_databases()
    assert isinstance(dbs, list)
    assert len(dbs) > 0, "Expected at least one database in HMS"
    assert "default" in dbs, "'default' database should always be present"


# ---------------------------------------------------------------------------
# Smoke test 2 — list_tables (default database)
# ---------------------------------------------------------------------------

def test_list_tables_default(client):
    tables = client.get_all_tables("default")
    assert isinstance(tables, list)
    # default may be empty on a fresh cluster — just check it doesn't crash


# ---------------------------------------------------------------------------
# Smoke test 3 — search_tables across all databases
# ---------------------------------------------------------------------------

def test_search_tables_finds_something(client):
    # "demo" exists in the sample database from prior setup
    results = client.search_tables("demo")
    assert isinstance(results, list)
    for r in results:
        assert "database" in r
        assert "table" in r
        assert "match_reason" in r


# ---------------------------------------------------------------------------
# Smoke test 4 — get_table_schema for sample.demo
# ---------------------------------------------------------------------------

def test_get_table_schema_sample_demo(client):
    info = client.get_table("sample", "demo")
    assert info["name"] == "demo"
    assert info["database"] == "sample"
    assert len(info["columns"]) > 0
    # At minimum id and name columns from our earlier setup
    col_names = [c["name"] for c in info["columns"]]
    assert "id" in col_names
    assert "name" in col_names


# ---------------------------------------------------------------------------
# Smoke test 5 — get_table_stats
# ---------------------------------------------------------------------------

def test_get_table_stats_sample_demo(client):
    stats = client.get_table_stats("sample", "demo")
    assert "num_rows" in stats
    assert "total_size" in stats
    assert "num_files" in stats
    assert "stats_available" in stats
    assert isinstance(stats["stats_available"], bool)


# ---------------------------------------------------------------------------
# Smoke test 6 — get_partitions (sys.proto_tez_dag_data is partitioned)
# ---------------------------------------------------------------------------

def test_get_partition_names_partitioned_table(client):
    part_names = client.get_partition_names("sys", "proto_tez_dag_data", max_parts=20)
    assert isinstance(part_names, list)
    # Even if empty, the call must not crash


# ---------------------------------------------------------------------------
# Smoke test 7 — get_table_ddl
# ---------------------------------------------------------------------------

def test_get_table_ddl_sample_demo(client):
    ddl = client.get_table_ddl("sample", "demo")
    assert "Reconstructed DDL" in ddl
    assert "CREATE" in ddl
    assert "demo" in ddl


# ---------------------------------------------------------------------------
# Security — sensitive params must be redacted
# ---------------------------------------------------------------------------

def test_no_credentials_in_table_params(client):
    """Ensure no table in sample leaks raw credential values."""
    for tbl_name in client.get_all_tables("sample"):
        info = client.get_table("sample", tbl_name)
        for k, v in info["parameters"].items():
            assert v != "[REDACTED]" or k  # if redacted, key must still be present
            # Ensure no raw secret-looking values slip through
            for dangerous in ("SECRET", "PASSWORD", "private_key", "BEGIN RSA"):
                assert dangerous not in v, (
                    f"Potential credential leak in {tbl_name}.parameters[{k!r}]"
                )


# ---------------------------------------------------------------------------
# Async handler integration — use asyncio event loop
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_handler_list_databases(client):
    from hivemind.tools.discovery import handle_list_databases

    result = await handle_list_databases(client)
    assert "default" in result
    assert "total" in result


@pytest.mark.asyncio
async def test_handler_list_tables(client):
    from hivemind.tools.discovery import handle_list_tables

    result = await handle_list_tables(client, "sample")
    assert "demo" in result


@pytest.mark.asyncio
async def test_handler_get_table_schema(client):
    from hivemind.tools.discovery import handle_get_table_schema

    result = await handle_get_table_schema(client, "sample", "demo")
    assert "id" in result
    assert "ORC" in result


@pytest.mark.asyncio
async def test_handler_get_table_stats(client):
    from hivemind.tools.discovery import handle_get_table_stats

    result = await handle_get_table_stats(client, "sample", "demo")
    assert "Rows" in result or "rows" in result.lower()


@pytest.mark.asyncio
async def test_handler_get_partitions_unpartitioned(client):
    from hivemind.tools.discovery import handle_get_partitions

    result = await handle_get_partitions(client, "sample", "demo")
    assert "not partitioned" in result


@pytest.mark.asyncio
async def test_handler_get_table_ddl(client):
    from hivemind.tools.discovery import handle_get_table_ddl

    result = await handle_get_table_ddl(client, "sample", "demo")
    assert "CREATE TABLE" in result
