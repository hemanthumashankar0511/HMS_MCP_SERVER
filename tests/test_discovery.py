"""
Unit tests for discovery tool handlers (hivemind/tools/discovery.py).

Tests check the formatted text output — no live HMS connection required.
Run with:  pytest tests/test_discovery.py -v
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers — mock HMSClient factory
# ---------------------------------------------------------------------------

def _mock_client(**overrides):
    """Return a MagicMock wired like an HMSClient with sensible defaults."""
    c = MagicMock()
    c.get_all_databases.return_value = ["default", "sample", "sys"]
    c.get_all_tables.return_value = ["demo", "demo2"]
    c.get_table.return_value = {
        "name": "demo",
        "database": "sample",
        "table_type": "MANAGED_TABLE",
        "columns": [
            {"name": "id", "type": "int", "comment": ""},
            {"name": "name", "type": "string", "comment": "primary name field"},
        ],
        "partition_keys": [],
        "parameters": {"transactional": "true"},
        "location": "hdfs://host/warehouse/sample.db/demo",
        "input_format": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
        "serde": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        "num_files": "2",
        "num_rows": "5000",
        "total_size": "102400",
    }
    c.get_table_stats.return_value = {
        "num_rows": "5000",
        "total_size": "102400",
        "num_files": "2",
        "stats_available": True,
        "last_modified": "1777045903",
    }
    c.get_partition_names.return_value = ["dt=2024-01-01", "dt=2024-01-02"]
    c.search_tables.return_value = [
        {"database": "sample", "table": "demo", "match_reason": "table name"}
    ]
    c.get_table_ddl.return_value = (
        "-- Reconstructed DDL (from HMS metadata — not original source DDL)\n"
        "CREATE TABLE `sample`.`demo` (\n"
        "  `id` int,\n"
        "  `name` string\n"
        ");\n"
    )
    for k, v in overrides.items():
        setattr(c, k, v)
    return c


# ---------------------------------------------------------------------------
# handle_list_databases
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_databases_shows_all():
    from hivemind.tools.discovery import handle_list_databases

    result = await handle_list_databases(_mock_client())
    assert "default" in result
    assert "sample" in result
    assert "sys" in result
    assert "3 total" in result


@pytest.mark.asyncio
async def test_list_databases_empty():
    from hivemind.tools.discovery import handle_list_databases

    c = _mock_client()
    c.get_all_databases.return_value = []
    result = await handle_list_databases(c)
    assert "No databases" in result


@pytest.mark.asyncio
async def test_list_databases_error():
    from hivemind.tools.discovery import handle_list_databases

    c = _mock_client()
    c.get_all_databases.side_effect = RuntimeError("Thrift timeout")
    result = await handle_list_databases(c)
    assert "Error" in result
    assert "Thrift timeout" in result


# ---------------------------------------------------------------------------
# handle_list_tables
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_list_tables_shows_tables():
    from hivemind.tools.discovery import handle_list_tables

    result = await handle_list_tables(_mock_client(), "sample")
    assert "demo" in result
    assert "demo2" in result
    assert "2 total" in result


@pytest.mark.asyncio
async def test_list_tables_empty_db():
    from hivemind.tools.discovery import handle_list_tables

    c = _mock_client()
    c.get_all_tables.return_value = []
    result = await handle_list_tables(c, "emptydb")
    assert "No tables" in result


@pytest.mark.asyncio
async def test_list_tables_error_message():
    from hivemind.tools.discovery import handle_list_tables

    c = _mock_client()
    c.get_all_tables.side_effect = Exception("connection refused")
    result = await handle_list_tables(c, "broken")
    assert "Error" in result


# ---------------------------------------------------------------------------
# handle_search_tables
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_tables_shows_matches():
    from hivemind.tools.discovery import handle_search_tables

    result = await handle_search_tables(_mock_client(), "demo")
    assert "demo" in result
    assert "sample" in result


@pytest.mark.asyncio
async def test_search_tables_no_results():
    from hivemind.tools.discovery import handle_search_tables

    c = _mock_client()
    c.search_tables.return_value = []
    result = await handle_search_tables(c, "zzz_nonexistent")
    assert "No tables found" in result


@pytest.mark.asyncio
async def test_search_tables_scoped_database_mentioned():
    from hivemind.tools.discovery import handle_search_tables

    c = _mock_client()
    c.search_tables.return_value = []
    result = await handle_search_tables(c, "foo", "sample")
    assert "sample" in result


# ---------------------------------------------------------------------------
# handle_get_table_schema
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_schema_shows_columns():
    from hivemind.tools.discovery import handle_get_table_schema

    result = await handle_get_table_schema(_mock_client(), "sample", "demo")
    assert "id" in result
    assert "name" in result
    assert "int" in result
    assert "string" in result


@pytest.mark.asyncio
async def test_schema_shows_table_type_and_format():
    from hivemind.tools.discovery import handle_get_table_schema

    result = await handle_get_table_schema(_mock_client(), "sample", "demo")
    assert "Managed" in result or "MANAGED" in result
    assert "ORC" in result


@pytest.mark.asyncio
async def test_schema_stats_warning_when_missing():
    from hivemind.tools.discovery import handle_get_table_schema

    c = _mock_client()
    tbl = dict(c.get_table.return_value)
    tbl["num_rows"] = "-1"
    c.get_table.return_value = tbl
    result = await handle_get_table_schema(c, "sample", "demo")
    assert "ANALYZE TABLE" in result


@pytest.mark.asyncio
async def test_schema_shows_partition_keys():
    from hivemind.tools.discovery import handle_get_table_schema

    c = _mock_client()
    tbl = dict(c.get_table.return_value)
    tbl["partition_keys"] = [{"name": "dt", "type": "string", "comment": ""}]
    c.get_table.return_value = tbl
    result = await handle_get_table_schema(c, "sample", "demo")
    assert "Partition Keys" in result
    assert "dt" in result


@pytest.mark.asyncio
async def test_schema_error_handled():
    from hivemind.tools.discovery import handle_get_table_schema

    c = _mock_client()
    c.get_table.side_effect = Exception("table not found")
    result = await handle_get_table_schema(c, "sample", "ghost")
    assert "Error" in result


# ---------------------------------------------------------------------------
# handle_get_table_stats
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_shows_row_count():
    from hivemind.tools.discovery import handle_get_table_stats

    result = await handle_get_table_stats(_mock_client(), "sample", "demo")
    assert "5,000" in result or "5000" in result


@pytest.mark.asyncio
async def test_stats_shows_size():
    from hivemind.tools.discovery import handle_get_table_stats

    result = await handle_get_table_stats(_mock_client(), "sample", "demo")
    # 102400 bytes = 100.0 KB
    assert "KB" in result or "100" in result


@pytest.mark.asyncio
async def test_stats_warning_when_unavailable():
    from hivemind.tools.discovery import handle_get_table_stats

    c = _mock_client()
    c.get_table_stats.return_value = {
        "num_rows": "-1",
        "total_size": "0",
        "num_files": "0",
        "stats_available": False,
        "last_modified": "",
    }
    result = await handle_get_table_stats(c, "sample", "demo")
    assert "Statistics missing" in result
    assert "ANALYZE TABLE" in result


# ---------------------------------------------------------------------------
# handle_get_partitions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_partitions_not_partitioned():
    from hivemind.tools.discovery import handle_get_partitions

    c = _mock_client()
    tbl = dict(c.get_table.return_value)
    tbl["partition_keys"] = []
    c.get_table.return_value = tbl
    result = await handle_get_partitions(c, "sample", "demo")
    assert "not partitioned" in result


@pytest.mark.asyncio
async def test_partitions_shows_key_structure():
    from hivemind.tools.discovery import handle_get_partitions

    c = _mock_client()
    tbl = dict(c.get_table.return_value)
    tbl["partition_keys"] = [{"name": "dt", "type": "string", "comment": ""}]
    c.get_table.return_value = tbl
    result = await handle_get_partitions(c, "sample", "demo")
    assert "dt" in result
    assert "string" in result


@pytest.mark.asyncio
async def test_partitions_shows_values():
    from hivemind.tools.discovery import handle_get_partitions

    c = _mock_client()
    tbl = dict(c.get_table.return_value)
    tbl["partition_keys"] = [{"name": "dt", "type": "string", "comment": ""}]
    c.get_table.return_value = tbl
    result = await handle_get_partitions(c, "sample", "demo")
    assert "2024-01-01" in result
    assert "most recent" in result


# ---------------------------------------------------------------------------
# handle_get_table_ddl
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ddl_contains_create_table():
    from hivemind.tools.discovery import handle_get_table_ddl

    result = await handle_get_table_ddl(_mock_client(), "sample", "demo")
    assert "CREATE TABLE" in result or "CREATE" in result


@pytest.mark.asyncio
async def test_ddl_labelled_as_reconstructed():
    from hivemind.tools.discovery import handle_get_table_ddl

    result = await handle_get_table_ddl(_mock_client(), "sample", "demo")
    assert "Reconstructed" in result or "reconstructed" in result


@pytest.mark.asyncio
async def test_ddl_error_handled():
    from hivemind.tools.discovery import handle_get_table_ddl

    c = _mock_client()
    c.get_table_ddl.side_effect = Exception("thrift error")
    result = await handle_get_table_ddl(c, "sample", "ghost")
    assert "Error" in result
