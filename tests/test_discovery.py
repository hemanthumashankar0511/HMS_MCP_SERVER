from hivemind.tools.discovery import (
    _format_bytes,
    _format_count,
    _format_table_type,
    _is_missing_stat,
    _stat_int,
    handle_get_partitions,
    handle_get_table_schema,
    handle_list_databases,
)


class FakeHMSClient:
    """Minimal stand-in for HMSClient that needs no Thrift/metastore connection."""

    def __init__(self, databases=None, tables=None):
        self._databases = databases or ["default", "sales"]
        self._tables = tables or {}

    def get_all_databases(self):
        return list(self._databases)

    def get_table(self, database, table):
        return {
            "name": table,
            "database": database,
            "table_type": "MANAGED_TABLE",
            "columns": [
                {"name": "order_id", "type": "bigint", "comment": ""},
                {"name": "amount", "type": "double", "comment": "in USD"},
            ],
            "partition_keys": [{"name": "year", "type": "int", "comment": ""}],
            "parameters": {"transactional": "true"},
            "location": "/warehouse/sales.db/orders",
            "input_format": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "output_format": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            "serde": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
            "num_files": "3",
            "num_rows": "100",
            "total_size": "1024",
        }


def test_is_missing_stat():
    assert _is_missing_stat(None) is True
    assert _is_missing_stat("") is True
    assert _is_missing_stat("N/A") is True
    assert _is_missing_stat("-1") is True
    assert _is_missing_stat("unknown") is True
    assert _is_missing_stat("0") is False
    assert _is_missing_stat("42") is False


def test_stat_int():
    assert _stat_int("1,234") == 1234
    assert _stat_int("5.0") == 5
    assert _stat_int("-1") == -1
    assert _stat_int("") == -1
    assert _stat_int("abc") == -1
    assert _stat_int(None) == -1


def test_format_bytes():
    assert _format_bytes("-1") == "unknown"
    assert _format_bytes("0") == "0.0 B"
    assert _format_bytes("1024") == "1.0 KB"
    assert _format_bytes("1048576") == "1.0 MB"
    # consistency fix: comma/float strings now format instead of returning raw
    assert _format_bytes("1,024") == "1.0 KB"
    assert _format_bytes("1024.0") == "1.0 KB"


def test_format_count():
    assert _format_count("1234567") == "1,234,567"
    assert _format_count("-1") == "unknown"
    assert _format_count("abc") == "abc"


def test_format_table_type():
    assert _format_table_type("MANAGED_TABLE") == "Managed"
    assert _format_table_type("EXTERNAL_TABLE") == "External"
    assert _format_table_type("VIRTUAL_VIEW") == "View"
    assert _format_table_type("MATERIALIZED_VIEW") == "Materialized View"
    assert _format_table_type("SOMETHING_ELSE") == "SOMETHING_ELSE"


async def test_handle_list_databases():
    out = await handle_list_databases(FakeHMSClient(databases=["alpha", "beta"]))
    assert "Databases in Hive Metastore (2 total)" in out
    assert "alpha" in out and "beta" in out


async def test_handle_get_table_schema():
    out = await handle_get_table_schema(FakeHMSClient(), "sales", "orders")
    assert "Schema: sales.orders" in out
    assert "Format  : ORC" in out
    assert "order_id" in out
    assert "Partition Keys:" in out
    assert "year" in out


class FakePartitionedClient:
    """Exercises the bulk partition-stats path (and its per-name fallback)."""

    def __init__(self, bulk_missing=False):
        self._bulk_missing = bulk_missing
        self.bulk_calls = 0
        self.per_name_calls = 0

    def get_table(self, database, table):
        return {
            "partition_keys": [{"name": "year", "type": "int", "comment": ""}],
        }

    def get_partition_names(self, database, table, max_parts=20):
        return ["year=2024", "year=2025"]

    def get_partition_basic_stats_bulk(self, database, table, names, key_names):
        self.bulk_calls += 1
        stats = {
            "year=2024": {"num_rows": "100", "num_files": "2", "total_size": "2048"},
        }
        if not self._bulk_missing:
            stats["year=2025"] = {"num_rows": "200", "num_files": "3", "total_size": "4096"}
        return stats

    def get_partition_basic_stats(self, database, table, name):
        self.per_name_calls += 1
        return {"num_rows": "200", "num_files": "3", "total_size": "4096"}


async def test_handle_get_partitions_uses_bulk_single_call():
    client = FakePartitionedClient()
    out = await handle_get_partitions(client, "sales", "txns")
    assert client.bulk_calls == 1
    # All names present in bulk result -> no per-name fallback round-trips.
    assert client.per_name_calls == 0
    assert "year=2024" in out and "year=2025" in out
    assert "rows=100" in out and "rows=200" in out


async def test_handle_get_partitions_falls_back_for_missing_name():
    client = FakePartitionedClient(bulk_missing=True)
    out = await handle_get_partitions(client, "sales", "txns")
    assert client.bulk_calls == 1
    # year=2025 was absent from the bulk result, so exactly one fallback call is made.
    assert client.per_name_calls == 1
    assert "rows=100" in out and "rows=200" in out
