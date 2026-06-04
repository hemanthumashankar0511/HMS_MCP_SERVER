import contextlib

from hivemind.tools.optimize import _extract_tables_from_sql, handle_optimize_query


def test_extract_tables_from_join():
    sql = (
        "SELECT o.id, c.name "
        "FROM sales.orders o "
        "JOIN sales.customers c ON o.cust_id = c.id "
        "WHERE o.year = 2025"
    )
    assert _extract_tables_from_sql(sql) == [("sales", "orders"), ("sales", "customers")]


def test_extract_tables_dedup_case_insensitive():
    sql = "SELECT * FROM Sales.Orders a JOIN sales.orders b ON a.id = b.id"
    assert _extract_tables_from_sql(sql) == [("Sales", "Orders")]


def test_extract_tables_comma_join_captures_both():
    sql = "SELECT * FROM a.b x, c.d y WHERE x.id = y.id"
    assert _extract_tables_from_sql(sql) == [("a", "b"), ("c", "d")]


def test_extract_tables_none_when_unqualified():
    sql = "SELECT * FROM orders WHERE id = 1"
    assert _extract_tables_from_sql(sql) == []


class _FakeOptimizeClient:
    """Minimal HMS client supporting the metadata calls handle_optimize_query makes."""

    @contextlib.contextmanager
    def request_cache(self):
        yield

    def get_table(self, database, table):
        return {
            "name": table,
            "database": database,
            "table_type": "MANAGED_TABLE",
            "columns": [{"name": "amount", "type": "double", "comment": ""}],
            "partition_keys": [{"name": "sale_date", "type": "string", "comment": ""}],
            "parameters": {"transactional": "true"},
            "location": "/warehouse/sample.db/sales_transactions",
            "input_format": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
            "output_format": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
            "serde": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
            "num_files": "3",
            "num_rows": "10000",
            "total_size": "900000",
        }

    def get_table_stats(self, database, table):
        return {
            "num_rows": "10000",
            "total_size": "900000",
            "num_files": "3",
            "stats_available": True,
            "last_modified": "",
        }

    def get_partition_names(self, database, table, max_parts=20):
        return ["sale_date=2026-05-11"]

    def get_partition_basic_stats_bulk(self, database, table, names, key_names):
        return {"sale_date=2026-05-11": {"num_rows": "500", "num_files": "1", "total_size": "4500"}}

    def get_partition_basic_stats(self, database, table, name):
        return {"num_rows": "500", "num_files": "1", "total_size": "4500"}


class _FakeHS2:
    def __init__(self, available=True):
        self._available = available
        self.calls = []

    def is_available(self):
        return self._available

    def explain_with_row_estimates(self, query, hms_total_rows=None, table=None, compact=False):
        self.calls.append({"query": query, "hms_total_rows": hms_total_rows, "table": table, "compact": compact})
        return "HS2 EXPLAIN PLAN\nCBO SCAN ESTIMATES\n  sales_transactions: 10,000 rows\nReduction: ~95.0%"


_SELECT = "SELECT amount FROM sample.sales_transactions WHERE sale_date = '2026-05-11'"


async def test_optimize_includes_hs2_block_when_available():
    hs2 = _FakeHS2()
    out = await handle_optimize_query(_FakeOptimizeClient(), _SELECT, hs2)
    assert "HS2 EXPLAIN context:" in out
    assert "Reduction: ~95.0%" in out
    assert hs2.calls[0]["hms_total_rows"] == 10000


async def test_optimize_fallback_when_hs2_none():
    out = await handle_optimize_query(_FakeOptimizeClient(), _SELECT, None)
    assert "HS2 EXPLAIN context:" in out
    assert "HS2 EXPLAIN unavailable" in out


async def test_optimize_blocks_writes_without_hs2_call():
    hs2 = _FakeHS2()
    out = await handle_optimize_query(_FakeOptimizeClient(), "DELETE FROM sample.t", hs2)
    assert "not supported" in out
    assert hs2.calls == []
