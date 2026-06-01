from hivemind.tools.optimize import _extract_tables_from_sql


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


def test_extract_tables_comma_join_not_captured():
    # Locks in current behavior: only db.table after FROM/JOIN is captured,
    # so the second comma-separated source is intentionally not extracted.
    sql = "SELECT * FROM a.b, c.d"
    assert _extract_tables_from_sql(sql) == [("a", "b")]


def test_extract_tables_none_when_unqualified():
    sql = "SELECT * FROM orders WHERE id = 1"
    assert _extract_tables_from_sql(sql) == []
