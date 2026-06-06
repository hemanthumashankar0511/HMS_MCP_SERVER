from hivemind.tools.sql_gen import (
    _build_hints,
    _default_limit,
    _detect_query_type,
    _format_row_count_abbreviated,
    _is_error_context,
    _is_write_operation_request,
    _parse_columns_and_partitions,
    _parse_row_counts,
    _parse_tables,
    _strip_backticks,
    handle_text_to_hiveql,
)

_SCHEMA_CTX = """\
Schema: sales.orders
==================================================
  Type    : Managed
  Format  : ORC
  Location: /warehouse/sales.db/orders
Columns:
  Name                           Type                 Comment
  -----------------------------------------------------------------
  order_id                       bigint
  created_date                   string
  amount                         double
Partition Keys:
  Name                           Type                 Comment
  -----------------------------------------------------------------
  year                           int
"""

_STATS_CTX = """\
Statistics: sales.orders
==================================================
Table-level BASIC_STATS:
  Rows       : 1,234,567
  Total size : 48.2 KB
  Files      : 3
"""

_STATS_ROLLUP_CTX = """\
Statistics: sales.orders
Table-level BASIC_STATS:
  Rows       : unknown
  Sum(rows) over partitions with usable numRows (3 partition(s)): 500
"""

_STATS_DERIVED_CTX = """\
Statistics: sales.orders
==================================================
Table-level BASIC_STATS:
  Rows       : unknown
  Total size : unknown
  Files      : unknown

Derived table-level totals (aggregated from partition BASIC_STATS; ...):
  Rows      : 1,500  (from 3/3 partition(s))
  Total size: 6.0 KB  (from 3/3 partition(s))
  Files     : 9  (from 3/3 partition(s))
"""


def test_parse_tables():
    assert _parse_tables(_SCHEMA_CTX) == ["sales.orders"]
    assert _parse_tables("no schema here") == []


def test_parse_row_counts_real_value():
    assert _parse_row_counts(_STATS_CTX) == {"sales.orders": "1,234,567"}


def test_parse_row_counts_partition_rollup():
    # When the table-level Rows line is "unknown", the partition rollup sum is used.
    assert _parse_row_counts(_STATS_ROLLUP_CTX) == {"sales.orders": "500"}


def test_parse_row_counts_derived_totals():
    # The derived "Rows : N (from .../...)" line supplies the effective count when the
    # table-level Rows line reads "unknown"; the trailing annotation is ignored.
    assert _parse_row_counts(_STATS_DERIVED_CTX) == {"sales.orders": "1,500"}


def test_parse_columns_and_partitions():
    pkeys, date_cols = _parse_columns_and_partitions(_SCHEMA_CTX)
    assert pkeys == {"sales.orders": ["year"]}
    assert date_cols == {"sales.orders": ["created_date"]}


def test_detect_query_type():
    assert "aggregation with ORDER BY" in _detect_query_type("top 5 customers by revenue")
    assert "time-series" in _detect_query_type("show revenue trend over time")
    assert _detect_query_type("count of active users") == "aggregation or count"
    assert _detect_query_type("find user records") == "filtered SELECT"
    assert _detect_query_type("hello there") == "general SELECT"


def test_default_limit():
    assert _default_limit("top 5 customers", "aggregation with ORDER BY … DESC/ASC LIMIT N") == "5"
    assert _default_limit("top customers", "aggregation with ORDER BY … DESC/ASC LIMIT N") == "10"
    assert _default_limit("revenue trend", "time-series aggregation grouped by date/period column") == "30"
    assert _default_limit("list everything", "general SELECT") == "100"


def test_format_row_count_abbreviated():
    assert _format_row_count_abbreviated("1,234,567") == "~1.2M"
    assert _format_row_count_abbreviated("1500") == "~1.5K"
    assert _format_row_count_abbreviated("2000000000") == "~2.0B"
    assert _format_row_count_abbreviated("500") == "500"
    assert _format_row_count_abbreviated("notanumber") == "notanumber"


def test_is_write_operation_request():
    assert _is_write_operation_request("delete inactive users") == (True, "DELETE")
    assert _is_write_operation_request("insert the missing rows") == (True, "INSERT")
    assert _is_write_operation_request("show me the top customers") == (False, "")


def test_strip_backticks():
    assert _strip_backticks("SELECT `col` FROM `db`.`t`") == "SELECT col FROM db.t"


def test_is_error_context():
    assert _is_error_context("No tables found matching 'foo'.") is True
    assert _is_error_context("Error fetching schema for 'db.t': boom") is True
    assert _is_error_context(_SCHEMA_CTX) is False


def test_build_hints_with_and_without_rows():
    with_rows = _build_hints(
        ["sales.orders"],
        {"sales.orders": "1,234,567"},
        {"sales.orders": ["year"]},
        {"sales.orders": ["created_date"]},
        include_footer=True,
    )
    assert "=== METASTORE HINTS ===" in with_rows
    assert "=== END HINTS ===" in with_rows
    assert "Row counts from HMS stats:" in with_rows

    without_rows = _build_hints([], {}, {}, {}, include_footer=False)
    assert "Row counts unavailable in HMS stats." in without_rows


async def test_handle_text_to_hiveql_empty_query():
    out = await handle_text_to_hiveql("", _SCHEMA_CTX)
    assert out == "Error: natural_query cannot be empty."


async def test_handle_text_to_hiveql_empty_context():
    out = await handle_text_to_hiveql("top customers", "")
    assert out.startswith("Error: assembled_context is empty.")


async def test_handle_text_to_hiveql_error_context():
    out = await handle_text_to_hiveql("top customers", "No tables found matching 'x'.")
    assert "previous table search or metadata lookup failed" in out


async def test_handle_text_to_hiveql_write_blocked():
    out = await handle_text_to_hiveql("delete old orders", _SCHEMA_CTX)
    assert out.startswith("Error: DELETE operations are not supported.")


async def test_handle_text_to_hiveql_happy_path():
    out = await handle_text_to_hiveql("top 5 customers by revenue", _SCHEMA_CTX + "\n\n" + _STATS_CTX)
    assert "=== METASTORE HINTS ===" in out
    assert "top 5 customers by revenue" in out
