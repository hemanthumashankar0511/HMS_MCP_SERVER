"""
Unit tests for compare.py partition-filter extraction helpers.
"""
from hivemind.tools.compare import (
    _extract_partition_filter_values,
    _lookup_partition_sample_rows,
)

# ---------------------------------------------------------------------------
# _extract_partition_filter_values
# ---------------------------------------------------------------------------

def test_extract_integer_partition_filter():
    sql = "SELECT * FROM db.store_sales WHERE ss_sold_date_sk = 2450816"
    result = _extract_partition_filter_values(sql, ["ss_sold_date_sk"])
    assert result == {"ss_sold_date_sk": "2450816"}


def test_extract_quoted_string_partition_filter():
    sql = "SELECT * FROM sample.sales WHERE sale_date = '2026-05-11'"
    result = _extract_partition_filter_values(sql, ["sale_date"])
    assert result == {"sale_date": "2026-05-11"}


def test_extract_multiple_partition_keys():
    sql = "SELECT * FROM db.t WHERE region = 'US' AND ds = '2026-01-01'"
    result = _extract_partition_filter_values(sql, ["region", "ds"])
    assert result == {"region": "US", "ds": "2026-01-01"}


def test_extract_no_match_returns_empty():
    sql = "SELECT * FROM db.t WHERE customer_id = 42"
    result = _extract_partition_filter_values(sql, ["ss_sold_date_sk"])
    assert result == {}


def test_extract_case_insensitive():
    sql = "SELECT * FROM db.t WHERE SS_SOLD_DATE_SK = 2450816"
    result = _extract_partition_filter_values(sql, ["ss_sold_date_sk"])
    assert result == {"ss_sold_date_sk": "2450816"}


def test_extract_with_spaces_around_equals():
    sql = "WHERE ss_sold_date_sk=2450816"
    result = _extract_partition_filter_values(sql, ["ss_sold_date_sk"])
    assert result == {"ss_sold_date_sk": "2450816"}


def test_extract_empty_partition_keys_returns_empty():
    sql = "SELECT * FROM db.t WHERE x = 1"
    assert _extract_partition_filter_values(sql, []) == {}


# ---------------------------------------------------------------------------
# _lookup_partition_sample_rows
# ---------------------------------------------------------------------------

_PARTITION_CONTEXT = """\
Partition Key Structure:
  ss_sold_date_sk bigint

Partition-level BASIC_STATS sample:
  ss_sold_date_sk=2450816  rows=88,103  files=1  size=3.1 MB
  ss_sold_date_sk=2450817  rows=86,226  files=1  size=3.1 MB
  ss_sold_date_sk=2450818  rows=87,624  files=1  size=3.1 MB
"""

_PARTITION_CONTEXT_STRING = """\
  sale_date=2026-05-11  rows=500  files=1  size=7.7 KB
"""


def test_lookup_integer_partition():
    n = _lookup_partition_sample_rows(_PARTITION_CONTEXT, "ss_sold_date_sk", "2450816")
    assert n == 88_103


def test_lookup_another_partition_value():
    n = _lookup_partition_sample_rows(_PARTITION_CONTEXT, "ss_sold_date_sk", "2450817")
    assert n == 86_226


def test_lookup_string_partition():
    n = _lookup_partition_sample_rows(_PARTITION_CONTEXT_STRING, "sale_date", "2026-05-11")
    assert n == 500


def test_lookup_missing_value_returns_none():
    n = _lookup_partition_sample_rows(_PARTITION_CONTEXT, "ss_sold_date_sk", "9999999")
    assert n is None


def test_lookup_wrong_key_returns_none():
    n = _lookup_partition_sample_rows(_PARTITION_CONTEXT, "bad_key", "2450816")
    assert n is None


def test_lookup_empty_context_returns_none():
    assert _lookup_partition_sample_rows("", "ss_sold_date_sk", "2450816") is None
