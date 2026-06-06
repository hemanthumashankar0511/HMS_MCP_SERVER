from hivemind.hms_client import (
    HMSClient,
    _field_to_dict,
    _friendly_format,
    _sanitize_params,
)


class _Field:
    def __init__(self, name, type_, comment=""):
        self.name = name
        self.type = type_
        self.comment = comment


class _Serde:
    serializationLib = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"


class _Sd:
    location = "/warehouse/db/t"
    inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
    outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
    serdeInfo = _Serde()
    cols = [_Field("id", "bigint")]


class _Table:
    tableName = "t"
    dbName = "db"
    tableType = "MANAGED_TABLE"
    sd = _Sd()
    parameters = {"numRows": "10", "numFiles": "1", "totalSize": "1024"}
    partitionKeys = [_Field("year", "int")]


class _Partition:
    def __init__(self, values, parameters):
        self.values = values
        self.parameters = parameters


def _bare_client():
    """Construct an HMSClient without connecting (for unit-testing pure logic)."""
    return HMSClient.__new__(HMSClient)


def test_friendly_format_known():
    assert _friendly_format("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat") == "ORC"
    assert _friendly_format("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat") == "Parquet"


def test_friendly_format_unknown_falls_back_to_class_tail():
    assert _friendly_format("com.example.MyCustomFormat") == "MyCustomFormat"
    assert _friendly_format("") == "Unknown"


def test_sanitize_params_redacts_sensitive_keys():
    params = {
        "fs.s3.awsSecretAccessKey": "shhh",
        "password": "p@ss",
        "my_token": "tok",
        "owner": "alice",
        "numRows": "100",
    }
    out = _sanitize_params(params)
    assert out["fs.s3.awsSecretAccessKey"] == "[REDACTED]"
    assert out["password"] == "[REDACTED]"
    assert out["my_token"] == "[REDACTED]"
    # non-sensitive keys preserved verbatim
    assert out["owner"] == "alice"
    assert out["numRows"] == "100"


def test_field_to_dict():
    f = _Field("order_id", "bigint", "primary key")
    assert _field_to_dict(f) == {"name": "order_id", "type": "bigint", "comment": "primary key"}


def test_field_to_dict_handles_none():
    f = _Field(None, None, None)
    assert _field_to_dict(f) == {"name": "", "type": "", "comment": ""}


def test_get_table_request_cache_memoizes():
    c = _bare_client()
    c._table_cache = None
    calls = {"n": 0}

    def fake_fetch(db, table):
        calls["n"] += 1
        return _Table()

    c._fetch_table_with_fallbacks = fake_fetch

    # Without an active cache, every call hits HMS.
    c.get_table("db", "t")
    c.get_table("db", "t")
    assert calls["n"] == 2

    # Within request_cache, the same table is fetched only once.
    calls["n"] = 0
    with c.request_cache():
        a = c.get_table("db", "t")
        b = c.get_table("db", "t")
    assert calls["n"] == 1
    assert a == b

    # Cache is cleared on exit — next call fetches again.
    calls["n"] = 0
    c.get_table("db", "t")
    assert calls["n"] == 1


def test_request_cache_nesting_shares_outer_cache():
    c = _bare_client()
    c._table_cache = None
    calls = {"n": 0}

    def fake_fetch(db, table):
        calls["n"] += 1
        return _Table()

    c._fetch_table_with_fallbacks = fake_fetch

    with c.request_cache():
        c.get_table("db", "t")
        with c.request_cache():  # nested: should reuse outer cache, not reset it
            c.get_table("db", "t")
        c.get_table("db", "t")
    assert calls["n"] == 1


def test_get_partition_basic_stats_bulk_reconstructs_names():
    c = _bare_client()
    parts = [
        _Partition(["2026-05-11"], {"numRows": "100", "numFiles": "2", "totalSize": "2048"}),
        _Partition(["2026-05-12"], {"numRows": "50"}),
    ]
    c._call = lambda fn, *args: parts

    out = c.get_partition_basic_stats_bulk(
        "sales", "txns", ["sale_date=2026-05-11", "sale_date=2026-05-12"], ["sale_date"]
    )
    assert out["sale_date=2026-05-11"] == {
        "num_rows": "100",
        "num_files": "2",
        "total_size": "2048",
    }
    # Missing params default to "-1", matching get_partition_basic_stats behavior.
    assert out["sale_date=2026-05-12"] == {
        "num_rows": "50",
        "num_files": "-1",
        "total_size": "-1",
    }


def test_get_partition_basic_stats_bulk_empty_skips_call():
    c = _bare_client()
    called = {"n": 0}

    def boom(*args):
        called["n"] += 1
        raise AssertionError("should not call HMS for empty input")

    c._call = boom
    assert c.get_partition_basic_stats_bulk("d", "t", [], ["k"]) == {}
    assert called["n"] == 0


def _table_obj(name, col_names, part_key_names=()):
    class _T:
        tableName = name

        class sd:
            cols = [_Field(c, "string") for c in col_names]

        partitionKeys = [_Field(k, "string") for k in part_key_names]

    return _T()


def test_get_table_objects_bulk_single_call():
    c = _bare_client()
    calls = {"n": 0}

    def fake_call(fn, dbname, names):
        calls["n"] += 1
        assert fn == "get_table_objects_by_name"
        return [_table_obj(n, ["c1"]) for n in names]

    c._call = fake_call
    out = c.get_table_objects("db", ["a", "b", "c"])
    assert set(out) == {"a", "b", "c"}
    assert calls["n"] == 1  # one bulk round-trip, not three


def test_get_table_objects_falls_back_when_bulk_unavailable():
    c = _bare_client()

    def boom_call(fn, *args):
        raise RuntimeError("bulk API not available")

    fetched = []

    def fake_fetch(db, t):
        fetched.append(t)
        return _table_obj(t, ["c1"])

    c._call = boom_call
    c._fetch_table_with_fallbacks = fake_fetch
    out = c.get_table_objects("db", ["a", "b"])
    assert set(out) == {"a", "b"}
    assert fetched == ["a", "b"]


def test_search_tables_name_and_column_match_via_bulk():
    c = _bare_client()

    c.get_all_databases = lambda: ["sales_db"]
    c.get_all_tables = lambda db: ["sales_summary", "inventory"]

    bulk_calls = {"n": 0}

    def fake_call(fn, dbname, names):
        bulk_calls["n"] += 1
        assert fn == "get_table_objects_by_name"
        # only the non-name-matching table is prefetched
        assert names == ["inventory"]
        return [_table_obj("inventory", ["item_id", "sales_amount"])]

    c._call = fake_call

    results = c.search_tables("sales")
    assert bulk_calls["n"] == 1  # one bulk round-trip for the whole database
    assert results == [
        {"database": "sales_db", "table": "sales_summary", "match_reason": "table name"},
        {"database": "sales_db", "table": "inventory", "match_reason": "column 'sales_amount'"},
    ]


def test_search_tables_global_cap_is_30():
    """Global result cap must be exactly 30."""
    c = _bare_client()
    c.get_all_databases = lambda: ["db"]
    c.get_all_tables = lambda db: [f"match_t{i}" for i in range(50)]

    def fake_call(fn, dbname, names):
        return []  # no column matches needed — all tables match by name

    c._call = fake_call
    results = c.search_tables("match")
    assert len(results) == 30
    assert all(r["match_reason"] == "table name" for r in results)


def test_search_tables_scoped_returns_up_to_30_column_matches():
    """Scoped single-db search should not apply the per-database column cap."""
    c = _bare_client()
    c.get_all_tables = lambda db: [f"t{i}" for i in range(40)]

    def fake_call(fn, dbname, names):
        return [_table_obj(n, [f"metric_{n}"]) for n in names]

    c._call = fake_call
    results = c.search_tables("metric", database="sales_db")
    assert len(results) == 30
    assert all(r["match_reason"].startswith("column") for r in results)


def test_search_tables_unscoped_limits_column_matches_per_database():
    """Unscoped search caps column matches at 20 per db, leaving slots for other dbs."""
    c = _bare_client()
    c.get_all_databases = lambda: ["db_a", "db_b"]
    c.get_all_tables = lambda db: [f"{db}_t{i}" for i in range(25)]

    def fake_call(fn, dbname, names):
        return [_table_obj(n, ["shared_col"]) for n in names]

    c._call = fake_call
    results = c.search_tables("shared")
    db_a = [r for r in results if r["database"] == "db_a"]
    db_b = [r for r in results if r["database"] == "db_b"]
    assert len(db_a) == 20  # capped at _SEARCH_COLUMN_MATCH_PER_DB
    assert len(db_b) == 10  # remaining slots
    assert len(results) == 30
