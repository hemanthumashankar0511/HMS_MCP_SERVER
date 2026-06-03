from hivemind.tools.explain_plan import (
    build_comparison_report,
    build_hs2_report,
    compute_row_reduction,
    format_cbo_scan_summary,
    max_hms_total_rows,
    parse_explain_plan,
    pick_focus_table,
    table_scan_rows_for,
)

# A representative Hive 3 plain EXPLAIN plan for a partition-filtered scan.
_PLAN = """\
STAGE PLANS:
  Stage: Stage-1
    Tez
      Vertices:
        Map 1
            Map Operator Tree:
                TableScan
                  alias: sales_transactions
                  filterExpr: (sale_date = '2026-05-11') (type: boolean)
                  Statistics: Num rows: 500 Data size: 4500 Basic stats: COMPLETE Column stats: NONE
                  Filter Operator
                    predicate: (sale_date = '2026-05-11') (type: boolean)
                    Statistics: Num rows: 500 Data size: 4500 Basic stats: COMPLETE
                  Select Operator
                    Statistics: Num rows: 500 Data size: 4500 Basic stats: COMPLETE
"""

# A plan with no partition filter at all (full scan of a partitioned table).
_PLAN_NO_FILTER = """\
STAGE PLANS:
  Stage: Stage-1
    Tez
      Vertices:
        Map 1
            Map Operator Tree:
                TableScan
                  alias: sales_transactions
                  Statistics: Num rows: 10000 Data size: 900000 Basic stats: COMPLETE Column stats: NONE
"""


def test_parse_extracts_rows_and_filter():
    parsed = parse_explain_plan(_PLAN)
    assert parsed["table_scans"][0]["table"] == "sales_transactions"
    assert parsed["table_scans"][0]["rows"] == 500
    assert parsed["table_scans"][0]["filter"] == "(sale_date = '2026-05-11')"
    assert parsed["cbo_est_rows"] == 500


def test_parse_detects_partition_pruning():
    parsed = parse_explain_plan(_PLAN)
    assert parsed["partition_pruning_detected"] is True
    assert "(sale_date = '2026-05-11')" in parsed["partition_filters"]


def test_parse_no_filter_means_no_pruning():
    parsed = parse_explain_plan(_PLAN_NO_FILTER)
    assert parsed["partition_pruning_detected"] is False
    assert parsed["partition_filters"] == []
    assert parsed["cbo_est_rows"] == 10000


def test_parse_explicit_partition_filters_line():
    plan = "TableScan\n  Partition Filters: sale_date=2026-05-11\n"
    parsed = parse_explain_plan(plan)
    assert parsed["partition_pruning_detected"] is True
    assert parsed["partition_filters"] == ["sale_date=2026-05-11"]


# Hive 3 CBO inline EXPLAIN (as returned by EXPLAIN on CDP clusters).
_CBO_PLAN_FULL = """\
Plan optimized by CBO.
TableScan [TS_2] (rows=287997024 width=117) tpcds_bin_partitioned_orc_100@store_sales,store_sales
TableScan [TS_0] (rows=2000000 width=288) tpcds_bin_partitioned_orc_100@customer,customer
Map Join Operator [MAPJOIN_34] (rows=137142412 width=290) PARTITION_ONLY_SHUFFLE
"""

_CBO_PLAN_FILTERED = """\
Plan optimized by CBO.
TableScan [TS_2] (rows=87120 width=114) tpcds_bin_partitioned_orc_100@store_sales,store_sales
TableScan [TS_0] (rows=2000000 width=288) tpcds_bin_partitioned_orc_100@customer,customer
Map Join Operator [MAPJOIN_33] (rows=2200000 width=288) BROADCAST
"""


def test_parse_cbo_tablescan_format():
    parsed = parse_explain_plan(_CBO_PLAN_FULL)
    assert len(parsed["table_scans"]) == 2
    store = next(ts for ts in parsed["table_scans"] if ts["table"] == "store_sales")
    assert store["rows"] == 287997024
    assert parsed["cbo_est_rows"] == 287997024  # dominant scan, not customer 2M
    assert parsed["join_strategy"] == "Shuffle / map join"


def test_parse_cbo_filtered_plan():
    parsed = parse_explain_plan(_CBO_PLAN_FILTERED)
    store = next(ts for ts in parsed["table_scans"] if ts["table"] == "store_sales")
    assert store["rows"] == 87120
    assert parsed["join_strategy"] == "Broadcast join"


def test_table_scan_rows_for_focus_table():
    parsed = parse_explain_plan(_CBO_PLAN_FULL)
    assert table_scan_rows_for(parsed, "tpcds_bin_partitioned_orc_100.store_sales") == 287997024


def test_pick_focus_table_prefers_partitioned():
    tables = ["db.customer", "db.store_sales"]
    pkeys = {"db.customer": [], "db.store_sales": ["ss_sold_date_sk"]}
    assert pick_focus_table(tables, pkeys) == "db.store_sales"


def test_parse_empty_plan():
    parsed = parse_explain_plan("")
    assert parsed["operators"] == []
    assert parsed["cbo_est_rows"] is None
    assert parsed["partition_pruning_detected"] is False


# ---------------------------------------------------------------------------
# Metastore-level partition pruning detection (inferred from absent predicates)
# ---------------------------------------------------------------------------

# Hive 3 plan for an insert-only Parquet table where partition predicates were
# applied at file-listing stage and stripped from the runtime FilterOperator.
# This is the exact shape produced for sample.iot_telemetry when ANALYZE TABLE
# has not been run.
_IOT_PLAN = """\
Plan optimized by CBO.

Stage-0
  Fetch Operator
    limit:100
    Select Operator [SEL_2]
      Output:["_col0","_col1","_col2"]
      Limit [LIM_3]
        Number of rows:100
        Filter Operator [FIL_5]
          predicate:((battery_level < 25.0D) and (temperature_celsius > 70.0D))
          TableScan [TS_0]
            Output:["device_id","firmware_version","temperature_celsius","battery_level","recorded_ts"]
"""


def test_inferred_partition_pruning_when_pkeys_absent_from_filter():
    """
    When partition keys are provided but absent from every runtime predicate,
    the parser should infer metastore-level partition pruning rather than
    reporting a false-negative 'no pruning detected'.
    """
    parsed = parse_explain_plan(_IOT_PLAN, partition_keys=["location_zone", "reading_date"])
    assert parsed["partition_pruning_detected"] is True
    assert any("inferred" in f for f in parsed["partition_filters"])


def test_no_inferred_pruning_without_pkeys():
    """Without partition_keys provided, the heuristic must not fire."""
    parsed = parse_explain_plan(_IOT_PLAN)
    assert parsed["partition_pruning_detected"] is False


def test_no_inferred_pruning_when_no_runtime_filter():
    """Heuristic must not fire when there are no runtime filter predicates at all."""
    plan = """\
Plan optimized by CBO.

Stage-0
  Fetch Operator
    TableScan [TS_0]
      Output:["device_id"]
"""
    parsed = parse_explain_plan(plan, partition_keys=["location_zone"])
    assert parsed["partition_pruning_detected"] is False


def test_no_inferred_pruning_when_pkey_present_in_filter():
    """If a partition key IS in the runtime filter, existing logic applies — no double-detection."""
    plan = """\
TableScan
  alias: iot_telemetry
  filterExpr: (location_zone = 'North_Sector') (type: boolean)
  Statistics: Num rows: 100 Data size: 900 Basic stats: COMPLETE
"""
    parsed = parse_explain_plan(plan, partition_keys=["location_zone", "reading_date"])
    assert parsed["partition_pruning_detected"] is True
    # Inferred note should NOT be added when the key is present in the explicit filter
    assert not any("inferred" in f for f in parsed["partition_filters"])


def test_compute_row_reduction_math():
    out = compute_row_reduction(10000, 500)
    assert "Total rows (HMS stats): 10,000" in out
    assert "Estimated rows scanned (CBO): 500" in out
    assert "~95.0%" in out


def test_compute_row_reduction_unknown_cbo():
    out = compute_row_reduction(10000, None)
    assert "CBO row estimate unavailable" in out


def test_compute_row_reduction_unknown_hms():
    out = compute_row_reduction(None, 500)
    assert "Estimated rows scanned (CBO): 500" in out
    assert "unavailable" in out


def test_compute_row_reduction_no_effective_pruning():
    out = compute_row_reduction(500, 500)
    assert "~0%" in out


def test_max_hms_total_rows_picks_largest():
    assert max_hms_total_rows({"a": "100", "b": "1,000", "c": "-1"}) == 1000


def test_max_hms_total_rows_none_when_no_usable():
    assert max_hms_total_rows({"a": "-1", "b": "unknown"}) is None
    assert max_hms_total_rows({}) is None


def test_format_cbo_scan_summary():
    parsed = parse_explain_plan(_CBO_PLAN_FULL)
    summary = format_cbo_scan_summary(parsed, "tpcds_bin_partitioned_orc_100.store_sales")
    assert "287,997,024" in summary
    assert "store_sales" in summary
    assert "focus table" in summary
    assert "Partition pruning: no" in summary


def test_build_report_compact_omits_raw_plan_and_reduction():
    parsed = parse_explain_plan(_PLAN)
    report = build_hs2_report(_PLAN, parsed, compact=True)
    assert "CBO SCAN ESTIMATES" in report
    assert "RAW PLAN" not in report
    assert "ROW REDUCTION ESTIMATE" not in report


def test_build_report_includes_all_sections():
    parsed = parse_explain_plan(_PLAN)
    report = build_hs2_report(_PLAN, parsed, hms_total_rows=10000, table="sample.sales_transactions")
    assert "HS2 EXPLAIN PLAN" in report
    assert "PARSED ROW ESTIMATES" in report
    assert "PARTITION PRUNING" in report
    assert "Detected: yes" in report
    assert "ROW REDUCTION ESTIMATE" in report
    assert "Table: sample.sales_transactions" in report
    assert "~95.0%" in report


# ---------------------------------------------------------------------------
# build_comparison_report tests
# ---------------------------------------------------------------------------

def test_comparison_report_shows_all_sections():
    before = parse_explain_plan(_PLAN_NO_FILTER)
    after = parse_explain_plan(_PLAN)
    report = build_comparison_report(
        original_parsed=before,
        optimized_parsed=after,
        hms_total_rows=10000,
        table="sample.sales_transactions",
        partition_keys=["sale_date"],
    )
    assert "PLAN COMPARISON" in report
    assert "Focus table" in report
    assert "TableScan rows" in report
    assert "VERDICT" in report
    assert "ORIGINAL PLAN" not in report  # raw dumps removed


def test_comparison_cbo_store_sales_focus():
    before = parse_explain_plan(_CBO_PLAN_FULL)
    after = parse_explain_plan(_CBO_PLAN_FILTERED)
    report = build_comparison_report(
        before,
        after,
        hms_total_rows=287_997_024,
        table="tpcds_bin_partitioned_orc_100.store_sales",
        partition_keys=["ss_sold_date_sk"],
    )
    assert "287,997,024" in report
    assert "87,120" in report
    assert "-100.0" in report or "-99.9" in report
    assert "store_sales" in report
    assert "Partition pruning" in report
    assert "Broadcast join" in report


def test_comparison_report_row_delta():
    before = parse_explain_plan(_PLAN_NO_FILTER)   # cbo_est_rows = 10,000
    after  = parse_explain_plan(_PLAN)             # cbo_est_rows = 500
    report = build_comparison_report(before, after, hms_total_rows=10000)
    # The delta line shows -95.0%
    assert "-95.0%" in report


def test_comparison_verdict_pruning_activated():
    before = parse_explain_plan(_PLAN_NO_FILTER)
    after  = parse_explain_plan(_PLAN)
    report = build_comparison_report(before, after)
    assert "activated" in report.lower()


def test_comparison_verdict_no_change():
    parsed = parse_explain_plan(_PLAN)
    report = build_comparison_report(parsed, parsed)
    assert "same number of rows" in report


def test_comparison_report_unknown_rows():
    # Neither plan has CBO estimates — report must not fabricate numbers.
    empty = parse_explain_plan("")
    report = build_comparison_report(empty, empty)
    assert "could not be parsed" in report.lower()


def test_comparison_hms_total_shows_vs_savings():
    before = parse_explain_plan(_PLAN_NO_FILTER)   # 10,000 scanned
    after  = parse_explain_plan(_PLAN)             # 500 scanned
    report = build_comparison_report(before, after, hms_total_rows=10000)
    # HMS total row context should appear
    assert "HMS total rows" in report
    # After plan scans 5% of total → 95% saved
    assert "95.0%" in report
