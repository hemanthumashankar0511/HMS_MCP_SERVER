from hivemind.tools.explain import handle_explain_query

# Minimal assembled context with a parseable Schema line, partition key, and
# a Statistics block carrying a row count (so HMS totals flow into the report).
_CONTEXT = """\
Schema: sample.sales_transactions
Columns:
  Name Type Comment
  amount double
Partition Keys:
  Name Type Comment
  sale_date string

Statistics: sample.sales_transactions
  Rows       : 10,000
"""

_QUERY = "SELECT amount FROM sample.sales_transactions WHERE sale_date = '2026-05-11'"


class FakeHS2:
    def __init__(self, available=True):
        self._available = available
        self.calls = []

    def is_available(self):
        return self._available

    def explain_with_row_estimates(self, query, hms_total_rows=None, table=None):
        self.calls.append({"query": query, "hms_total_rows": hms_total_rows, "table": table})
        return (
            "HS2 EXPLAIN PLAN\n================\nMode: EXPLAIN\n"
            f"ROW REDUCTION ESTIMATE\nTotal rows (HMS stats): {hms_total_rows}\n"
            "Estimated rows scanned (CBO): 500\nReduction: ~95.0%"
        )


async def test_explain_includes_hs2_section():
    hs2 = FakeHS2()
    out = await handle_explain_query(_QUERY, _CONTEXT, hs2)
    assert "HS2 EXPLAIN context:" in out
    assert "HS2 EXPLAIN PLAN" in out
    assert "~95.0%" in out


async def test_explain_passes_hms_total_rows_to_hs2():
    hs2 = FakeHS2()
    await handle_explain_query(_QUERY, _CONTEXT, hs2)
    assert hs2.calls[0]["hms_total_rows"] == 10000
    assert hs2.calls[0]["table"] == "sample.sales_transactions"


async def test_explain_fallback_when_hs2_none():
    out = await handle_explain_query(_QUERY, _CONTEXT, None)
    assert "HS2 EXPLAIN context:" in out
    assert "HS2 EXPLAIN unavailable" in out


async def test_explain_fallback_when_hs2_unavailable():
    hs2 = FakeHS2(available=False)
    out = await handle_explain_query(_QUERY, _CONTEXT, hs2)
    assert "HS2 EXPLAIN unavailable" in out
    assert hs2.calls == []  # never invoked when unavailable


async def test_explain_hs2_error_degrades_gracefully():
    class BoomHS2(FakeHS2):
        def explain_with_row_estimates(self, query, hms_total_rows=None, table=None):
            raise RuntimeError("explain crashed")

    out = await handle_explain_query(_QUERY, _CONTEXT, BoomHS2())
    assert "HS2 EXPLAIN context:" in out
    assert "HS2 EXPLAIN failed" in out
    # The rest of the HMS-based prompt is still present.
    assert "Full metastore context:" in out


async def test_explain_still_works_without_hs2_arg():
    # Backward compatibility: default hs2_client=None must not break callers.
    out = await handle_explain_query(_QUERY, _CONTEXT)
    assert "QUERY EXPLANATION" in out
    assert "HS2 EXPLAIN unavailable" in out
