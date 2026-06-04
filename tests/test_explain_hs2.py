from hivemind.tools.explain import handle_explain_query

# Minimal assembled context with a parseable Schema line and a partition key.
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

_RAW_PLAN = (
    "Plan optimized by CBO.\n"
    "Stage-0\n"
    "  Fetch Operator\n"
    "    Select Operator [SEL_2]\n"
    "      TableScan [TS_0]\n"
)


class FakeHS2:
    """Stub HS2 client: explain() returns a canned raw plan and records calls."""

    def __init__(self, available=True, plan=_RAW_PLAN):
        self._available = available
        self._plan = plan
        self.calls = []

    def is_available(self):
        return self._available

    def explain(self, query):
        self.calls.append(query)
        return self._plan


async def test_explain_includes_raw_plan_as_reference():
    hs2 = FakeHS2()
    out = await handle_explain_query(_QUERY, _CONTEXT, hs2)
    assert "HS2 EXPLAIN context" in out
    assert "reference only" in out
    assert "TableScan [TS_0]" in out  # raw plan text passed through verbatim
    assert hs2.calls == [_QUERY]


async def test_explain_prompt_warns_against_plan_based_pruning():
    # The guard note that prevents the false "no pruning" verdict must be present.
    out = await handle_explain_query(_QUERY, _CONTEXT, FakeHS2())
    assert "stripped from the runtime plan" in out
    assert "judge pruning from the SQL" in out


async def test_explain_does_not_emit_cbo_output_sections():
    # The CBO-driven output sections must be gone from explain. (The prompt still
    # mentions CBO inside a rule that forbids quoting it — that's expected.)
    out = await handle_explain_query(_QUERY, _CONTEXT, FakeHS2())
    assert "HS2 PLAN ANALYSIS" not in out
    assert "ROW REDUCTION ESTIMATE" not in out
    assert "Partition pruning confirmed" not in out


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
        def explain(self, query):
            raise RuntimeError("explain crashed")

    out = await handle_explain_query(_QUERY, _CONTEXT, BoomHS2())
    assert "HS2 EXPLAIN failed" in out
    # The rest of the HMS-based prompt is still present.
    assert "Full metastore context:" in out


async def test_explain_hs2_plan_error_string_degrades_gracefully():
    # When explain() returns an "Error: ..." string, it is shown with the fallback note.
    out = await handle_explain_query(_QUERY, _CONTEXT, FakeHS2(plan="Error: boom"))
    assert "Error: boom" in out
    assert "HS2 EXPLAIN unavailable" in out


async def test_explain_still_works_without_hs2_arg():
    # Backward compatibility: default hs2_client=None must not break callers.
    out = await handle_explain_query(_QUERY, _CONTEXT)
    assert "QUERY EXPLANATION" in out
    assert "HS2 EXPLAIN unavailable" in out
