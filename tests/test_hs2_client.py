import threading

from hivemind.hs2_client import HS2Client, _strip_trailing_semicolons


class FakeCursor:
    def __init__(self, rows=None, error=None):
        self._rows = rows or []
        self._error = error
        self.executed = []

    def execute(self, statement):
        self.executed.append(statement)
        if self._error is not None:
            raise self._error

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, rows=None, error=None):
        self._rows = rows
        self._error = error
        self.cursors = []

    def cursor(self):
        c = FakeCursor(self._rows, self._error)
        self.cursors.append(c)
        return c

    def close(self):
        pass


def _bare_hs2(conn=None):
    """Build an HS2Client without connecting; reconnect is stubbed to a no-op."""
    c = HS2Client.__new__(HS2Client)
    c._lock = threading.Lock()
    c._auth = "NONE"
    c._conn = conn
    c._connect = lambda: None  # avoid touching real PyHive during tests
    return c


def test_strip_trailing_semicolons():
    assert _strip_trailing_semicolons("SELECT 1;") == "SELECT 1"
    assert _strip_trailing_semicolons("SELECT 1 ;;  ") == "SELECT 1"
    assert _strip_trailing_semicolons("  SELECT 1  ") == "SELECT 1"


def test_is_available():
    assert _bare_hs2(conn=None).is_available() is False
    assert _bare_hs2(conn=FakeConnection(rows=[])).is_available() is True


def test_explain_joins_rows():
    conn = FakeConnection(rows=[("line1",), ("line2",), ("line3",)])
    c = _bare_hs2(conn)
    out = c.explain("SELECT * FROM sample.t")
    assert out == "line1\nline2\nline3"


def test_explain_strips_semicolons_and_wraps():
    conn = FakeConnection(rows=[("plan",)])
    c = _bare_hs2(conn)
    c.explain("SELECT 1;;  ")
    assert conn.cursors[0].executed == ["EXPLAIN SELECT 1"]


def test_explain_graceful_error():
    conn = FakeConnection(error=RuntimeError("hs2 boom"))
    c = _bare_hs2(conn)
    out = c.explain("SELECT 1")
    assert out.startswith("Error")
    assert "boom" in out


def test_explain_empty_query():
    c = _bare_hs2(FakeConnection(rows=[("x",)]))
    assert c.explain("   ").startswith("Error")


def test_explain_unavailable_when_no_conn():
    c = _bare_hs2(conn=None)
    assert c.explain("SELECT 1").startswith("Error")


def test_explain_with_row_estimates_builds_report():
    plan_rows = [
        ("                TableScan",),
        ("                  alias: sales_transactions",),
        ("                  filterExpr: (sale_date = '2026-05-11') (type: boolean)",),
        ("                  Statistics: Num rows: 500 Data size: 4500 Basic stats: COMPLETE",),
    ]
    c = _bare_hs2(FakeConnection(rows=plan_rows))
    out = c.explain_with_row_estimates("SELECT * FROM sample.sales_transactions", hms_total_rows=10000)
    assert "HS2 EXPLAIN PLAN" in out
    assert "PARTITION PRUNING" in out
    assert "Detected: yes" in out
    assert "~95.0%" in out


def test_explain_with_row_estimates_unavailable():
    c = _bare_hs2(conn=None)
    out = c.explain_with_row_estimates("SELECT 1")
    assert "unavailable" in out.lower()


def test_explain_with_row_estimates_handles_error():
    c = _bare_hs2(FakeConnection(error=RuntimeError("nope")))
    out = c.explain_with_row_estimates("SELECT 1")
    assert "HS2 EXPLAIN PLAN" in out
    assert "Error" in out


# ---------------------------------------------------------------------------
# compare_explain_plans
# ---------------------------------------------------------------------------

_FULL_SCAN_ROWS = [
    ("TableScan",),
    ("  alias: sales_transactions",),
    ("  Statistics: Num rows: 10000 Data size: 900000 Basic stats: COMPLETE",),
]

_FILTERED_ROWS = [
    ("TableScan",),
    ("  alias: sales_transactions",),
    ("  filterExpr: (sale_date = '2026-05-11') (type: boolean)",),
    ("  Statistics: Num rows: 500 Data size: 4500 Basic stats: COMPLETE",),
]


class _SequentialFakeConnection:
    """Returns a different row set on each cursor() call (simulates two EXPLAINs)."""
    def __init__(self, row_sets):
        self._sets = list(row_sets)
        self._idx = 0
        self.cursors = []

    def cursor(self):
        rows = self._sets[self._idx % len(self._sets)]
        self._idx += 1
        c = FakeCursor(rows)
        self.cursors.append(c)
        return c

    def close(self):
        pass


def test_compare_explain_plans_builds_report():
    conn = _SequentialFakeConnection([_FULL_SCAN_ROWS, _FILTERED_ROWS])
    c = _bare_hs2(conn)
    original = "SELECT * FROM sample.sales_transactions"
    optimized = "SELECT * FROM sample.sales_transactions WHERE sale_date = '2026-05-11'"
    out = c.compare_explain_plans(original, optimized, hms_total_rows=10000,
                                  table="sample.sales_transactions")
    assert "PLAN COMPARISON" in out
    assert "TableScan rows" in out
    assert "-95.0%" in out
    # Pruning should be detected in the optimized plan
    assert "activated" in out.lower()


def test_compare_explain_plans_unavailable():
    c = _bare_hs2(conn=None)
    out = c.compare_explain_plans("SELECT 1", "SELECT 1")
    assert "unavailable" in out.lower()


def test_compare_explain_plans_graceful_on_first_error():
    # Original EXPLAIN fails; optimized succeeds. Report must still be produced.
    conn = _SequentialFakeConnection([
        [],               # first EXPLAIN returns empty → triggers "Error" path
        _FILTERED_ROWS,   # second EXPLAIN succeeds
    ])
    c = _bare_hs2(conn)
    out = c.compare_explain_plans("SELECT bad FROM t", "SELECT * FROM t WHERE d='x'")
    assert "PLAN COMPARISON" in out
