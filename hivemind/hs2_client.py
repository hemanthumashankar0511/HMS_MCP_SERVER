from __future__ import annotations

import logging
import re
import threading
import time
from typing import Any

from hivemind.tools.explain_plan import (
    build_comparison_report,
    build_hs2_report,
    parse_explain_plan,
)

logger = logging.getLogger(__name__)

_TRAILING_SEMICOLONS_RE = re.compile(r"[\s;]+$")

_UNAVAILABLE_MSG = (
    "HS2 EXPLAIN unavailable — HiveServer2 is not configured or the connection "
    "could not be established. Analysis is based on HMS metadata only."
)


def _strip_trailing_semicolons(query: str) -> str:
    """EXPLAIN must wrap a bare statement — a trailing ';' inside breaks the prefix."""
    return _TRAILING_SEMICOLONS_RE.sub("", query.strip())


class HS2Client:
    """
    Thin PyHive wrapper for EXPLAIN-only HiveServer2 (HS2) operations.

    This client never executes data-returning queries. It only issues
    EXPLAIN statements, which produce plan text and optimizer row estimates
    without reading table data. It is an optional enrichment layer on top
    of the HMS discovery tools: if HS2 is unreachable the server runs HMS-only.

    Access is guarded by a lock so the shared PyHive connection cannot be corrupted
    by concurrent tool invocations, mirroring HMSClient's threading model.
    """

    def __init__(
        self,
        host: str,
        port: int = 10000,
        user: str | None = None,
        password: str | None = None,
        database: str = "default",
        auth: str = "NONE",
        timeout_s: int = 60,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user or None
        self._password = password
        self._database = database or "default"
        self._auth = (auth or "NONE").upper()
        self._timeout_s = timeout_s
        self._conn: Any = None
        self._lock = threading.Lock()
        self._connect()

    def _connect(self) -> None:
        try:
            from pyhive import hive  # noqa: PLC0415
        except ImportError as exc:
            raise RuntimeError(
                "PyHive is not installed. Install it with `pip install 'pyhive[hive]'` "
                "to enable HS2 EXPLAIN features (see README)."
            ) from exc

        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001 - best-effort close before reconnect
                pass
            self._conn = None

        # For NONE auth PyHive requires password to be None; LDAP/CUSTOM use it.
        password = None if self._auth == "NONE" else (self._password or None)
        self._conn = hive.Connection(
            host=self._host,
            port=self._port,
            username=self._user,
            password=password,
            database=self._database,
            auth=self._auth,
        )
        logger.info(
            "Connected to HS2 at %s:%d (database=%s, auth=%s)",
            self._host,
            self._port,
            self._database,
            self._auth,
        )

    def is_available(self) -> bool:
        return self._conn is not None

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001 - best-effort close on shutdown
                pass
            self._conn = None

    def _execute_fetch(self, statement: str) -> list[tuple]:
        """
        Run a statement and return all rows, reconnecting once on transport failure.

        Mirrors HMSClient._call: a single transparent reconnect handles dropped
        idle connections without surfacing a transient error to the caller.
        """
        with self._lock:
            cursor = None
            try:
                cursor = self._conn.cursor()
                cursor.execute(statement)
                return cursor.fetchall()
            except Exception:  # noqa: BLE001 - retry once on any execute/transport error
                logger.warning("HS2 statement failed — reconnecting and retrying once")
                self._connect()
                cursor = self._conn.cursor()
                cursor.execute(statement)
                return cursor.fetchall()
            finally:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:  # noqa: BLE001
                        pass

    def explain(self, query: str) -> str:
        """
        Run EXPLAIN against HS2 and return the plan text as a single string.

        Trailing semicolons are stripped before wrapping. HS2 errors are returned as
        an "Error: ..." string rather than raised, so a plan failure never crashes
        the server.
        """
        if not self.is_available():
            return "Error: HS2 is not available."

        bare = _strip_trailing_semicolons(query)
        if not bare:
            return "Error: empty query."

        statement = f"EXPLAIN {bare}"
        try:
            rows = self._execute_fetch(statement)
            text = "\n".join(
                str(r[0]) if len(r) == 1 else "\t".join(str(c) for c in r)
                for r in rows
            )
            if text.strip():
                return text
            return "Error: EXPLAIN returned an empty plan"
        except Exception as exc:  # noqa: BLE001 - graceful: never crash the server
            logger.warning("EXPLAIN failed: %s", exc)
            return f"Error: {exc}"

    def compare_explain_plans(
        self,
        original_query: str,
        optimized_query: str,
        hms_total_rows: int | None = None,
        table: str | None = None,
        partition_keys: list[str] | None = None,
        partition_sample_rows: int | None = None,
    ) -> str:
        """
        Run EXPLAIN on two versions of a query and return a structured comparison.

        The report shows, side-by-side:
          - Optimizer row estimates before and after, with the percentage delta
          - Whether partition pruning is active in each plan
          - How much of the HMS total is scanned in each version
          - A plain-English verdict (which plan is better and why)

        partition_sample_rows: HMS partition sample row count for the partition
        matched by the optimized query's filter.  Supplied by the caller when
        the optimized plan is expected to be a pure-fetch shape (which emits no
        CBO row estimate), so the comparison table shows real numbers.
        """
        if not self.is_available():
            return _UNAVAILABLE_MSG

        start = time.perf_counter()

        orig_plan = self.explain(original_query)
        opt_plan = self.explain(optimized_query)

        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info("compare_explain_plans HS2 EXPLAIN pair took %.0fms", elapsed_ms)

        if orig_plan.startswith("Error:"):
            orig_plan = f"[EXPLAIN failed: {orig_plan}]"
        if opt_plan.startswith("Error:"):
            opt_plan = f"[EXPLAIN failed: {opt_plan}]"

        orig_parsed = parse_explain_plan(orig_plan, partition_keys=partition_keys)
        opt_parsed = parse_explain_plan(opt_plan, partition_keys=partition_keys)

        return build_comparison_report(
            original_parsed=orig_parsed,
            optimized_parsed=opt_parsed,
            hms_total_rows=hms_total_rows,
            table=table,
            partition_keys=partition_keys,
            partition_sample_rows=partition_sample_rows,
        )

    def explain_with_row_estimates(
        self,
        query: str,
        hms_total_rows: int | None = None,
        table: str | None = None,
        compact: bool = False,
        partition_keys: list[str] | None = None,
    ) -> str:
        """
        Produce a structured HS2 EXPLAIN report with parsed row estimates.

        Runs EXPLAIN, which on Hive 3 carries optimizer Statistics ('Num rows')
        and TableScan filterExpr lines — the evidence needed to verify partition
        pruning and estimate rows scanned.

        When hms_total_rows is supplied and compact=False, a row reduction
        percentage (HMS total vs scan estimate) is included.

        partition_keys, when provided, enables metastore-level partition pruning
        detection: Hive strips partition predicates from the runtime plan when it
        resolves them during file-listing, so their absence from the FilterOperator
        is used as evidence that pruning was applied.
        """
        if not self.is_available():
            return _UNAVAILABLE_MSG

        start = time.perf_counter()
        plan = self.explain(query)
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info("explain_with_row_estimates HS2 EXPLAIN took %.0fms", elapsed_ms)

        if plan.startswith("Error:"):
            return (
                "HS2 EXPLAIN PLAN\n"
                "================\n"
                f"{plan}\n"
                "Analysis falls back to HMS metadata only."
            )

        parsed = parse_explain_plan(plan, partition_keys=partition_keys)
        return build_hs2_report(
            plan_text=plan,
            parsed=parsed,
            hms_total_rows=hms_total_rows,
            table=table,
            compact=compact,
        )
