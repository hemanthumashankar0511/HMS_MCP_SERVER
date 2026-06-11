from __future__ import annotations

import logging
import re
import socket
import ssl
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TSSLSocket, TTransport
from thrift.transport.TTransport import TTransportException
from thrift.Thrift import TApplicationException

logger = logging.getLogger(__name__)


def _service_from_principal(principal: str, default: str = "hive") -> str:
    """
    Extract the SASL/GSSAPI service (primary) component from a Kerberos principal.

    A service principal looks like ``hive/host.fqdn@REALM`` (or ``hive@REALM``);
    the primary is everything before the first ``/`` (or ``@``). When no principal
    is supplied we fall back to the explicitly configured service name (usually
    ``hive``), which is what the HS2/HMS SASL transport mechanism expects.
    """
    principal = (principal or "").strip()
    if "/" in principal:
        return principal.split("/", 1)[0]
    if "@" in principal:
        return principal.split("@", 1)[0]
    return default or "hive"


def _canonical_fqdn(host: str) -> str:
    """
    Resolve host to its canonical fully-qualified domain name for Kerberos.

    GSSAPI builds the service ticket from ``service@FQDN``; an IP address or a
    short hostname makes the KDC reject the request, so we canonicalize via DNS.
    Falls back to the original value if resolution yields nothing useful.
    """
    try:
        fqdn = socket.getfqdn(host)
    except Exception:  # noqa: BLE001 - resolution is best-effort
        return host
    return fqdn or host


def _insecure_ssl_context() -> ssl.SSLContext:
    """
    Build a permissive TLS context for internal Auto-TLS endpoints.

    Auto-TLS clusters present internal/self-signed certificates that chain to a
    private CA, so chain and hostname verification are disabled here. This is the
    intent of Thrift's deprecated ``validate=False`` flag, but it is applied via an
    explicit context because modern Thrift evaluates ``validate=`` against a
    ``PROTOCOL_TLS_CLIENT`` default (verify_mode=CERT_REQUIRED) and rejects the
    socket before the relaxed setting is applied. The wire is still TLS-encrypted;
    only peer-certificate validation is relaxed (suitable for internal/test use).
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _make_pure_sasl_client(host: str, service: str) -> Any:
    """
    Build a corrected pure-Python SASL/GSSAPI client for thrift_sasl.

    The pure-Python fallback (``pure-sasl`` via PyHive's ``PureSASLClient`` shim)
    is used when the C ``sasl`` extension is not installed. That shim has a known
    bug: its ``encode``/``decode`` methods are inverted and omit the frame length
    header. thrift_sasl's contract is:

        encode(outgoing)        -> GSS-wrap the bytes, prefix a 4-byte BE length
        decode(header + body)   -> strip the 4-byte length, GSS-unwrap the body

    With QOP=auth this is a no-op (wrap/unwrap return the input unchanged), so the
    shim's bug is invisible. But on a cluster that negotiates auth-int/auth-conf
    (``hadoop.rpc.protection`` = integrity/privacy) the broken shim calls GSS
    *unwrap* on outgoing plaintext, producing GSS_S_DEFECTIVE_TOKEN
    ("A token was invalid") on the first RPC after a successful handshake.

    This subclass restores the correct contract so GSSAPI works regardless of the
    negotiated QOP.
    """
    import struct  # noqa: PLC0415

    from pyhive.sasl_compat import PureSASLClient  # noqa: PLC0415

    class _CorrectedPureSASLClient(PureSASLClient):  # type: ignore[misc, valid-type]
        def encode(self, outgoing: Any) -> tuple[bool, Any]:
            try:
                self.error = None
                wrapped = self.wrap(outgoing)
                # QOP=auth: wrap() returns the input unchanged and thrift_sasl
                # sends it without a length header (it detects the no-op by the
                # length being unchanged), so pass it straight through.
                if wrapped is outgoing or len(wrapped) == len(outgoing):
                    return True, wrapped
                return True, struct.pack(">I", len(wrapped)) + wrapped
            except Exception as exc:  # noqa: BLE001 - surfaced via getError()
                self.error = str(exc)
                return False, None

        def decode(self, incoming: Any) -> tuple[bool, Any]:
            try:
                self.error = None
                # thrift_sasl hands us the 4-byte length header + body for
                # encoded frames; strip it before unwrapping. For QOP=auth the
                # unwrap is a no-op and no header is present.
                body = incoming[4:] if len(incoming) > 4 else incoming
                return True, self.unwrap(body)
            except Exception as exc:  # noqa: BLE001 - surfaced via getError()
                self.error = str(exc)
                return False, None

    return _CorrectedPureSASLClient(host=host, service=service)


def _make_gssapi_sasl_client(host: str, service: str) -> Any:
    """
    Build a SASL client for GSSAPI that satisfies thrift_sasl's factory protocol.

    thrift_sasl expects an object exposing start/step/encode/decode/getError.
    The C-extension ``sasl`` (python-sasl) provides this natively; when only the
    pure-Python ``pure-sasl`` backend is installed we use a corrected wrapper
    around PyHive's PureSASLClient shim (see _make_pure_sasl_client for why the
    stock shim is broken for QOP=auth-int/auth-conf). GSSAPI itself is provided by
    the OS-level ``gssapi``/``kerberos`` module that the SASL backend loads.
    """
    try:
        import sasl  # noqa: PLC0415 - optional C SASL backend

        client = sasl.Client()
        client.setAttr("host", host)
        client.setAttr("service", service)
        client.init()
        return client
    except ImportError:
        return _make_pure_sasl_client(host, service)

_GEN = Path(__file__).resolve().parent.parent / "gen-py"
if _GEN.is_dir() and str(_GEN) not in sys.path:
    sys.path.insert(0, str(_GEN))

_REDACT_EXACT: frozenset[str] = frozenset({
    "fs.s3.awsAccessKeyId",
    "fs.s3.awsSecretAccessKey",
    "fs.azure.account.key",
    "google.cloud.auth.service.account.json.keyfile",
})

_REDACT_PATTERNS: tuple[re.Pattern, ...] = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (r"key", r"secret", r"password", r"token", r"credential", r"access")
)

# Canonical mapping from Hadoop I/O class names to human-readable storage format names.
# Used by both hms_client and discovery to avoid duplication.
_FORMAT_ALIASES: dict[str, str] = {
    "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat": "ORC",
    "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat": "ORC",
    "org.apache.hadoop.mapred.TextInputFormat": "TextFile",
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat": "Parquet",
    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat": "Parquet",
    "org.apache.hadoop.mapred.SequenceFileInputFormat": "SequenceFile",
    "org.apache.hadoop.mapred.SequenceFileOutputFormat": "SequenceFile",
    "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat": "Avro",
    "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat": "Avro",
    "org.apache.hadoop.hive.ql.io.RCFileInputFormat": "RCFile",
    "org.apache.hadoop.hive.ql.io.RCFileOutputFormat": "RCFile",
}

# Global cap on results returned by search_tables.
_SEARCH_RESULTS_CAP = 30

# Per-database column-match cap for unscoped (multi-db) search only.
# Prevents one large database from filling all 30 slots via column matches
# alone, leaving room for other databases. Scoped single-db searches use
# the full _SEARCH_RESULTS_CAP instead.
_SEARCH_COLUMN_MATCH_PER_DB = 20

# Hard ceiling of the HMS get_partition_names Thrift signature (i16 max_parts).
_HMS_MAX_PARTS_I16 = 32767

# Partitions enumerated to derive accurate table-level totals. Set to the HMS i16
# max so every partition is included in rollup sums; the display layer shows only a
# small representative sample rather than dumping the full list to the LLM.
PARTITION_ROLLUP_CAP = _HMS_MAX_PARTS_I16

# Partitions shown individually in LLM-facing output (head + tail sampling).
# Separate from PARTITION_ROLLUP_CAP: rollup always scans all partitions for
# accurate totals, while sample controls how many lines appear in the response.
PARTITION_SAMPLE_CAP = 30

# Batch size for the rollup scan (get_partitions_by_names per chunk).
_PARTITION_ROLLUP_BATCH = 500


def _friendly_format(class_name: str) -> str:
    return _FORMAT_ALIASES.get(class_name, class_name.split(".")[-1] if class_name else "Unknown")


def _sanitize_params(params: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in params.items():
        if k in _REDACT_EXACT or any(p.search(k) for p in _REDACT_PATTERNS):
            out[k] = "[REDACTED]"
        else:
            out[k] = v
    return out


def _field_to_dict(f: Any) -> dict[str, str]:
    return {
        "name": f.name or "",
        "type": f.type or "",
        "comment": f.comment or "",
    }


class HMSClient:
    """Thin Thrift wrapper for HMS discovery queries. Read-only, auto-reconnects once on failure."""

    def __init__(
        self,
        host: str,
        port: int = 9083,
        timeout_ms: int = 10_000,
        *,
        use_tls: bool = False,
        use_kerberos: bool = False,
        kerberos_principal: str = "",
        kerberos_service_name: str = "hive",
    ) -> None:
        self._host = host
        self._port = port
        self._timeout_ms = timeout_ms
        # Enterprise security layers. Both are independent: TLS secures the
        # transport, Kerberos authenticates the session. Either, both, or neither
        # may be enabled; when both are off the client uses a plain text socket.
        self._use_tls = use_tls
        self._use_kerberos = use_kerberos
        self._kerberos_principal = (kerberos_principal or "").strip()
        # Principal primary (e.g. "hive") wins; otherwise the configured service name.
        self._kerberos_service = _service_from_principal(
            self._kerberos_principal, kerberos_service_name or "hive"
        )
        self._transport: TTransport.TTransportBase | None = None
        self._client: Any = None
        # Guards transport access so the shared Thrift client cannot be corrupted
        # by concurrent use. Non-reentrant is safe: _connect never calls _call.
        self._lock = threading.Lock()
        # Request-scoped memoization for get_table. None means caching is off;
        # request_cache() turns it on for the duration of a single tool invocation
        # so the same table is fetched from HMS only once. No cross-request staleness.
        self._table_cache: dict[tuple[str, str], dict[str, Any]] | None = None
        self._connect()

    @contextmanager
    def request_cache(self) -> Iterator[None]:
        """
        Enable get_table memoization for the duration of the block.

        Scoped to a single tool invocation: redundant get_table calls within the
        same request (e.g. schema + partitions + stats for one table) hit the cache
        instead of the network. Nested uses share the outermost cache, and the cache
        is always cleared on exit, so no result is ever reused across requests.
        """
        created = self._table_cache is None
        if created:
            self._table_cache = {}
        try:
            yield
        finally:
            if created:
                self._table_cache = None

    def _connect(self) -> None:
        try:
            from hive_metastore import ThriftHiveMetastore  # noqa: PLC0415
        except ImportError as exc:
            raise RuntimeError(
                "Thrift bindings not found. Generate them from hive_metastore.thrift "
                f"and place under {_GEN}/hive_metastore/ (see README)."
            ) from exc

        if self._transport and self._transport.isOpen():
            try:
                self._transport.close()
            except TTransportException:
                pass

        # Layer 1 — raw socket. TSSLSocket for Auto-TLS clusters, plain TSocket
        # otherwise. The permissive context tolerates internal self-signed certs
        # (the validate=False intent) while keeping the wire encrypted.
        if self._use_tls:
            sock = TSSLSocket.TSSLSocket(
                self._host, self._port, ssl_context=_insecure_ssl_context()
            )
        else:
            sock = TSocket.TSocket(self._host, self._port)
        sock.setTimeout(self._timeout_ms)

        # Layer 2 — SASL/GSSAPI wrapper for AD Kerberos, else a plain buffered
        # transport. The SASL transport is itself framed/buffered, so it is the
        # top transport when Kerberos is on (no extra TBufferedTransport needed).
        if self._use_kerberos:
            self._transport = self._kerberos_transport(sock)
        else:
            self._transport = TTransport.TBufferedTransport(sock)

        # Layer 3 — binary protocol over whichever transport stack we built.
        protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = ThriftHiveMetastore.Client(protocol)
        self._transport.open()
        logger.info(
            "Connected to HMS at %s:%d (tls=%s, kerberos=%s)",
            self._host,
            self._port,
            self._use_tls,
            self._use_kerberos,
        )

    def _kerberos_transport(self, sock: Any) -> Any:
        """
        Wrap a socket in a thrift_sasl GSSAPI transport for AD Kerberos.

        The SASL service is the principal primary (usually ``hive``) and the SASL
        host MUST be the canonical FQDN — Kerberos ticket generation fails on an
        IP or short name. A valid TGT (via ``kinit`` or a keytab) must already be
        present in the credential cache; the handshake happens on transport open.
        """
        try:
            import thrift_sasl  # noqa: PLC0415
        except ImportError as exc:
            raise RuntimeError(
                "Kerberos transport requires 'thrift_sasl' plus a SASL backend "
                "('pure-sasl' or 'sasl') and an OS GSSAPI module ('gssapi'/'kerberos'). "
                "Install the kerberos extras (see README)."
            ) from exc

        fqdn = _canonical_fqdn(self._host)
        service = self._kerberos_service

        def sasl_factory() -> Any:
            return _make_gssapi_sasl_client(host=fqdn, service=service)

        logger.info(
            "Wrapping HMS transport in SASL/GSSAPI (service=%s, host=%s)", service, fqdn
        )
        return thrift_sasl.TSaslClientTransport(sasl_factory, "GSSAPI", sock)

    def _call(self, fn_name: str, *args: Any) -> Any:
        """Calls a Thrift method, retrying once on transport failure."""
        with self._lock:
            try:
                return getattr(self._client, fn_name)(*args)
            except TTransportException:
                logger.warning("Transport error on %s - reconnecting", fn_name)
                self._connect()
                return getattr(self._client, fn_name)(*args)

    def close(self) -> None:
        if self._transport and self._transport.isOpen():
            self._transport.close()

    def get_all_databases(self) -> list[str]:
        return sorted(self._call("get_all_databases"))

    def get_all_tables(self, database: str) -> list[str]:
        return sorted(self._call("get_all_tables", database))

    def _fetch_table_with_fallbacks(self, db: str, table: str) -> Any:
        """
        Fetch the raw Thrift Table object using the most capable API available.

        Tries three tiers in order:
          1. get_table_req with INSERT_ONLY_TABLES capability (HMS 3.0+, full ACID support)
          2. get_table_req without capabilities (some HMS 3.x configurations)
          3. get_table simple API (HMS 2.x and earlier)

        Raises the original exception from tier 1 if all fallbacks fail, to preserve
        the most informative diagnostic.
        """
        from hive_metastore import ttypes  # noqa: PLC0415

        # Tier 1: capability-aware request (HMS 3.0+)
        try:
            req = ttypes.GetTableRequest(
                dbName=db,
                tblName=table,
                capabilities=ttypes.ClientCapabilities(
                    values=[ttypes.ClientCapability.INSERT_ONLY_TABLES]
                ),
            )
            result = self._call("get_table_req", req)
            return result.table
        except (AttributeError, TApplicationException):
            pass  # intentional fallback chain

        # Tier 2: request without capabilities (some HMS 3.x configs)
        try:
            req = ttypes.GetTableRequest(dbName=db, tblName=table)
            result = self._call("get_table_req", req)
            return result.table
        except (AttributeError, TApplicationException):
            pass

        # Tier 3: legacy simple API (HMS 2.x)
        return self._call("get_table", db, table)

    def get_table(self, database: str, table: str) -> dict[str, Any]:
        """
        Fetch normalized table metadata from HMS.

        Uses a 3-tier HMS version-compatibility fallback: capability-aware get_table_req
        (HMS 3.0+), plain get_table_req (some 3.x configs), then the legacy get_table
        API (HMS 2.x). Returns a dict with columns, partition_keys, storage info, and
        sanitized table properties (credentials are redacted).

        When request_cache() is active, the result is memoized per (database, table)
        so repeated lookups within one tool invocation avoid extra HMS round-trips.
        """
        cache_key = (database, table)
        if self._table_cache is not None and cache_key in self._table_cache:
            return self._table_cache[cache_key]

        tbl = self._fetch_table_with_fallbacks(database, table)
        sd = tbl.sd
        raw_params: dict[str, str] = dict(tbl.parameters or {})
        clean_params = _sanitize_params(raw_params)

        location = ""
        input_format = ""
        output_format = ""
        serde = ""
        if sd:
            location = sd.location or ""
            input_format = sd.inputFormat or ""
            output_format = sd.outputFormat or ""
            if sd.serdeInfo:
                serde = sd.serdeInfo.serializationLib or ""

        cols = [_field_to_dict(c) for c in (sd.cols if sd else [])]
        part_keys = [_field_to_dict(k) for k in (tbl.partitionKeys or [])]

        info = {
            "name": tbl.tableName or "",
            "database": tbl.dbName or "",
            "table_type": tbl.tableType or "",
            "columns": cols,
            "partition_keys": part_keys,
            "parameters": clean_params,
            "location": location,
            "input_format": input_format,
            "output_format": output_format,
            "serde": serde,
            "num_files": raw_params.get("numFiles", "-1"),
            "num_rows": raw_params.get("numRows", "-1"),
            "total_size": raw_params.get("totalSize", "-1"),
        }
        if self._table_cache is not None:
            self._table_cache[cache_key] = info
        return info

    def get_partition_names(
        self, database: str, table: str, max_parts: int = PARTITION_ROLLUP_CAP
    ) -> list[str]:
        """
        Return up to max_parts partition name strings for the given table.

        Partition names are returned in HMS-native format (e.g. 'year=2024/month=01').
        The cap defaults to PARTITION_ROLLUP_CAP (HMS i16 max) so callers get the full
        partition list for accurate rollup totals; the display layer applies PARTITION_SAMPLE_CAP
        to show a representative head/tail sample rather than all names.
        """
        cap = max(1, min(int(max_parts), PARTITION_ROLLUP_CAP))
        return self._call("get_partition_names", database, table, cap)

    def get_partition_basic_stats(self, database: str, table: str, partition_name: str) -> dict[str, str]:
        """
        BASIC_STATS persisted on a partition row in HMS (same numRows/totalSize/numFiles keys as table level).

        Use when partition-level ANALYZE was run without table rollup, or alongside table-level totals.
        """
        part = self._call("get_partition_by_name", database, table, partition_name)
        params = dict(part.parameters or {})
        return {
            "num_rows": params.get("numRows", "-1"),
            "num_files": params.get("numFiles", "-1"),
            "total_size": params.get("totalSize", "-1"),
        }

    def get_partition_basic_stats_bulk(
        self,
        database: str,
        table: str,
        partition_names: list[str],
        partition_key_names: list[str],
    ) -> dict[str, dict[str, str]]:
        """
        Fetch BASIC_STATS for many partitions in a single HMS round-trip.

        Uses get_partitions_by_names instead of one get_partition_by_name call per
        partition, collapsing N network round-trips into one. Returns a mapping of
        {partition_name: {num_rows, num_files, total_size}} keyed by the canonical
        'key=value/...' name reconstructed from each partition's values.

        Names whose values contain characters HMS escapes in partition-name form may
        not match the input names; callers should fall back to get_partition_basic_stats
        for any requested name missing from the returned mapping.
        """
        names = list(partition_names)
        if not names:
            return {}
        parts = self._call("get_partitions_by_names", database, table, names)
        out: dict[str, dict[str, str]] = {}
        for p in parts:
            name = "/".join(
                f"{k}={v}" for k, v in zip(partition_key_names, p.values or [])
            )
            params = dict(p.parameters or {})
            out[name] = {
                "num_rows": params.get("numRows", "-1"),
                "num_files": params.get("numFiles", "-1"),
                "total_size": params.get("totalSize", "-1"),
            }
        return out

    def get_table_stats(self, database: str, table: str) -> dict[str, Any]:
        """Fetch table statistics. Uses capability-aware get_table internally."""
        info = self.get_table(database, table)
        num_rows = info.get("num_rows", "-1")
        total_size = info.get("total_size", "-1")
        num_files = info.get("num_files", "-1")
        params: dict[str, str] = info["parameters"]
        last_modified = params.get("transient_lastDdlTime", "")
        try:
            stats_available = int(num_rows) >= 0
        except (TypeError, ValueError):
            stats_available = False

        return {
            "num_rows": num_rows,
            "total_size": total_size,
            "num_files": num_files,
            "stats_available": stats_available,
            "last_modified": last_modified,
        }

    def get_table_objects(self, database: str, tables: list[str]) -> dict[str, Any]:
        """
        Bulk-fetch raw Thrift Table objects for many tables in one round-trip.

        Uses get_table_objects_by_name (batched) instead of one get_table call per
        table. Returns {table_name: Table} for tables that could be fetched; tables
        that fail are simply omitted (matching the per-table skip-on-error behavior).
        Falls back to per-table fetch for any batch where the bulk API is unavailable,
        so results are identical regardless of HMS version.
        """
        out: dict[str, Any] = {}
        if not tables:
            return out
        batch = 200
        for i in range(0, len(tables), batch):
            chunk = tables[i:i + batch]
            try:
                objs = self._call("get_table_objects_by_name", database, chunk)
                for o in objs:
                    if o.tableName:
                        out[o.tableName] = o
            except Exception:
                for t in chunk:
                    try:
                        out[t] = self._fetch_table_with_fallbacks(database, t)
                    except Exception:
                        pass
        return out

    def search_tables(self, keyword: str, database: str | None = None) -> list[dict[str, str]]:
        """
        Search for tables whose name or column names contain keyword.

        Searches all databases unless database is specified. Name matches are cheap
        (no Thrift fetch required); column matches read schema metadata that is
        bulk-fetched once per database via get_table_objects_by_name, so column
        scanning costs one round-trip per database instead of one per table.
        Results are capped at _SEARCH_RESULTS_CAP total matches. When searching all
        databases, column matches from any one database are further capped at
        _SEARCH_COLUMN_MATCH_PER_DB so other databases can still contribute results.
        """
        kw = keyword.lower()
        databases = [database] if database else self.get_all_databases()
        results: list[dict[str, str]] = []

        for db in databases:
            if len(results) >= _SEARCH_RESULTS_CAP:
                break
            try:
                tables = self.get_all_tables(db)
            except Exception:
                continue

            # Bulk-prefetch column metadata for tables that don't match by name, so the
            # per-table loop below does column matching from memory (one round-trip per
            # database) instead of a separate get_table call for every table.
            to_fetch = [t for t in tables if kw not in t.lower()]
            table_objs = self.get_table_objects(db, to_fetch) if to_fetch else {}

            for tbl_name in tables:
                if len(results) >= _SEARCH_RESULTS_CAP:
                    break

                if kw in tbl_name.lower():
                    results.append({"database": db, "table": tbl_name, "match_reason": "table name"})
                    continue

                # Unscoped multi-db search: limit column matches per database so one
                # large schema cannot consume all remaining result slots.
                if database is None:
                    col_matches_in_db = sum(
                        1 for r in results
                        if r["database"] == db and r["match_reason"].startswith("column")
                    )
                    if col_matches_in_db >= _SEARCH_COLUMN_MATCH_PER_DB:
                        continue

                tbl_obj = table_objs.get(tbl_name)
                if tbl_obj is None:
                    continue
                sd = tbl_obj.sd
                col_names = [c.name.lower() for c in (sd.cols if sd else [])]
                part_key_names = [k.name.lower() for k in (tbl_obj.partitionKeys or [])]
                matched_col = next((c for c in col_names + part_key_names if kw in c), None)
                if matched_col:
                    results.append({
                        "database": db,
                        "table": tbl_name,
                        "match_reason": f"column '{matched_col}'",
                    })

        return results

    def get_table_ddl(self, database: str, table: str) -> str:
        """
        Reconstruct a CREATE TABLE statement from HMS metadata.

        The output is derived from stored metadata and may omit properties set at
        table creation time that HMS does not persist. Uses the capability-aware
        fetch path to support ACID and insert-only tables.
        """
        tbl = self._fetch_table_with_fallbacks(database, table)
        sd = tbl.sd
        params: dict[str, str] = _sanitize_params(dict(tbl.parameters or {}))

        lines: list[str] = [
            "-- Note: DDL reconstructed from Hive Metastore metadata.",
            "-- Some properties may be normalized or omitted.",
            f"CREATE {'EXTERNAL ' if tbl.tableType == 'EXTERNAL_TABLE' else ''}TABLE {database}.{tbl.tableName} (",
        ]

        cols = list(sd.cols if sd else [])
        all_fields = [
            f"{c.name} {c.type}{('  -- ' + c.comment) if c.comment else ''}"
            for c in cols
        ]
        for i, field_line in enumerate(all_fields):
            comma = "," if i < len(all_fields) - 1 else ""
            lines.append(f"  {field_line}{comma}")
        lines.append(")")

        pkeys = list(tbl.partitionKeys or [])
        if pkeys:
            pk_defs = ", ".join(f"{k.name} {k.type}" for k in pkeys)
            lines.append(f"PARTITIONED BY ({pk_defs})")

        if sd:
            fmt = _friendly_format(sd.inputFormat or "")
            if fmt in ("ORC", "Parquet", "Avro", "RCFile", "SequenceFile"):
                lines.append(f"STORED AS {fmt}")
            else:
                if sd.inputFormat:
                    lines.append(f"STORED AS INPUTFORMAT '{sd.inputFormat}'")
                if sd.outputFormat:
                    lines.append(f"           OUTPUTFORMAT '{sd.outputFormat}'")
            if sd.serdeInfo and sd.serdeInfo.serializationLib:
                lines.append(f"ROW FORMAT SERDE '{sd.serdeInfo.serializationLib}'")
            if sd.location:
                lines.append(f"LOCATION '{sd.location}'")

        _internal_skip = {
            "numFiles", "numRows", "rawDataSize", "totalSize",
            "numFilesErasureCoded", "transient_lastDdlTime",
            "bucketing_version", "COLUMN_STATS_ACCURATE",
        }
        tbl_props = {k: v for k, v in params.items() if k not in _internal_skip}
        if tbl_props:
            lines.append("TBLPROPERTIES (")
            props_list = list(tbl_props.items())
            for i, (k, v) in enumerate(props_list):
                comma = "," if i < len(props_list) - 1 else ""
                lines.append(f"  '{k}'='{v}'{comma}")
            lines.append(")")

        lines.append(";")
        return "\n".join(lines)
