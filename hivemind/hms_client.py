from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from typing import Any

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport
from thrift.transport.TTransport import TTransportException

logger = logging.getLogger(__name__)

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

_SEARCH_TABLE_CAP = 50


def _friendly_format(class_name: str) -> str:
    return _FORMAT_ALIASES.get(class_name, class_name.split(".")[-1] if class_name else "Unknown")


def _sanitise_params(params: dict[str, str]) -> dict[str, str]:
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

    def __init__(self, host: str, port: int = 9083, timeout_ms: int = 10_000) -> None:
        self._host = host
        self._port = port
        self._timeout_ms = timeout_ms
        self._transport: TTransport.TBufferedTransport | None = None
        self._client: Any = None
        self._connect()

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
            except Exception:
                pass

        sock = TSocket.TSocket(self._host, self._port)
        sock.setTimeout(self._timeout_ms)
        self._transport = TTransport.TBufferedTransport(sock)
        protocol = TBinaryProtocol.TBinaryProtocol(self._transport)
        self._client = ThriftHiveMetastore.Client(protocol)
        self._transport.open()
        logger.info("Connected to HMS at %s:%d", self._host, self._port)

    def _call(self, fn_name: str, *args: Any) -> Any:
        """Calls a Thrift method, retrying once on transport failure."""
        try:
            return getattr(self._client, fn_name)(*args)
        except TTransportException:
            logger.warning("Transport error on %s - reconnecting", fn_name)
            self._connect()
            return getattr(self._client, fn_name)(*args)

    def close(self) -> None:
        if self._transport and self._transport.isOpen():
            self._transport.close()

    def ping(self) -> bool:
        try:
            self._call("get_all_databases")
            return True
        except Exception:
            return False

    def get_all_databases(self) -> list[str]:
        return sorted(self._call("get_all_databases"))

    def get_all_tables(self, database: str) -> list[str]:
        return sorted(self._call("get_all_tables", database))

    def get_table(self, database: str, table: str) -> dict[str, Any]:
        tbl = self._call("get_table", database, table)
        sd = tbl.sd
        raw_params: dict[str, str] = dict(tbl.parameters or {})
        clean_params = _sanitise_params(raw_params)

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

        return {
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

    def get_partition_names(self, database: str, table: str, max_parts: int = 20) -> list[str]:
        cap = min(max_parts, 20)
        return self._call("get_partition_names", database, table, cap)

    def get_table_stats(self, database: str, table: str) -> dict[str, Any]:
        tbl = self._call("get_table", database, table)
        params: dict[str, str] = dict(tbl.parameters or {})
        num_rows = params.get("numRows", "-1")
        total_size = params.get("totalSize", "-1")
        num_files = params.get("numFiles", "-1")
        last_modified = params.get("transient_lastDdlTime", "")
        stats_available = num_rows not in ("-1", "", None) and int(num_rows) >= 0

        return {
            "num_rows": num_rows,
            "total_size": total_size,
            "num_files": num_files,
            "stats_available": stats_available,
            "last_modified": last_modified,
        }

    def search_tables(self, keyword: str, database: str | None = None) -> list[dict[str, str]]:
        kw = keyword.lower()
        databases = [database] if database else self.get_all_databases()
        results: list[dict[str, str]] = []

        for db in databases:
            if len(results) >= 20:
                break
            try:
                tables = self.get_all_tables(db)
            except Exception:
                continue

            for tbl_name in tables:
                if len(results) >= 20:
                    break

                if kw in tbl_name.lower():
                    results.append({"database": db, "table": tbl_name, "match_reason": "table name"})
                    continue

                scanned = sum(1 for r in results if r["database"] == db)
                if scanned >= _SEARCH_TABLE_CAP:
                    continue
                try:
                    tbl_obj = self._call("get_table", db, tbl_name)
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
                except Exception:
                    continue

        return results

    def get_table_ddl(self, database: str, table: str) -> str:
        tbl = self._call("get_table", database, table)
        sd = tbl.sd
        params: dict[str, str] = _sanitise_params(dict(tbl.parameters or {}))

        lines: list[str] = [
            "-- Reconstructed DDL (from HMS metadata - not original source DDL)",
            f"CREATE {'EXTERNAL ' if tbl.tableType == 'EXTERNAL_TABLE' else ''}TABLE `{database}`.`{tbl.tableName}` (",
        ]

        cols = list(sd.cols if sd else [])
        all_fields = [
            f"`{c.name}` {c.type}{('  -- ' + c.comment) if c.comment else ''}"
            for c in cols
        ]
        for i, field_line in enumerate(all_fields):
            comma = "," if i < len(all_fields) - 1 else ""
            lines.append(f"  {field_line}{comma}")
        lines.append(")")

        pkeys = list(tbl.partitionKeys or [])
        if pkeys:
            pk_defs = ", ".join(f"`{k.name}` {k.type}" for k in pkeys)
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
