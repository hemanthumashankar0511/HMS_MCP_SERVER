"""
Unit tests for HMSClient.

All Thrift calls are mocked — no live HMS connection required.
Run with:  pytest tests/test_hms_client.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# We patch the Thrift transport so HMSClient never opens a real socket.
_TRANSPORT_PATCH = "hivemind.hms_client.TTransport.TBufferedTransport"
_SOCKET_PATCH = "hivemind.hms_client.TSocket.TSocket"


def _make_client(mock_thrift_client: MagicMock) -> "HMSClient":
    """
    Build an HMSClient with all Thrift I/O replaced by mocks.
    mock_thrift_client is the MagicMock that will be used as the underlying
    ThriftHiveMetastore.Client instance.
    """
    from hivemind.hms_client import HMSClient

    with (
        patch(_SOCKET_PATCH),
        patch(_TRANSPORT_PATCH) as mock_transport_cls,
        patch("hivemind.hms_client.TBinaryProtocol.TBinaryProtocol"),
        patch(
            "hivemind.hms_client.HMSClient._connect",
            lambda self: setattr(self, "_client", mock_thrift_client),
        ),
    ):
        client = HMSClient.__new__(HMSClient)
        client._host = "localhost"
        client._port = 9083
        client._timeout_ms = 5000
        client._transport = MagicMock()
        client._client = mock_thrift_client
    return client


# ---------------------------------------------------------------------------
# get_all_databases
# ---------------------------------------------------------------------------

class TestGetAllDatabases:
    def test_returns_sorted_list(self):
        mock = MagicMock()
        mock.get_all_databases.return_value = ["sys", "default", "sample"]
        from hivemind.hms_client import HMSClient

        client = HMSClient.__new__(HMSClient)
        client._host = "localhost"
        client._port = 9083
        client._transport = MagicMock()
        client._client = mock

        result = client.get_all_databases()
        assert result == ["default", "sample", "sys"]

    def test_empty_metastore(self):
        mock = MagicMock()
        mock.get_all_databases.return_value = []
        from hivemind.hms_client import HMSClient

        client = HMSClient.__new__(HMSClient)
        client._host = "localhost"
        client._port = 9083
        client._transport = MagicMock()
        client._client = mock

        assert client.get_all_databases() == []


# ---------------------------------------------------------------------------
# get_all_tables
# ---------------------------------------------------------------------------

class TestGetAllTables:
    def _client(self, tables):
        mock = MagicMock()
        mock.get_all_tables.return_value = tables
        from hivemind.hms_client import HMSClient

        c = HMSClient.__new__(HMSClient)
        c._host = "localhost"
        c._port = 9083
        c._transport = MagicMock()
        c._client = mock
        return c

    def test_sorted_output(self):
        c = self._client(["zebra", "apple", "mango"])
        assert c.get_all_tables("db") == ["apple", "mango", "zebra"]

    def test_passes_database_name(self):
        mock = MagicMock()
        mock.get_all_tables.return_value = []
        from hivemind.hms_client import HMSClient

        c = HMSClient.__new__(HMSClient)
        c._host = "localhost"
        c._port = 9083
        c._transport = MagicMock()
        c._client = mock
        c.get_all_tables("mydb")
        mock.get_all_tables.assert_called_once_with("mydb")


# ---------------------------------------------------------------------------
# get_table — field extraction and parameter sanitisation
# ---------------------------------------------------------------------------

def _make_thrift_table(
    db="testdb",
    tbl="testtbl",
    ttype="MANAGED_TABLE",
    cols=None,
    part_keys=None,
    params=None,
    location="hdfs://host/path",
    input_fmt="org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
    output_fmt="org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
    serde_lib="org.apache.hadoop.hive.ql.io.orc.OrcSerde",
):
    col_obj = MagicMock()
    col_obj.name = "id"
    col_obj.type = "int"
    col_obj.comment = ""

    serde_info = MagicMock()
    serde_info.serializationLib = serde_lib
    serde_info.name = ""
    serde_info.parameters = {}

    sd = MagicMock()
    sd.location = location
    sd.inputFormat = input_fmt
    sd.outputFormat = output_fmt
    sd.serdeInfo = serde_info
    sd.cols = cols if cols is not None else [col_obj]

    tbl_obj = MagicMock()
    tbl_obj.tableName = tbl
    tbl_obj.dbName = db
    tbl_obj.tableType = ttype
    tbl_obj.partitionKeys = part_keys if part_keys is not None else []
    tbl_obj.parameters = params if params is not None else {"numRows": "100", "numFiles": "2", "totalSize": "1024"}
    tbl_obj.sd = sd
    tbl_obj.owner = "hive"
    tbl_obj.viewOriginalText = ""
    tbl_obj.viewExpandedText = ""
    return tbl_obj


class TestGetTable:
    def _client_for(self, tbl_obj):
        mock = MagicMock()
        mock.get_table.return_value = tbl_obj
        from hivemind.hms_client import HMSClient

        c = HMSClient.__new__(HMSClient)
        c._host = "localhost"
        c._port = 9083
        c._transport = MagicMock()
        c._client = mock
        return c

    def test_basic_fields(self):
        tbl = _make_thrift_table()
        c = self._client_for(tbl)
        result = c.get_table("testdb", "testtbl")
        assert result["name"] == "testtbl"
        assert result["database"] == "testdb"
        assert result["table_type"] == "MANAGED_TABLE"
        assert result["location"] == "hdfs://host/path"
        assert result["input_format"] == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"

    def test_columns_extracted(self):
        tbl = _make_thrift_table()
        c = self._client_for(tbl)
        result = c.get_table("testdb", "testtbl")
        assert len(result["columns"]) == 1
        assert result["columns"][0]["name"] == "id"
        assert result["columns"][0]["type"] == "int"

    def test_sensitive_params_redacted(self):
        params = {
            "fs.s3.awsSecretAccessKey": "SUPER_SECRET",
            "fs.s3.awsAccessKeyId": "AK123",
            "normalParam": "visible",
            "my.token.value": "hidden",
        }
        tbl = _make_thrift_table(params=params)
        c = self._client_for(tbl)
        result = c.get_table("testdb", "testtbl")
        assert result["parameters"]["fs.s3.awsSecretAccessKey"] == "[REDACTED]"
        assert result["parameters"]["fs.s3.awsAccessKeyId"] == "[REDACTED]"
        assert result["parameters"]["my.token.value"] == "[REDACTED]"
        assert result["parameters"]["normalParam"] == "visible"

    def test_no_sd_returns_empty_strings(self):
        tbl = _make_thrift_table()
        tbl.sd = None
        c = self._client_for(tbl)
        result = c.get_table("testdb", "testtbl")
        assert result["location"] == ""
        assert result["columns"] == []


# ---------------------------------------------------------------------------
# get_partition_names — hard cap
# ---------------------------------------------------------------------------

class TestGetPartitionNames:
    def _client_for(self, names):
        mock = MagicMock()
        mock.get_partition_names.return_value = names
        from hivemind.hms_client import HMSClient

        c = HMSClient.__new__(HMSClient)
        c._host = "localhost"
        c._port = 9083
        c._transport = MagicMock()
        c._client = mock
        return c

    def test_respects_hard_cap(self):
        names = [f"dt=2024-01-{i:02d}" for i in range(1, 25)]
        c = self._client_for(names[:20])
        c.get_partition_names("db", "tbl", max_parts=50)
        # The hard cap forces max_parts to 20 internally
        _, args, _ = c._client.get_partition_names.mock_calls[0]
        assert args[2] == 20

    def test_zero_partitions(self):
        c = self._client_for([])
        result = c.get_partition_names("db", "empty_tbl")
        assert result == []


# ---------------------------------------------------------------------------
# get_table_stats
# ---------------------------------------------------------------------------

class TestGetTableStats:
    def _client_for(self, params):
        tbl = _make_thrift_table(params=params)
        mock = MagicMock()
        mock.get_table.return_value = tbl
        from hivemind.hms_client import HMSClient

        c = HMSClient.__new__(HMSClient)
        c._host = "localhost"
        c._port = 9083
        c._transport = MagicMock()
        c._client = mock
        return c

    def test_stats_available(self):
        c = self._client_for({"numRows": "5000", "totalSize": "102400", "numFiles": "3"})
        s = c.get_table_stats("db", "tbl")
        assert s["stats_available"] is True
        assert s["num_rows"] == "5000"

    def test_stats_missing_when_minus_one(self):
        c = self._client_for({"numRows": "-1", "totalSize": "0", "numFiles": "0"})
        s = c.get_table_stats("db", "tbl")
        assert s["stats_available"] is False

    def test_stats_missing_when_absent(self):
        c = self._client_for({})
        s = c.get_table_stats("db", "tbl")
        assert s["stats_available"] is False


# ---------------------------------------------------------------------------
# search_tables
# ---------------------------------------------------------------------------

class TestSearchTables:
    def _make_client_multi(self, db_table_map: dict):
        """db_table_map: {db: [table, ...]}"""
        mock = MagicMock()
        mock.get_all_databases.return_value = sorted(db_table_map.keys())

        def _get_all_tables(db):
            return db_table_map.get(db, [])

        def _get_table(db, tbl):
            t = _make_thrift_table(db=db, tbl=tbl)
            return t

        mock.get_all_tables.side_effect = _get_all_tables
        mock.get_table.side_effect = _get_table

        from hivemind.hms_client import HMSClient

        c = HMSClient.__new__(HMSClient)
        c._host = "localhost"
        c._port = 9083
        c._transport = MagicMock()
        c._client = mock
        return c

    def test_finds_table_by_name(self):
        c = self._make_client_multi({"sales": ["orders", "products"], "hr": ["employees"]})
        results = c.search_tables("order")
        assert any(r["table"] == "orders" for r in results)
        assert all(r["match_reason"] == "table name" for r in results if r["table"] == "orders")

    def test_scoped_to_database(self):
        c = self._make_client_multi({"sales": ["orders"], "hr": ["order_audit"]})
        results = c.search_tables("order", database="sales")
        assert all(r["database"] == "sales" for r in results)

    def test_caps_at_20(self):
        # 25 matching tables
        tables = [f"order_{i}" for i in range(25)]
        c = self._make_client_multi({"bigdb": tables})
        results = c.search_tables("order")
        assert len(results) <= 20

    def test_no_matches(self):
        c = self._make_client_multi({"db": ["apple", "banana"]})
        results = c.search_tables("zzz_nonexistent")
        assert results == []


# ---------------------------------------------------------------------------
# Credential sanitisation unit test
# ---------------------------------------------------------------------------

class TestSanitiseParams:
    def test_exact_redact_keys(self):
        from hivemind.hms_client import _sanitise_params

        params = {
            "fs.s3.awsAccessKeyId": "KEY",
            "fs.s3.awsSecretAccessKey": "SECRET",
            "fs.azure.account.key": "AZKEY",
            "google.cloud.auth.service.account.json.keyfile": "GCKEY",
        }
        result = _sanitise_params(params)
        assert all(v == "[REDACTED]" for v in result.values())

    def test_pattern_redact(self):
        from hivemind.hms_client import _sanitise_params

        params = {
            "my.api.password": "pass123",
            "auth.token": "tok456",
            "my.credential.store": "cred789",
            "table.description": "safe value",
        }
        result = _sanitise_params(params)
        assert result["my.api.password"] == "[REDACTED]"
        assert result["auth.token"] == "[REDACTED]"
        assert result["my.credential.store"] == "[REDACTED]"
        assert result["table.description"] == "safe value"

    def test_keys_preserved(self):
        from hivemind.hms_client import _sanitise_params

        params = {"secret.key": "val"}
        result = _sanitise_params(params)
        assert "secret.key" in result
        assert result["secret.key"] == "[REDACTED]"
