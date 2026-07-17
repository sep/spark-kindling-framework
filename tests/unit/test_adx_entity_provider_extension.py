import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from kindling.data_entities import EntityMetadata

EXTENSION_PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "packages" / "kindling_adx"


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


def _entity(tags):
    return EntityMetadata(
        entityid="gold.orders",
        name="orders",
        partition_columns=[],
        merge_columns=[],
        tags={"provider_type": "adx", **tags},
        schema=None,
    )


class _Writer:
    def __init__(self):
        self.format_name = None
        self.options = {}
        self.mode_name = None
        self.saved = False

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def mode(self, name):
        self.mode_name = name
        return self

    def save(self):
        self.saved = True


class _StreamWriter:
    def __init__(self):
        self.format_name = None
        self.options = {}
        self.output_mode = None
        self.query_name = None
        self.started = False
        self.query = MagicMock(name="streaming_query")

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def outputMode(self, name):
        self.output_mode = name
        return self

    def queryName(self, name):
        self.query_name = name
        return self

    def start(self):
        self.started = True
        return self.query


def _provider():
    from kindling_adx import AdxEntityProvider

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    return AdxEntityProvider(logger_provider)


def test_import_registers_adx_provider():
    for module_name in list(sys.modules):
        if module_name == "kindling_adx" or module_name.startswith("kindling_adx."):
            del sys.modules[module_name]

    registry = MagicMock()

    with patch("kindling.injection.GlobalInjector.get", return_value=registry):
        import kindling_adx

    provider_class = registry.register_provider.call_args.args[1]
    assert registry.register_provider.call_args.args[0] == "adx"
    assert provider_class.__name__ == "AdxEntityProvider"


def test_managed_identity_append_uses_generic_kusto_sink_options():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer
    entity = _entity(
        {
            "provider.auth": "managed_identity",
            "provider.cluster": "https://fawkes.eastus.kusto.windows.net",
            "provider.database": "Kindling",
            "provider.table": "Orders",
        }
    )

    provider.append_to_entity(df, entity)

    assert writer.format_name == "com.microsoft.kusto.spark.datasource"
    assert writer.options["kustoCluster"] == "https://fawkes.eastus.kusto.windows.net"
    assert writer.options["kustoDatabase"] == "Kindling"
    assert writer.options["kustoTable"] == "Orders"
    assert writer.options["managedIdentityAuth"] == "true"
    assert writer.options["writeMode"] == "Queued"
    assert writer.options["tableCreateOptions"] == "FailIfNotExist"
    assert writer.mode_name == "Append"
    assert writer.saved is True


def test_synapse_linked_service_uses_synapse_connector_format():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer
    entity = _entity(
        {
            "provider.auth": "synapse_linked_service",
            "provider.linked_service": "fawkes_adx",
            "provider.database": "Kindling",
            "provider.table": "Orders",
        }
    )

    provider.append_to_entity(df, entity)

    assert writer.format_name == "com.microsoft.kusto.spark.synapse.datasource"
    assert writer.options == {
        "kustoDatabase": "Kindling",
        "kustoTable": "Orders",
        "writeMode": "Queued",
        "tableCreateOptions": "FailIfNotExist",
        "spark.synapse.linkedService": "fawkes_adx",
    }


def test_service_principal_requires_all_auth_fields():
    provider = _provider()
    df = MagicMock()
    df.write = _Writer()
    entity = _entity(
        {
            "provider.auth": "service_principal",
            "provider.cluster": "https://fawkes.eastus.kusto.windows.net",
            "provider.database": "Kindling",
            "provider.table": "Orders",
            "provider.client_id": "client-id",
        }
    )

    with pytest.raises(ValueError, match="kustoAadAppSecret"):
        provider.append_to_entity(df, entity)


def test_extra_connector_options_are_passed_through():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer
    entity = _entity(
        {
            "provider.auth": "managed_identity",
            "provider.cluster": "fawkes.eastus",
            "provider.database": "Kindling",
            "provider.table": "Orders",
            "provider.option.adjustSchema": "GenerateDynamicCsvMapping",
            "provider.option.clientBatchingLimit": "1024",
            "provider.option.useManagedIdentity": "true",
        }
    )

    provider.append_to_entity(df, entity)

    assert writer.options["adjustSchema"] == "GenerateDynamicCsvMapping"
    assert writer.options["clientBatchingLimit"] == "1024"
    assert writer.options["useManagedIdentity"] == "true"


def test_stream_append_starts_query_with_kusto_sink_options():
    provider = _provider()
    writer = _StreamWriter()
    df = MagicMock()
    df.writeStream = writer
    entity = _entity(
        {
            "provider.auth": "managed_identity",
            "provider.cluster": "https://fawkes.eastus.kusto.windows.net",
            "provider.database": "Kindling",
            "provider.table": "Orders",
            "provider.query_name": "orders_to_adx",
            "provider.option.adjustSchema": "GenerateDynamicCsvMapping",
        }
    )

    query = provider.append_as_stream(df, entity, "/chk/orders")

    assert query is writer.query
    assert writer.format_name == "com.microsoft.kusto.spark.datasource"
    assert writer.output_mode == "append"
    assert writer.query_name == "orders_to_adx"
    assert writer.options["checkpointLocation"] == "/chk/orders"
    assert writer.options["kustoCluster"] == "https://fawkes.eastus.kusto.windows.net"
    assert writer.options["kustoDatabase"] == "Kindling"
    assert writer.options["kustoTable"] == "Orders"
    assert writer.options["managedIdentityAuth"] == "true"
    assert writer.options["writeMode"] == "Queued"
    assert writer.options["adjustSchema"] == "GenerateDynamicCsvMapping"
    assert writer.started is True


def test_stream_append_merges_call_options_and_allows_format_override():
    provider = _provider()
    writer = _StreamWriter()
    df = MagicMock()
    df.writeStream = writer
    entity = _entity(
        {
            "provider.auth": "managed_identity",
            "provider.cluster": "https://fawkes.eastus.kusto.windows.net",
            "provider.database": "Kindling",
            "provider.table": "Orders",
        }
    )

    provider.append_as_stream(
        df,
        entity,
        "/chk/orders",
        format="custom.kusto.format",
        options={
            "output_mode": "update",
            "write_mode": "Transactional",
            "option.clientBatchingLimit": "1024",
        },
    )

    assert writer.format_name == "custom.kusto.format"
    assert writer.output_mode == "update"
    assert writer.options["writeMode"] == "Transactional"
    assert writer.options["clientBatchingLimit"] == "1024"


def test_stream_append_uses_synapse_linked_service_format():
    provider = _provider()
    writer = _StreamWriter()
    df = MagicMock()
    df.writeStream = writer
    entity = _entity(
        {
            "provider.auth": "synapse_linked_service",
            "provider.linked_service": "fawkes_adx",
            "provider.database": "Kindling",
            "provider.table": "Orders",
        }
    )

    provider.append_as_stream(df, entity, "/chk/orders")

    assert writer.format_name == "com.microsoft.kusto.spark.synapse.datasource"
    assert writer.options["spark.synapse.linkedService"] == "fawkes_adx"


def test_check_entity_exists_defaults_to_true_for_write_only_target():
    assert _provider().check_entity_exists(_entity({})) is True


def test_table_is_required():
    provider = _provider()
    df = MagicMock()
    df.write = _Writer()
    entity = _entity(
        {
            "provider.auth": "managed_identity",
            "provider.cluster": "fawkes.eastus",
            "provider.database": "Kindling",
        }
    )

    with pytest.raises(ValueError, match="provider.table"):
        provider.append_to_entity(df, entity)


class _Reader:
    def __init__(self):
        self.format_name = None
        self.options = {}
        self.loaded_df = MagicMock(name="read_df")

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        return self.loaded_df


def _patched_spark_read(reader):
    spark = MagicMock()
    spark.read = reader
    return patch(
        "kindling_adx.entity_provider_adx.get_or_create_spark_session",
        return_value=spark,
    )


def test_read_entity_by_table_uses_kusto_source_options():
    provider = _provider()
    reader = _Reader()
    entity = _entity(
        {
            "provider.auth": "managed_identity",
            "provider.cluster": "https://fawkes.eastus.kusto.windows.net",
            "provider.database": "Kindling",
            "provider.table": "Orders",
        }
    )

    with _patched_spark_read(reader):
        df = provider.read_entity(entity)

    assert df is reader.loaded_df
    assert reader.format_name == "com.microsoft.kusto.spark.datasource"
    assert reader.options["kustoCluster"] == "https://fawkes.eastus.kusto.windows.net"
    assert reader.options["kustoDatabase"] == "Kindling"
    # The connector's read path is KQL-only; a table read passes the table
    # name as kustoQuery (kustoTable is a sink option).
    assert reader.options["kustoQuery"] == "Orders"
    assert "kustoTable" not in reader.options
    assert reader.options["managedIdentityAuth"] == "true"
    # Write-only options must not leak into reads
    assert "writeMode" not in reader.options
    assert "tableCreateOptions" not in reader.options


def test_read_entity_prefers_query_over_table():
    provider = _provider()
    reader = _Reader()
    entity = _entity(
        {
            "provider.auth": "managed_identity",
            "provider.cluster": "fawkes.eastus",
            "provider.database": "Kindling",
            "provider.table": "Orders",
            "provider.query": "Orders | where Amount > 0",
        }
    )

    with _patched_spark_read(reader):
        provider.read_entity(entity)

    assert reader.options["kustoQuery"] == "Orders | where Amount > 0"
    assert "kustoTable" not in reader.options


def test_read_entity_requires_table_or_query():
    provider = _provider()
    entity = _entity(
        {
            "provider.auth": "managed_identity",
            "provider.cluster": "fawkes.eastus",
            "provider.database": "Kindling",
        }
    )

    with pytest.raises(ValueError, match="provider.table"):
        provider.read_entity(entity)


def test_read_entity_uses_synapse_linked_service_format():
    provider = _provider()
    reader = _Reader()
    entity = _entity(
        {
            "provider.auth": "synapse_linked_service",
            "provider.linked_service": "fawkes_adx",
            "provider.database": "Kindling",
            "provider.table": "Orders",
        }
    )

    with _patched_spark_read(reader):
        provider.read_entity(entity)

    assert reader.format_name == "com.microsoft.kusto.spark.synapse.datasource"
    assert reader.options["spark.synapse.linkedService"] == "fawkes_adx"


def test_read_entity_passes_extra_connector_options():
    provider = _provider()
    reader = _Reader()
    entity = _entity(
        {
            "provider.auth": "managed_identity",
            "provider.cluster": "fawkes.eastus",
            "provider.database": "Kindling",
            "provider.table": "Orders",
            "provider.option.readMode": "ForceSingleMode",
        }
    )

    with _patched_spark_read(reader):
        provider.read_entity(entity)

    assert reader.options["readMode"] == "ForceSingleMode"
