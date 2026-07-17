import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import EntityMetadata

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_cosmos"
)


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


def _entity(tags):
    return EntityMetadata(
        entityid="gold.orders",
        name="orders",
        partition_columns=[],
        merge_columns=[],
        tags={"provider_type": "cosmos", **tags},
        schema=None,
    )


BASE_TAGS = {
    "provider.auth": "service_principal",
    "provider.account_endpoint": "https://fawkes.documents.azure.com:443/",
    "provider.database": "Kindling",
    "provider.container": "Orders",
    "provider.client_id": "client-id",
    "provider.client_secret": "client-secret",
    "provider.tenant_id": "tenant-id",
    "provider.subscription_id": "sub-id",
    "provider.resource_group": "rg-name",
}


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


def _provider():
    from kindling_ext_cosmos import CosmosEntityProvider

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    return CosmosEntityProvider(logger_provider)


def _patched_spark_read(reader):
    spark = MagicMock()
    spark.read = reader
    return patch(
        "kindling_ext_cosmos.entity_provider_cosmos.get_or_create_spark_session",
        return_value=spark,
    )


def test_import_registers_cosmos_provider():
    for module_name in list(sys.modules):
        if module_name == "kindling_ext_cosmos" or module_name.startswith("kindling_ext_cosmos."):
            del sys.modules[module_name]

    registry = MagicMock()

    with patch("kindling.injection.GlobalInjector.get", return_value=registry):
        import kindling_ext_cosmos  # noqa: F401

    provider_class = registry.register_provider.call_args.args[1]
    assert registry.register_provider.call_args.args[0] == "cosmos"
    assert provider_class.__name__ == "CosmosEntityProvider"


def test_write_upserts_with_service_principal_options():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer

    provider.write_to_entity(df, _entity(BASE_TAGS))

    assert writer.format_name == "cosmos.oltp"
    assert writer.options["spark.cosmos.accountEndpoint"] == (
        "https://fawkes.documents.azure.com:443/"
    )
    assert writer.options["spark.cosmos.database"] == "Kindling"
    assert writer.options["spark.cosmos.container"] == "Orders"
    assert writer.options["spark.cosmos.auth.type"] == "ServicePrincipal"
    assert writer.options["spark.cosmos.auth.aad.clientId"] == "client-id"
    assert writer.options["spark.cosmos.auth.aad.clientSecret"] == "client-secret"
    assert writer.options["spark.cosmos.account.tenantId"] == "tenant-id"
    assert writer.options["spark.cosmos.account.subscriptionId"] == "sub-id"
    assert writer.options["spark.cosmos.account.resourceGroupName"] == "rg-name"
    assert writer.options["spark.cosmos.write.strategy"] == "ItemOverwrite"
    assert writer.mode_name == "Append"
    assert writer.saved is True


def test_write_strategy_is_configurable():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer

    provider.append_to_entity(df, _entity({**BASE_TAGS, "provider.write_strategy": "ItemAppend"}))

    assert writer.options["spark.cosmos.write.strategy"] == "ItemAppend"


def test_master_key_auth():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer
    tags = {
        "provider.auth": "master_key",
        "provider.account_endpoint": "https://fawkes.documents.azure.com:443/",
        "provider.database": "Kindling",
        "provider.container": "Orders",
        "provider.account_key": "s3cret==",
    }

    provider.write_to_entity(df, _entity(tags))

    assert writer.options["spark.cosmos.accountKey"] == "s3cret=="
    assert "spark.cosmos.auth.type" not in writer.options


def test_service_principal_requires_all_auth_fields():
    provider = _provider()
    df = MagicMock()
    df.write = _Writer()
    tags = {key: value for key, value in BASE_TAGS.items() if key != "provider.client_secret"}

    with pytest.raises(ValueError, match="clientSecret"):
        provider.write_to_entity(df, _entity(tags))


def test_container_is_required():
    provider = _provider()
    df = MagicMock()
    df.write = _Writer()
    tags = {key: value for key, value in BASE_TAGS.items() if key != "provider.container"}

    with pytest.raises(ValueError, match="provider.container"):
        provider.write_to_entity(df, _entity(tags))


def test_extra_connector_options_are_passed_through():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer

    provider.write_to_entity(
        df,
        _entity({**BASE_TAGS, "provider.option.spark.cosmos.write.bulk.enabled": "false"}),
    )

    assert writer.options["spark.cosmos.write.bulk.enabled"] == "false"


def test_read_entity_scans_container_with_inferred_schema():
    provider = _provider()
    reader = _Reader()

    with _patched_spark_read(reader):
        df = provider.read_entity(_entity(BASE_TAGS))

    assert df is reader.loaded_df
    assert reader.format_name == "cosmos.oltp"
    assert reader.options["spark.cosmos.container"] == "Orders"
    assert reader.options["spark.cosmos.read.inferSchema.enabled"] == "true"
    assert reader.options["spark.cosmos.auth.type"] == "ServicePrincipal"
    # Write-only options must not leak into reads
    assert "spark.cosmos.write.strategy" not in reader.options


def test_read_entity_uses_custom_query():
    provider = _provider()
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(
            _entity({**BASE_TAGS, "provider.query": "SELECT c.id FROM c WHERE c.amount > 15"})
        )

    assert reader.options["spark.cosmos.read.customQuery"] == (
        "SELECT c.id FROM c WHERE c.amount > 15"
    )


def test_read_infer_schema_can_be_disabled():
    provider = _provider()
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(_entity({**BASE_TAGS, "provider.infer_schema": "false"}))

    assert reader.options["spark.cosmos.read.inferSchema.enabled"] == "false"


def test_stream_append_starts_query_with_cosmos_sink_options():
    provider = _provider()
    writer = _StreamWriter()
    df = MagicMock()
    df.writeStream = writer

    query = provider.append_as_stream(
        df, _entity({**BASE_TAGS, "provider.query_name": "orders_to_cosmos"}), "/chk/orders"
    )

    assert query is writer.query
    assert writer.format_name == "cosmos.oltp"
    assert writer.output_mode == "append"
    assert writer.query_name == "orders_to_cosmos"
    assert writer.options["checkpointLocation"] == "/chk/orders"
    assert writer.options["spark.cosmos.container"] == "Orders"
    assert writer.options["spark.cosmos.write.strategy"] == "ItemOverwrite"
    assert writer.started is True


def test_check_entity_exists_defaults_to_true():
    assert _provider().check_entity_exists(_entity(BASE_TAGS)) is True


def test_unsupported_auth_mode_raises():
    provider = _provider()
    df = MagicMock()
    df.write = _Writer()

    with pytest.raises(ValueError, match="Unsupported Cosmos auth mode"):
        provider.write_to_entity(df, _entity({**BASE_TAGS, "provider.auth": "magic"}))
