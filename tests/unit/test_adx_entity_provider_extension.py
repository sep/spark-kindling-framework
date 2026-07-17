import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import EntityMetadata

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_adx"
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


def _provider():
    from kindling_ext_adx import AdxEntityProvider

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    return AdxEntityProvider(logger_provider)


def test_import_registers_adx_provider():
    for module_name in list(sys.modules):
        if module_name == "kindling_ext_adx" or module_name.startswith("kindling_ext_adx."):
            del sys.modules[module_name]

    registry = MagicMock()

    with patch("kindling.injection.GlobalInjector.get", return_value=registry):
        import kindling_ext_adx

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
