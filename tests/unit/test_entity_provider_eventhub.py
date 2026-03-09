from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_eventhub import EventHubEntityProvider


@pytest.fixture
def provider():
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    config_service = MagicMock()
    config_service.get.return_value = "fabric"

    with patch(
        "kindling.entity_provider_eventhub.get_or_create_spark_session", return_value=MagicMock()
    ):
        return EventHubEntityProvider(logger_provider, config_service)


def _entity(tags):
    return EntityMetadata(
        entityid="stream.eventhub.test",
        name="eventhub_test",
        partition_columns=[],
        merge_columns=[],
        tags=tags,
        schema=None,
    )


def test_check_entity_exists_true_for_valid_config(provider):
    entity = _entity(
        {
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": (
                "Endpoint=sb://example.servicebus.windows.net/;"
                "SharedAccessKeyName=test;"
                "SharedAccessKey=abc123;"
                "EntityPath=my-hub"
            ),
            "provider.eventhub.name": "my-hub",
        }
    )

    assert provider.check_entity_exists(entity) is True
    provider.spark.read.format.assert_not_called()


def test_check_entity_exists_false_when_connection_string_missing_required_segments(provider):
    entity = _entity(
        {
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": "Endpoint=sb://example.servicebus.windows.net/;",
            "provider.eventhub.name": "my-hub",
        }
    )

    assert provider.check_entity_exists(entity) is False
    provider.spark.read.format.assert_not_called()


def test_check_entity_exists_false_when_eventhub_name_missing(provider):
    entity = _entity(
        {
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": (
                "Endpoint=sb://example.servicebus.windows.net/;"
                "SharedAccessKeyName=test;"
                "SharedAccessKey=abc123;"
            ),
        }
    )

    assert provider.check_entity_exists(entity) is False
    provider.spark.read.format.assert_not_called()


def test_build_eventhub_config_encrypts_connection_string(provider):
    provider.spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt.return_value = (
        "encrypted_conn"
    )

    config = {
        "eventhub.connectionString": (
            "Endpoint=sb://example.servicebus.windows.net/;"
            "SharedAccessKeyName=test;"
            "SharedAccessKey=abc123;"
            "EntityPath=my-hub"
        ),
        "eventhub.name": "my-hub",
    }

    eh_config = provider._build_eventhub_config(config)

    assert eh_config["eventhubs.connectionString"] == "encrypted_conn"
    provider.spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt.assert_called_once()


def test_build_eventhub_config_falls_back_to_raw_connection_string_when_encrypt_unavailable(
    provider,
):
    provider.spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt.side_effect = Exception(
        "encrypt unavailable"
    )
    raw_connection = (
        "Endpoint=sb://example.servicebus.windows.net/;"
        "SharedAccessKeyName=test;"
        "SharedAccessKey=abc123;"
        "EntityPath=my-hub"
    )

    config = {
        "eventhub.connectionString": raw_connection,
        "eventhub.name": "my-hub",
    }

    eh_config = provider._build_eventhub_config(config)

    assert eh_config["eventhubs.connectionString"] == raw_connection
