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


def test_resolve_transport_defaults_to_eventhubs_for_fabric(provider):
    entity = _entity(
        {
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": (
                "Endpoint=sb://example.servicebus.windows.net/;"
                "SharedAccessKeyName=test;"
                "SharedAccessKey=abc123;"
            ),
            "provider.eventhub.name": "my-hub",
        }
    )

    assert provider.resolve_transport(entity) == "eventhubs"


def test_resolve_transport_defaults_to_eventhubs_for_synapse():
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    config_service = MagicMock()
    config_service.get.return_value = "synapse"

    with patch(
        "kindling.entity_provider_eventhub.get_or_create_spark_session", return_value=MagicMock()
    ):
        provider = EventHubEntityProvider(logger_provider, config_service)

    entity = _entity(
        {
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": (
                "Endpoint=sb://example.servicebus.windows.net/;"
                "SharedAccessKeyName=test;"
                "SharedAccessKey=abc123;"
            ),
            "provider.eventhub.name": "my-hub",
        }
    )

    assert provider.resolve_transport(entity) == "eventhubs"


def test_build_kafka_config_maps_eventhub_settings(provider):
    raw_connection = (
        "Endpoint=sb://example.servicebus.windows.net/;"
        "SharedAccessKeyName=test;"
        "SharedAccessKey=abc123;"
    )
    config = {
        "eventhub.connectionString": raw_connection,
        "eventhub.name": "my-hub",
        "eventhub.consumerGroup": "$Default",
        "startingPosition": "earliest",
        "maxEventsPerTrigger": 500,
        "operationTimeout": 45000,
    }

    kafka_config = provider._build_kafka_config(config, streaming=True)

    assert kafka_config["kafka.bootstrap.servers"] == "example.servicebus.windows.net:9093"
    assert kafka_config["subscribe"] == "my-hub"
    assert kafka_config["kafka.group.id"] == "$Default"
    assert kafka_config["startingOffsets"] == "earliest"
    assert kafka_config["maxOffsetsPerTrigger"] == "500"
    assert kafka_config["kafka.request.timeout.ms"] == "45000"
    assert kafka_config["kafka.session.timeout.ms"] == "45000"
    assert 'username="$ConnectionString"' in kafka_config["kafka.sasl.jaas.config"]
    assert "EntityPath=my-hub" in kafka_config["kafka.sasl.jaas.config"]


def test_build_kafka_config_rejects_custom_json_offsets(provider):
    config = {
        "eventhub.connectionString": (
            "Endpoint=sb://example.servicebus.windows.net/;"
            "SharedAccessKeyName=test;"
            "SharedAccessKey=abc123;"
        ),
        "eventhub.name": "my-hub",
        "startingPosition": '{"offset":"@123"}',
    }

    with pytest.raises(ValueError, match="supports only 'earliest' and 'latest'"):
        provider._build_kafka_config(config, streaming=True)


def test_read_entity_uses_eventhubs_transport_for_fabric(provider):
    entity = _entity(
        {
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": (
                "Endpoint=sb://example.servicebus.windows.net/;"
                "SharedAccessKeyName=test;"
                "SharedAccessKey=abc123;"
            ),
            "provider.eventhub.name": "my-hub",
            "provider.startingPosition": "earliest",
        }
    )
    eventhub_df = MagicMock()
    provider.spark.read.format.return_value.options.return_value.load.return_value = eventhub_df

    provider.read_entity(entity)

    provider.spark.read.format.assert_called_once_with("eventhubs")


def test_read_entity_uses_kafka_transport_for_databricks():
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    config_service = MagicMock()
    config_service.get.return_value = "databricks"

    with patch(
        "kindling.entity_provider_eventhub.get_or_create_spark_session", return_value=MagicMock()
    ):
        provider = EventHubEntityProvider(logger_provider, config_service)

    entity = _entity(
        {
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": (
                "Endpoint=sb://example.servicebus.windows.net/;"
                "SharedAccessKeyName=test;"
                "SharedAccessKey=abc123;"
            ),
            "provider.eventhub.name": "my-hub",
            "provider.startingPosition": "earliest",
        }
    )
    kafka_df = MagicMock()
    kafka_df.withColumnRenamed.return_value = kafka_df
    kafka_df.withColumn.return_value = kafka_df
    provider.spark.read.format.return_value.options.return_value.load.return_value = kafka_df

    provider.read_entity(entity)

    provider.spark.read.format.assert_called_once_with("kafka")


def test_read_entity_as_stream_uses_eventhubs_transport_when_overridden(provider):
    entity = _entity(
        {
            "provider_type": "eventhub",
            "provider.transport": "eventhubs",
            "provider.eventhub.connectionString": (
                "Endpoint=sb://example.servicebus.windows.net/;"
                "SharedAccessKeyName=test;"
                "SharedAccessKey=abc123;"
                "EntityPath=my-hub"
            ),
            "provider.eventhub.name": "my-hub",
        }
    )
    provider.spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt.return_value = (
        "encrypted_conn"
    )
    stream_df = MagicMock()
    provider.spark.readStream.format.return_value.options.return_value.load.return_value = stream_df

    provider.read_entity_as_stream(entity)

    provider.spark.readStream.format.assert_called_once_with("eventhubs")
