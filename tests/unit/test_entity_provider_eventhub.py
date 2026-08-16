from unittest.mock import MagicMock, patch

import pytest
from pyspark import SparkContext

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_eventhub import (
    _AVRO_SINGLE_OBJECT_MARKER,
    _PREPROCESS_MODES,
    EventHubEntityProvider,
)


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


def test_build_kafka_config_passes_through_kafka_prefixed_options(provider):
    config = {
        "eventhub.connectionString": (
            "Endpoint=sb://example.servicebus.windows.net/;"
            "SharedAccessKeyName=test;"
            "SharedAccessKey=abc123;"
        ),
        "eventhub.name": "my-hub",
        "kafka.includeHeaders": True,
        "kafka.minPartitions": 10,
    }

    kafka_config = provider._build_kafka_config(config, streaming=True)

    assert kafka_config["includeHeaders"] == "true"
    assert kafka_config["minPartitions"] == "10"


def test_build_kafka_config_omits_include_headers_when_unset(provider):
    config = {
        "eventhub.connectionString": (
            "Endpoint=sb://example.servicebus.windows.net/;"
            "SharedAccessKeyName=test;"
            "SharedAccessKey=abc123;"
        ),
        "eventhub.name": "my-hub",
    }

    kafka_config = provider._build_kafka_config(config, streaming=True)

    assert "includeHeaders" not in kafka_config


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


def _connection_string(secret="abc123"):
    return (
        "Endpoint=sb://example.servicebus.windows.net/;"
        "SharedAccessKeyName=test;"
        f"SharedAccessKey={secret};"
    )


class TestEventHubPreprocessing:
    """provider.preprocess opt-in DataFrame preprocessing (see
    _PREPROCESS_MODES in kindling.entity_provider_eventhub: "kafka"/"avro")."""

    def test_read_entity_default_no_preprocess_is_noop(self, provider):
        """Entities that never set provider.preprocess get exactly today's
        output -- no-op, not even a wrapper call."""
        entity = _entity(
            {
                "provider_type": "eventhub",
                "provider.eventhub.connectionString": _connection_string(),
                "provider.eventhub.name": "my-hub",
            }
        )
        eventhub_df = MagicMock()
        provider.spark.read.format.return_value.options.return_value.load.return_value = eventhub_df

        result = provider.read_entity(entity)

        assert result is eventhub_df
        eventhub_df.withColumn.assert_not_called()

    def test_read_entity_as_stream_default_no_preprocess_is_noop(self, provider):
        entity = _entity(
            {
                "provider_type": "eventhub",
                "provider.eventhub.connectionString": _connection_string(),
                "provider.eventhub.name": "my-hub",
            }
        )
        stream_df = MagicMock()
        provider.spark.readStream.format.return_value.options.return_value.load.return_value = (
            stream_df
        )

        result = provider.read_entity_as_stream(entity)

        assert result is stream_df
        stream_df.withColumn.assert_not_called()

    def test_unknown_preprocess_mode_raises_clear_entity_scoped_error(self, provider):
        entity = _entity(
            {
                "provider_type": "eventhub",
                "provider.eventhub.connectionString": _connection_string(),
                "provider.eventhub.name": "my-hub",
                "provider.preprocess": "does_not_exist",
            }
        )
        provider.spark.read.format.return_value.options.return_value.load.return_value = MagicMock()

        with pytest.raises(ValueError, match="stream.eventhub.test") as exc_info:
            provider.read_entity(entity)
        message = str(exc_info.value)
        assert "does_not_exist" in message
        assert "kafka" in message
        assert "avro" in message

    def test_read_entity_dispatches_to_kafka_mode_batch(self, provider, monkeypatch):
        import kindling.entity_provider_eventhub as eventhub_module

        calls = []
        transformed_df = MagicMock()
        transformed_df.columns = ["body", "headers"]
        monkeypatch.setitem(
            eventhub_module._PREPROCESS_MODES,
            "kafka",
            lambda df: (calls.append(df), transformed_df)[1],
        )

        entity = _entity(
            {
                "provider_type": "eventhub",
                "provider.eventhub.connectionString": _connection_string(),
                "provider.eventhub.name": "my-hub",
                "provider.preprocess": "kafka",
            }
        )
        eventhub_df = MagicMock()
        provider.spark.read.format.return_value.options.return_value.load.return_value = eventhub_df

        result = provider.read_entity(entity)

        assert result is transformed_df
        assert calls == [eventhub_df]

    def test_read_entity_as_stream_dispatches_to_avro_mode(self, provider, monkeypatch):
        """Same tag, works identically for streaming -- no separate
        configuration needed for batch vs. streaming reads."""
        import kindling.entity_provider_eventhub as eventhub_module

        calls = []
        transformed_df = MagicMock()
        transformed_df.columns = ["body", "avro_schema_fingerprint"]
        monkeypatch.setitem(
            eventhub_module._PREPROCESS_MODES,
            "avro",
            lambda df: (calls.append(df), transformed_df)[1],
        )

        entity = _entity(
            {
                "provider_type": "eventhub",
                "provider.eventhub.connectionString": _connection_string(),
                "provider.eventhub.name": "my-hub",
                "provider.preprocess": "avro",
            }
        )
        stream_df = MagicMock()
        provider.spark.readStream.format.return_value.options.return_value.load.return_value = (
            stream_df
        )

        result = provider.read_entity_as_stream(entity)

        assert result is transformed_df
        assert calls == [stream_df]

    def test_preprocessing_failure_raises_entity_scoped_error_without_leaking_secret(
        self, provider, monkeypatch
    ):
        import kindling.entity_provider_eventhub as eventhub_module

        def _boom(df):
            raise RuntimeError("boom")

        monkeypatch.setitem(eventhub_module._PREPROCESS_MODES, "kafka", _boom)

        secret_value = "super-secret-shared-access-key"
        entity = _entity(
            {
                "provider_type": "eventhub",
                "provider.eventhub.connectionString": _connection_string(secret_value),
                "provider.eventhub.name": "my-hub",
                "provider.preprocess": "kafka",
            }
        )
        provider.spark.read.format.return_value.options.return_value.load.return_value = MagicMock()

        with pytest.raises(RuntimeError) as exc_info:
            provider.read_entity(entity)

        message = str(exc_info.value)
        assert "stream.eventhub.test" in message
        assert "kafka" in message
        assert secret_value not in message

    def test_config_and_secret_resolution_completes_before_preprocess_is_evaluated(
        self, monkeypatch
    ):
        """provider.preprocess must be read from the SAME fully-resolved
        entity tags that carry the resolved @secret: connection string --
        both sourced from a dataentities-bytag: config overlay, both must
        reach the provider only after bootstrap's config-overlay ->
        secret-resolution -> resolved-config-overlay sequence completes.
        Regression coverage tying this feature to the ordering bug fixed
        in kindling.bootstrap (see test_config_override_overlay.py)."""
        import kindling.bootstrap as bootstrap
        from kindling.data_entities import DataEntityManager, DataEntityRegistry
        from kindling.data_pipes import DataPipesRegistry
        from kindling.injection import GlobalInjector
        from kindling.platform_provider import SecretProvider
        from kindling.spark_config import ConfigService

        resolved_connection = _connection_string("resolved-from-secret-provider")

        class FakeSecretProvider(SecretProvider):
            def get_secret(self, secret_name, default=None):
                if secret_name == "myscope:eh_conn":
                    return resolved_connection
                raise KeyError(secret_name)

        class _DynaconfBackedConfigService:
            def __init__(self, dynaconf):
                self.dynaconf = dynaconf

            def get(self, key, default=None):
                return self.dynaconf.get(key, default)

            def get_entity_tags(self, entityid):
                return {}

        GlobalInjector.reset()
        try:
            GlobalInjector.bind(SecretProvider, FakeSecretProvider())

            from dynaconf import Dynaconf

            dynaconf = Dynaconf(environments=False, envvar_prefix="KINDLING")
            dynaconf.set(
                "dataentities",
                {
                    "stream.eventhub.test": {
                        "tags": {
                            "provider.eventhub.connectionString": "@secret:myscope:eh_conn",
                            "provider.preprocess": "kafka",
                        }
                    }
                },
            )
            config_service = _DynaconfBackedConfigService(dynaconf)

            signal_provider = MagicMock()
            entity_manager = DataEntityManager(signal_provider, config_service)
            entity_manager.register_entity(
                "stream.eventhub.test",
                name="eventhub_test",
                merge_columns=[],
                tags={"provider_type": "eventhub", "provider.eventhub.name": "my-hub"},
                schema=None,
            )

            pipes_registry = MagicMock(spec=["apply_config_overrides", "get_pipe_ids"])
            pipes_registry.get_pipe_ids.return_value = []
            services = {
                ConfigService: config_service,
                DataEntityRegistry: entity_manager,
                DataPipesRegistry: pipes_registry,
            }
            monkeypatch.setattr(bootstrap, "get_kindling_service", lambda iface: services[iface])

            calls = []
            transformed_df = MagicMock()
            transformed_df.columns = ["body"]

            logger = MagicMock()
            bootstrap.apply_config_overrides()
            bootstrap._resolve_and_validate_secrets(config_service, logger)
            bootstrap.apply_config_overrides()

            entity = entity_manager.get_entity_definition("stream.eventhub.test")
            assert entity.tags["provider.eventhub.connectionString"] == resolved_connection
            assert entity.tags["provider.preprocess"] == "kafka"

            import kindling.entity_provider_eventhub as eventhub_module

            monkeypatch.setitem(
                eventhub_module._PREPROCESS_MODES,
                "kafka",
                lambda df: (calls.append(df), transformed_df)[1],
            )

            logger_provider = MagicMock()
            logger_provider.get_logger.return_value = MagicMock()
            eventhub_config_service = MagicMock()
            eventhub_config_service.get.return_value = "databricks"
            with patch(
                "kindling.entity_provider_eventhub.get_or_create_spark_session",
                return_value=MagicMock(),
            ):
                event_hub_provider = EventHubEntityProvider(
                    logger_provider, eventhub_config_service
                )
            raw_df = MagicMock()
            event_hub_provider.spark.read.format.return_value.options.return_value.load.return_value = (
                raw_df
            )

            # _normalize_dataframe's kafka-rename branch only fires when a
            # real (JVM-backed) Spark context is active in-process --
            # explicitly force it off so this assertion doesn't depend on
            # whether some other test already started a real SparkSession
            # in the same test run.
            with patch.object(SparkContext, "_active_spark_context", None):
                result = event_hub_provider.read_entity(entity)

            assert result is transformed_df
            assert calls == [raw_df]

            # The Kafka connection secret must already be resolved by the
            # time the provider builds its transport config -- proving
            # preprocessing evaluation and secret resolution share the same
            # fully-overlaid entity, not a stale pre-resolution snapshot.
            built_kafka_config = event_hub_provider._build_kafka_config(
                {
                    "eventhub.connectionString": entity.tags["provider.eventhub.connectionString"],
                    "eventhub.name": "my-hub",
                },
                streaming=False,
            )
            assert "@secret" not in built_kafka_config["kafka.sasl.jaas.config"]
            assert "resolved-from-secret-provider" in built_kafka_config["kafka.sasl.jaas.config"]
        finally:
            GlobalInjector.reset()


def _headers_schema_df(spark_session, body_bytes, header_value_bytes=b"application/json"):
    from pyspark.sql import Row
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("body", BinaryType(), True),
            StructField(
                "headers",
                ArrayType(
                    StructType(
                        [
                            StructField("key", StringType(), True),
                            StructField("value", BinaryType(), True),
                        ]
                    )
                ),
                True,
            ),
        ]
    )
    return spark_session.createDataFrame(
        [Row(body=body_bytes, headers=[Row(key="content-type", value=header_value_bytes)])],
        schema=schema,
    )


class TestPreprocessKafkaMode:
    """The framework-provided provider.preprocess: kafka transform, for
    text-payload producers (JSON, delimited text)."""

    def test_decodes_binary_body_and_flattens_kafka_headers_batch(self, spark_session):
        df = _headers_schema_df(spark_session, "hello world".encode("utf-8"))

        result = _PREPROCESS_MODES["kafka"](df)

        row = result.collect()[0]
        assert row["body"] == "hello world"
        assert row["headers"] == {"content-type": "application/json"}

    def test_noop_when_body_already_text_and_headers_absent(self, spark_session):
        df = spark_session.createDataFrame([("already text",)], ["body"])

        result = _PREPROCESS_MODES["kafka"](df)

        assert result.collect()[0]["body"] == "already text"
        assert "headers" not in result.columns

    def test_decodes_binary_body_for_streaming_dataframe(self, spark_session):
        """Batch and streaming DataFrames go through identical Catalyst
        column expressions here, so schema-level verification on a real
        streaming source is sufficient without running a query."""
        from pyspark.sql.functions import col as _col
        from pyspark.sql.types import BinaryType

        streaming_df = (
            spark_session.readStream.format("rate")
            .load()
            .withColumn("body", _col("value").cast("string").cast(BinaryType()))
        )
        assert streaming_df.isStreaming is True

        result = _PREPROCESS_MODES["kafka"](streaming_df)

        assert result.isStreaming is True
        assert dict(result.dtypes)["body"] == "string"


class TestPreprocessAvroMode:
    """The framework-provided provider.preprocess: avro transform, for Avro
    single-object-encoded payloads (Avro spec standard: 2-byte marker +
    16-byte schema fingerprint + Avro-encoded body)."""

    def test_extracts_fingerprint_and_strips_header_from_conforming_row(self, spark_session):
        fingerprint = bytes(range(16))
        avro_payload = b"avro-encoded-bytes-here"
        body_bytes = _AVRO_SINGLE_OBJECT_MARKER + fingerprint + avro_payload
        df = _headers_schema_df(spark_session, body_bytes)

        result = _PREPROCESS_MODES["avro"](df)

        row = result.collect()[0]
        assert row["avro_schema_fingerprint"] == fingerprint.hex().upper()
        assert row["body"] == avro_payload
        assert row["headers"] == {"content-type": "application/json"}

    def test_non_conforming_row_left_untouched_not_corrupted(self, spark_session):
        body_bytes = b"not-single-object-encoded-at-all"
        df = spark_session.createDataFrame([(body_bytes,)], ["body"])

        result = _PREPROCESS_MODES["avro"](df)

        row = result.collect()[0]
        assert row["avro_schema_fingerprint"] is None
        assert row["body"] == body_bytes

    def test_noop_when_body_absent(self, spark_session):
        df = spark_session.createDataFrame([("no body column here",)], ["not_body"])

        result = _PREPROCESS_MODES["avro"](df)

        assert "avro_schema_fingerprint" not in result.columns
        assert result.collect()[0]["not_body"] == "no body column here"

    def test_streaming_dataframe_schema(self, spark_session):
        from pyspark.sql.functions import col as _col
        from pyspark.sql.types import BinaryType

        streaming_df = (
            spark_session.readStream.format("rate")
            .load()
            .withColumn("body", _col("value").cast("string").cast(BinaryType()))
        )
        assert streaming_df.isStreaming is True

        result = _PREPROCESS_MODES["avro"](streaming_df)

        assert result.isStreaming is True
        assert "avro_schema_fingerprint" in result.columns
