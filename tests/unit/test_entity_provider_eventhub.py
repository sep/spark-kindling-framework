from unittest.mock import MagicMock, patch

import pytest
from pyspark import SparkContext

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_eventhub import (
    _AVRO_SINGLE_OBJECT_MARKER,
    _PREPROCESS_MODES,
    EventHubEntityProvider,
    _decode_amqp_primitive,
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
            lambda df, amqp_headers=False: (calls.append(df), transformed_df)[1],
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
            lambda df, amqp_headers=False: (calls.append(df), transformed_df)[1],
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
                lambda df, amqp_headers=False: (calls.append(df), transformed_df)[1],
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


class TestDecodeAmqpPrimitiveFunction:
    """Pure-Python unit coverage for _decode_amqp_primitive, independent of
    Spark, for fast edge-case checks."""

    def test_empty_or_none_returns_none(self):
        assert _decode_amqp_primitive(None) is None
        assert _decode_amqp_primitive(b"") is None

    def test_null_constructor_returns_none(self):
        assert _decode_amqp_primitive(bytes([0x40])) is None

    def test_true_false_constructors(self):
        assert _decode_amqp_primitive(bytes([0x41])) == "true"
        assert _decode_amqp_primitive(bytes([0x42])) == "false"

    def test_uint0_ulong0_constructors(self):
        assert _decode_amqp_primitive(bytes([0x43])) == "0"
        assert _decode_amqp_primitive(bytes([0x44])) == "0"

    def test_smallint_family_signed(self):
        import struct as _struct

        assert _decode_amqp_primitive(bytes([0x54]) + _struct.pack(">b", -5)) == "-5"

    def test_truncated_payload_falls_back_without_raising(self):
        # 0x81 (long) claims 8 bytes follow but only 2 are given.
        result = _decode_amqp_primitive(bytes([0x81, 0x00, 0x01]))
        assert result is not None  # falls back to a lossy decode, doesn't raise


def test_read_entity_passes_amqp_headers_flag_to_preprocessor(provider, monkeypatch):
    import kindling.entity_provider_eventhub as eventhub_module

    received_kwargs = {}

    def _spy(df, amqp_headers=False):
        received_kwargs["amqp_headers"] = amqp_headers
        return df

    monkeypatch.setitem(eventhub_module._PREPROCESS_MODES, "kafka", _spy)

    entity = _entity(
        {
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": _connection_string(),
            "provider.eventhub.name": "my-hub",
            "provider.preprocess": "kafka",
            "provider.amqp_headers": "true",
        }
    )
    provider.spark.read.format.return_value.options.return_value.load.return_value = MagicMock()

    provider.read_entity(entity)

    assert received_kwargs["amqp_headers"] is True


class TestAmqpHeaderDecoding:
    """provider.amqp_headers: true -- AMQP 1.0 primitive-typed header value
    decoding (Event Hubs' Kafka protocol head surfaces AMQP annotations as
    Kafka headers whose values are AMQP-encoded, not plain UTF-8)."""

    def _df_with_header_value(self, spark_session, value_bytes, key="x-opt-enqueued-time"):
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
            [Row(body=b"unused", headers=[Row(key=key, value=value_bytes)])],
            schema=schema,
        )

    def test_default_false_decodes_headers_as_plain_utf8(self, spark_session):
        """Regression: amqp_headers unset/false must keep today's plain
        UTF-8 header decoding (kafka mode's existing behavior)."""
        df = _headers_schema_df(spark_session, b"unused", header_value_bytes=b"application/json")

        result = _PREPROCESS_MODES["kafka"](df, amqp_headers=False)

        assert result.collect()[0]["headers"] == {"content-type": "application/json"}

    def test_decodes_amqp_long_value(self, spark_session):
        import struct as _struct

        enqueued_time_ms = 1699999999123
        value_bytes = bytes([0x81]) + _struct.pack(">q", enqueued_time_ms)
        df = self._df_with_header_value(spark_session, value_bytes)

        result = _PREPROCESS_MODES["kafka"](df, amqp_headers=True)

        assert result.collect()[0]["headers"] == {"x-opt-enqueued-time": str(enqueued_time_ms)}

    def test_decodes_amqp_timestamp_value(self, spark_session):
        import struct as _struct

        ts_ms = 1700000000000
        value_bytes = bytes([0x83]) + _struct.pack(">q", ts_ms)
        df = self._df_with_header_value(spark_session, value_bytes, key="x-opt-enqueued-time")

        result = _PREPROCESS_MODES["kafka"](df, amqp_headers=True)

        assert result.collect()[0]["headers"] == {"x-opt-enqueued-time": str(ts_ms)}

    def test_decodes_amqp_str8_utf8_value(self, spark_session):
        text = "device-42"
        value_bytes = bytes([0xA1, len(text.encode("utf-8"))]) + text.encode("utf-8")
        df = self._df_with_header_value(spark_session, value_bytes, key="x-opt-partition-key")

        result = _PREPROCESS_MODES["kafka"](df, amqp_headers=True)

        assert result.collect()[0]["headers"] == {"x-opt-partition-key": text}

    def test_decodes_amqp_uint_and_boolean_values(self, spark_session):
        import struct as _struct

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
        df = spark_session.createDataFrame(
            [
                Row(
                    body=b"unused",
                    headers=[
                        Row(
                            key="x-opt-sequence-number",
                            value=bytes([0x70]) + _struct.pack(">I", 42),
                        ),
                        Row(key="x-opt-is-duplicate", value=bytes([0x41])),
                    ],
                )
            ],
            schema=schema,
        )

        result = _PREPROCESS_MODES["kafka"](df, amqp_headers=True)

        headers = result.collect()[0]["headers"]
        assert headers["x-opt-sequence-number"] == "42"
        assert headers["x-opt-is-duplicate"] == "true"

    def test_unrecognized_constructor_falls_back_to_lossy_utf8(self, spark_session):
        """An unimplemented/composite AMQP type constructor must not raise
        -- fall back to a best-effort decode instead of failing the read.
        Uses an x-opt- key so this actually exercises
        _decode_amqp_primitive's internal fallback, not the (also-safe but
        different) plain-UTF-8 path non-x-opt- keys always take."""
        value_bytes = bytes([0xC0]) + b"plain-fallback-text"
        df = self._df_with_header_value(spark_session, value_bytes, key="x-opt-custom")

        result = _PREPROCESS_MODES["kafka"](df, amqp_headers=True)

        # Fallback decodes the WHOLE byte sequence (including the
        # unrecognized constructor byte) as best-effort UTF-8 -- not a
        # crash, and not silently dropped.
        headers = result.collect()[0]["headers"]
        assert "plain-fallback-text" in headers["x-opt-custom"]

    def test_decodes_amqp_value_under_non_x_opt_key_when_structurally_exact(self, spark_session):
        """Some producers (observed with Azure IoT Hub's Kafka-compatible
        endpoint) AMQP-encode their OWN custom application headers, not just
        Event Hubs' x-opt- system properties. A structurally exact AMQP
        encoding (constructor + declared length exactly consuming every
        remaining byte) is decoded regardless of key name."""
        value_bytes = bytes([0xA1, len(b"SCD100000000007033")]) + b"SCD100000000007033"
        df = self._df_with_header_value(spark_session, value_bytes, key="DeviceId")

        result = _PREPROCESS_MODES["kafka"](df, amqp_headers=True)

        assert result.collect()[0]["headers"] == {"DeviceId": "SCD100000000007033"}

    def test_non_x_opt_key_left_plain_when_not_structurally_exact_amqp(self, spark_session):
        """A non-x-opt- header whose first byte coincidentally matches a
        real AMQP type-constructor byte, but whose remaining length does
        NOT exactly match that type's required width, must be left as a
        plain UTF-8 decode -- not corrupted by a false-positive AMQP
        decode. Uses 0xA1 (str8-utf8's constructor), the collision most
        likely to occur in real non-ASCII UTF-8 header text."""
        value_bytes = bytes([0xA1]) + "café".encode("utf-8")
        df = self._df_with_header_value(spark_session, value_bytes, key="custom-header")

        result = _PREPROCESS_MODES["kafka"](df, amqp_headers=True)

        headers = result.collect()[0]["headers"]
        assert headers["custom-header"] == value_bytes.decode("utf-8", "replace")

    def test_amqp_headers_composes_with_avro_mode(self, spark_session):
        """amqp_headers applies identically regardless of which preprocess
        mode (kafka/avro) is selected -- it's a header-decoding concern,
        orthogonal to the body payload codec."""
        import struct as _struct

        from pyspark.sql import Row
        from pyspark.sql.types import (
            ArrayType,
            BinaryType,
            StringType,
            StructField,
            StructType,
        )

        fingerprint = bytes(range(16))
        avro_body = _AVRO_SINGLE_OBJECT_MARKER + fingerprint + b"avro-payload"
        enqueued_time_ms = 1700000000000
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
        df = spark_session.createDataFrame(
            [
                Row(
                    body=avro_body,
                    headers=[
                        Row(
                            key="x-opt-enqueued-time",
                            value=bytes([0x81]) + _struct.pack(">q", enqueued_time_ms),
                        )
                    ],
                )
            ],
            schema=schema,
        )

        result = _PREPROCESS_MODES["avro"](df, amqp_headers=True)

        row = result.collect()[0]
        assert row["avro_schema_fingerprint"] == fingerprint.hex().upper()
        assert row["body"] == b"avro-payload"
        assert row["headers"] == {"x-opt-enqueued-time": str(enqueued_time_ms)}


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


def _amqp_str8(text: str) -> bytes:
    encoded = text.encode("utf-8")
    assert len(encoded) <= 255, "use _amqp_str32 for longer strings"
    return bytes([0xA1, len(encoded)]) + encoded


def _amqp_str32(text: str) -> bytes:
    import struct as _struct

    encoded = text.encode("utf-8")
    return bytes([0xB1]) + _struct.pack(">I", len(encoded)) + encoded


def _amqp_timestamp(epoch_ms: int) -> bytes:
    import struct as _struct

    return bytes([0x83]) + _struct.pack(">q", epoch_ms)


class TestAmqpHeaderDecodingIntegration:
    """End-to-end coverage exercising the ACTUAL provider.read_entity()
    preprocessing path -- not just _decode_amqp_primitive or
    _PREPROCESS_MODES in isolation -- with a realistic mix of AMQP-encoded
    Event Hubs system-property headers (x-opt-*) and a plain producer-set
    header in the same row."""

    DEVICE_ID = "device-42"
    PUBLISHER = "publisher-" + ("x" * 300)  # >255 bytes -> forces str32-utf8, not str8-utf8
    ENQUEUED_TIME_MS = 1700000000123
    BODY_TEXT = '{"device_id": "device-42", "temp": 21.5}'

    def _entity_with_preprocessing(self, amqp_headers):
        tags = {
            "provider_type": "eventhub",
            "provider.transport": "kafka",
            "provider.eventhub.connectionString": _connection_string(),
            "provider.eventhub.name": "my-hub",
            "provider.preprocess": "kafka",
            "provider.amqp_headers": "true" if amqp_headers else "false",
        }
        return _entity(tags)

    def _kafka_source_shaped_df(self, spark_session):
        """Matches Spark's real Kafka structured-streaming source schema
        (value/timestamp/headers) BEFORE _normalize_dataframe renames
        value->body -- so read_entity()'s full normalize+preprocess chain
        runs exactly as it would against a genuine Kafka read."""
        from pyspark.sql import Row
        from pyspark.sql.types import (
            ArrayType,
            BinaryType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("value", BinaryType(), True),
                StructField("timestamp", TimestampType(), True),
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
            [
                Row(
                    value=self.BODY_TEXT.encode("utf-8"),
                    timestamp=None,
                    headers=[
                        Row(key="x-opt-partition-key", value=_amqp_str8(self.DEVICE_ID)),
                        Row(key="x-opt-publisher", value=_amqp_str32(self.PUBLISHER)),
                        Row(
                            key="x-opt-enqueued-time",
                            value=_amqp_timestamp(self.ENQUEUED_TIME_MS),
                        ),
                        Row(key="content-type", value=b"application/json"),
                        Row(key="x-opt-malformed", value=bytes([0x81, 0x00])),  # truncated long
                    ],
                )
            ],
            schema=schema,
        )

    def _read_via_provider(self, spark_session, amqp_headers):
        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()
        config_service = MagicMock()
        config_service.get.return_value = "databricks"
        with patch(
            "kindling.entity_provider_eventhub.get_or_create_spark_session",
            return_value=MagicMock(),
        ):
            event_hub_provider = EventHubEntityProvider(logger_provider, config_service)
        event_hub_provider.spark.read.format.return_value.options.return_value.load.return_value = (
            self._kafka_source_shaped_df(spark_session)
        )

        entity = self._entity_with_preprocessing(amqp_headers)
        return event_hub_provider.read_entity(entity)

    def test_full_preprocessing_path_batch_amqp_enabled(self, spark_session):
        result = self._read_via_provider(spark_session, amqp_headers=True)
        row = result.collect()[0]

        # Payload decoded to expected text.
        assert row["body"] == self.BODY_TEXT
        # Headers became a map<string,string>.
        assert isinstance(row["headers"], dict)
        # Short AMQP string decodes exactly.
        assert row["headers"]["x-opt-partition-key"] == self.DEVICE_ID
        # Long AMQP string (str32-utf8) decodes exactly.
        assert row["headers"]["x-opt-publisher"] == self.PUBLISHER
        # AMQP timestamp becomes its expected epoch-millisecond text value.
        assert row["headers"]["x-opt-enqueued-time"] == str(self.ENQUEUED_TIME_MS)
        # Plain UTF-8 header (no x-opt- prefix) is unchanged.
        assert row["headers"]["content-type"] == "application/json"
        # Malformed/unknown value doesn't fail the read or corrupt siblings.
        assert "x-opt-malformed" in row["headers"]
        assert row["headers"]["x-opt-malformed"] is not None

    def test_full_preprocessing_path_amqp_disabled_does_not_claim_decoded_values(
        self, spark_session
    ):
        """With AMQP decoding disabled, the same input follows plain-UTF-8
        header decoding -- it must NOT happen to produce the correct
        string/timestamp values by coincidence."""
        result = self._read_via_provider(spark_session, amqp_headers=False)
        row = result.collect()[0]

        # Body decoding is unaffected by amqp_headers (kafka mode's own
        # body handling, not a header concern).
        assert row["body"] == self.BODY_TEXT
        assert isinstance(row["headers"], dict)
        # AMQP-encoded values must NOT decode to their real values under
        # plain UTF-8 -- proving no accidental/coincidental correctness.
        assert row["headers"]["x-opt-partition-key"] != self.DEVICE_ID
        assert row["headers"]["x-opt-publisher"] != self.PUBLISHER
        assert row["headers"]["x-opt-enqueued-time"] != str(self.ENQUEUED_TIME_MS)
        # The genuinely-plain header is correct either way.
        assert row["headers"]["content-type"] == "application/json"

    def test_streaming_schema_matches_batch_shape(self, spark_session):
        """Batch and streaming reads go through identical Catalyst column
        expressions in _apply_preprocessing/_flatten_kafka_headers --
        verified via schema parity on a real streaming source, since
        injecting fixed header rows into a genuine streaming source isn't
        practical in a unit test."""
        from pyspark.sql.functions import array
        from pyspark.sql.functions import col as _col
        from pyspark.sql.functions import lit as _lit
        from pyspark.sql.functions import struct as _struct_fn
        from pyspark.sql.types import BinaryType

        streaming_df = (
            spark_session.readStream.format("rate")
            .load()
            .withColumn("body", _col("value").cast("string").cast(BinaryType()))
            .withColumn(
                "headers",
                array(
                    _struct_fn(
                        _lit("x-opt-enqueued-time").alias("key"),
                        _col("value").cast("string").cast(BinaryType()).alias("value"),
                    )
                ),
            )
        )
        assert streaming_df.isStreaming is True

        streaming_result = _PREPROCESS_MODES["kafka"](streaming_df, amqp_headers=True)
        batch_result = _PREPROCESS_MODES["kafka"](
            self._kafka_source_shaped_df(spark_session)
            .withColumnRenamed("value", "body")
            .drop("timestamp"),
            amqp_headers=True,
        )

        assert streaming_result.isStreaming is True
        assert dict(streaming_result.dtypes)["body"] == dict(batch_result.dtypes)["body"]
        assert dict(streaming_result.dtypes)["headers"] == dict(batch_result.dtypes)["headers"]

    def test_pipe_extracts_identity_and_enqueue_time_without_amqp_decoder(self, spark_session):
        """An ingestion pipe consuming the preprocessed output extracts
        device identity and enqueue time using only generic map-key lookup
        and a cast -- no AMQP-specific decoding logic of its own, proving
        the provider already did that work."""
        from pyspark.sql.functions import col as _col

        preprocessed = self._read_via_provider(spark_session, amqp_headers=True)

        # This is what a consuming pipe's own transform looks like: plain
        # column/map access, zero knowledge of AMQP framing.
        ingested = preprocessed.select(
            _col("headers")["x-opt-partition-key"].alias("device_id"),
            _col("headers")["x-opt-enqueued-time"].cast("long").alias("enqueued_time_ms"),
            _col("body"),
        )

        row = ingested.collect()[0]
        assert row["device_id"] == self.DEVICE_ID
        assert row["enqueued_time_ms"] == self.ENQUEUED_TIME_MS
        assert row["body"] == self.BODY_TEXT
