"""Unit tests for the Event Hubs provider's config, transport and dispatch.

The preprocessing transforms and AMQP header-decoding UDF need a real
SparkSession to verify, so they live in
tests/integration/test_entity_provider_eventhub_preprocessing.py. Everything
here runs against a mocked session and needs no JVM.
"""

from dataclasses import asdict
from unittest.mock import MagicMock, patch

import pytest
from kindling.entity_provider import (
    DECLARATIVE_SOURCE_OPTION,
    is_declarable_streaming_source,
)
from kindling.entity_provider_eventhub import (
    EventHubEntityProvider,
    _decode_amqp_primitive,
)
from kindling.entity_provider_eventhub_declaration import DECLARABLE_SUPPORTED_TAGS
from pyspark import SparkContext

from tests.eventhub_test_helpers import _connection_string, _entity


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


class TestAmqpHeadersUdfWorkerSafety:
    """The exact regression this guards: _decode_amqp_headers_udf must be
    unpicklable and callable in a process where `kindling` is not
    importable -- the condition on every Spark executor, by design (see
    _build_decode_amqp_headers_udf's docstring). No existing in-process
    test can catch a violation of this, because kindling is already
    imported in the test process itself; this spawns a real subprocess
    with kindling's path removed instead."""

    def test_udf_function_unpickles_and_runs_without_kindling_importable(self):
        import subprocess
        import sys

        import pyspark.cloudpickle as cloudpickle
        from kindling.entity_provider_eventhub import _decode_amqp_headers_udf

        pickled = cloudpickle.dumps(_decode_amqp_headers_udf.func)

        script = """
import sys

# Simulate a Spark executor that never had kindling installed: strip the
# editable-install path this repo's own venv adds for `packages/`, which
# is the ONLY reason `import kindling` would succeed in this interpreter.
sys.path = [p for p in sys.path if "workspaces/kindling/packages" not in p]
assert "kindling" not in sys.modules

import pyspark.cloudpickle as cloudpickle

data = sys.stdin.buffer.read()
fn = cloudpickle.loads(data)
assert "kindling" not in sys.modules, (
    "unpickling imported kindling -- the function is not self-contained"
)

headers = [{"key": "x-opt-seq", "value": bytes([0x70, 0, 0, 0, 42])}]
result = fn(headers)
assert result == {"x-opt-seq": "42"}, result
print("OK")
"""
        proc = subprocess.run(
            [sys.executable, "-c", script],
            input=pickled,
            capture_output=True,
        )
        assert (
            proc.returncode == 0
        ), f"stdout={proc.stdout.decode()!r} stderr={proc.stderr.decode()!r}"
        assert proc.stdout.strip() == b"OK"


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


class TestEventHubDeclarableStreamingSourceSpec:
    def test_provider_implements_declarable_streaming_source_capability(self, provider):
        assert is_declarable_streaming_source(provider) is True

    def test_valid_kafka_config_produces_secret_safe_spec(self, provider):
        secret = "super-secret-shared-access-key"
        entity = _entity(
            {
                "provider_type": "eventhub",
                "provider.transport": "kafka",
                "provider.eventhub.connectionString": _connection_string(secret),
                "provider.eventhub.name": "my-hub",
                "provider.eventhub.consumerGroup": "$Default",
                "provider.startingPosition": "earliest",
                "provider.maxEventsPerTrigger": "500",
                "provider.operationTimeout": "45000",
                "provider.kafka.includeHeaders": "true",
                "provider.preprocess": "kafka",
                "provider.amqp_headers": "true",
            }
        )

        spec = provider.streaming_source_spec(entity)

        assert spec.is_valid is True
        provider.spark.read.format.assert_not_called()
        provider.spark.readStream.format.assert_not_called()
        assert spec.provider_type == "eventhub"
        assert spec.source_format == "kafka"
        assert spec.source_identity == "my-hub@example.servicebus.windows.net"
        assert spec.supported_option_names == DECLARABLE_SUPPORTED_TAGS
        assert "provider.eventhub.connectionString" in spec.applied_option_names
        assert "provider.kafka.includeHeaders" in spec.applied_option_names
        assert spec.preprocessing.mode == "kafka"
        assert spec.preprocessing.amqp_headers is True
        assert spec.preprocessing.kafka_headers_included is True
        rendered = f"{spec!r} {spec} {asdict(spec)}"
        assert secret not in rendered
        assert "SharedAccessKey" not in rendered

    @pytest.mark.parametrize(
        "tag, tags",
        [
            (
                "provider.eventhub.connectionString",
                {"provider.eventhub.name": "my-hub", "provider.transport": "kafka"},
            ),
            (
                "provider.eventhub.connectionString",
                {
                    "provider.eventhub.connectionString": "Endpoint=sb://example.servicebus.windows.net/;",
                    "provider.eventhub.name": "my-hub",
                    "provider.transport": "kafka",
                },
            ),
            (
                "provider.eventhub.name",
                {
                    "provider.eventhub.connectionString": _connection_string(),
                    "provider.transport": "kafka",
                },
            ),
            (
                "provider.transport",
                {
                    "provider.eventhub.connectionString": _connection_string(),
                    "provider.eventhub.name": "my-hub",
                    "provider.transport": "nats",
                },
            ),
            (
                "provider.transport",
                {
                    "provider.eventhub.connectionString": _connection_string(),
                    "provider.eventhub.name": "my-hub",
                    "provider.transport": "eventhubs",
                },
            ),
            (
                "provider.startingPosition",
                {
                    "provider.eventhub.connectionString": _connection_string(),
                    "provider.eventhub.name": "my-hub",
                    "provider.transport": "kafka",
                    "provider.startingPosition": '{"offset":"@123"}',
                },
            ),
            (
                "provider.preprocess",
                {
                    "provider.eventhub.connectionString": _connection_string(),
                    "provider.eventhub.name": "my-hub",
                    "provider.transport": "kafka",
                    "provider.preprocess": "protobuf",
                },
            ),
            (
                "provider.maxEventsPerTrigger",
                {
                    "provider.eventhub.connectionString": _connection_string(),
                    "provider.eventhub.name": "my-hub",
                    "provider.transport": "kafka",
                    "provider.maxEventsPerTrigger": "many",
                },
            ),
            (
                "provider.operationTimeout",
                {
                    "provider.eventhub.connectionString": _connection_string(),
                    "provider.eventhub.name": "my-hub",
                    "provider.transport": "kafka",
                    "provider.operationTimeout": "slow",
                },
            ),
        ],
    )
    def test_invalid_config_produces_secret_safe_issues(self, provider, tag, tags):
        entity = _entity({"provider_type": "eventhub", **tags})

        spec = provider.streaming_source_spec(entity)

        assert spec.is_valid is False
        assert tag in {issue.tag for issue in spec.validation_issues}
        rendered = f"{spec!r} {' '.join(str(issue) for issue in spec.validation_issues)}"
        assert "abc123" not in rendered
        assert "SharedAccessKey" not in rendered

    def test_fabric_auto_transport_is_invalid_for_lakeflow_spec(self, provider):
        entity = _entity(
            {
                "provider_type": "eventhub",
                "provider.eventhub.connectionString": _connection_string(),
                "provider.eventhub.name": "my-hub",
            }
        )

        spec = provider.streaming_source_spec(entity)

        assert spec.source_format == "eventhubs"
        assert any(issue.tag == "provider.transport" for issue in spec.validation_issues)
        assert "kafka" in str(spec.validation_issues[0])

    def test_declarative_source_option_is_not_forwarded_to_kafka_options(self, provider):
        config = {
            "eventhub.connectionString": _connection_string(),
            "eventhub.name": "my-hub",
            DECLARATIVE_SOURCE_OPTION: True,
        }

        kafka_config = provider._build_kafka_config(config, streaming=True)

        assert DECLARATIVE_SOURCE_OPTION not in kafka_config
