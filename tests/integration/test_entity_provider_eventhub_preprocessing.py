"""Integration tests for the Event Hubs provider's preprocessing transforms.

Integration rather than unit: these drive the real ``provider.preprocess``
kafka/avro Catalyst expressions and the AMQP header-decoding UDF over real
DataFrames, so a mocked session cannot stand in for them (see
docs/contributing/testing.md). The provider's config, transport-selection and
mode-dispatch behaviour stays in tests/unit/test_entity_provider_eventhub.py,
which needs no JVM.
"""

from unittest.mock import MagicMock, patch

import pytest
from kindling.entity_provider_eventhub import (
    _AVRO_SINGLE_OBJECT_MARKER,
    _PREPROCESS_MODES,
    EventHubEntityProvider,
    _decode_amqp_primitive,
)

from tests.conftest import _sockets_permitted
from tests.eventhub_test_helpers import _connection_string, _entity


@pytest.fixture(scope="module")
def spark_session():
    """Own module-scoped session rather than conftest.py's session-scoped one.

    Sibling integration modules (test_scd2_provider_parity.py,
    test_scd2_declared_flow.py) shut down the JVM gateway outright at module
    setup so their own session puts the Delta jars on the classpath. A
    session-scoped fixture cached earlier in the same worker process would
    still be handed out afterwards as a dead session.
    """
    if not _sockets_permitted():
        pytest.skip(
            "Sockets are not permitted in this environment; cannot start a real SparkSession."
        )
    from tests.spark_test_helper import get_standalone_spark_session

    yield get_standalone_spark_session("EventHubPreprocessingTests")
    # Not spark.stop(): shared JVM-singleton session.


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


class TestAmqpPrimitiveDecodeParity:
    """The production header-decoding UDF (_decode_amqp_headers_udf) wraps
    a function defined entirely inside _build_decode_amqp_headers_udf --
    NOT _decode_amqp_primitive -- specifically so it doesn't reference any
    kindling-module-level name (see that factory's docstring). That nested
    function is therefore not directly importable/callable from a test the
    way _decode_amqp_primitive is. This drives it indirectly, through a
    real x-opt- header value (the code path that uses it), and asserts the
    result matches _decode_amqp_primitive's direct output for the same
    bytes -- so the two implementations cannot silently drift apart."""

    @pytest.mark.parametrize(
        "value_bytes",
        [
            bytes([0x40]),  # null
            bytes([0x41]),  # true
            bytes([0x42]),  # false
            bytes([0x54, 0xFB]),  # smallint -5
            bytes([0x83, 0, 0, 0, 0, 0x65, 0xA0, 0xBC, 0x28]),  # timestamp
            bytes([0xA1, 5]) + b"hello",  # str8-utf8
            bytes([0x81, 0x00, 0x01]),  # truncated -- lossy fallback
        ],
    )
    def test_udf_matches_reference_implementation(self, spark_session, value_bytes):
        from kindling.entity_provider_eventhub import _decode_amqp_headers_udf
        from pyspark.sql import Row
        from pyspark.sql.functions import col
        from pyspark.sql.types import (
            ArrayType,
            BinaryType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
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
            [Row(headers=[Row(key="x-opt-test", value=value_bytes)])], schema=schema
        )
        result = df.withColumn("headers", _decode_amqp_headers_udf(col("headers"))).collect()[0][
            "headers"
        ]

        assert result["x-opt-test"] == _decode_amqp_primitive(value_bytes)


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
