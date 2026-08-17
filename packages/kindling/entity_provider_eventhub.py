"""Azure Event Hub entity provider with Event Hubs and Kafka transports."""

import struct as _struct
from typing import Any, Callable, Dict, Optional

from injector import inject
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.functions import decode as spark_decode
from pyspark.sql.functions import expr
from pyspark.sql.functions import hex as spark_hex
from pyspark.sql.functions import lit, map_from_entries, struct, substring
from pyspark.sql.functions import transform as sql_transform
from pyspark.sql.functions import udf, when
from pyspark.sql.types import ArrayType, BinaryType, MapType, StringType

from .data_entities import EntityMetadata
from .entity_provider import BaseEntityProvider, StreamableEntityProvider
from .injection import GlobalInjector
from .spark_config import ConfigService, get_or_create_spark_session
from .spark_log_provider import PythonLoggerProvider

# Avro single-object encoding (the Avro spec's standard, registry-free way to
# prefix a binary Avro payload with a schema identifier -- see
# https://avro.apache.org/docs/current/spec.html#single_object_encoding):
# a fixed 2-byte marker, then a 16-byte schema fingerprint, then the
# Avro-encoded body.
_AVRO_SINGLE_OBJECT_MARKER = bytes([0xC3, 0x01])
_AVRO_SINGLE_OBJECT_HEADER_LEN = 2 + 16


def _decode_amqp_primitive(data: Optional[bytes]) -> Optional[str]:
    """Decode a single AMQP 1.0 primitive-typed value into its string
    representation, per the AMQP 1.0 spec's type system (section 7.2:
    https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html#type-primitive).

    Event Hubs' Kafka protocol head surfaces AMQP message annotations/
    application properties (e.g. enqueue time, sequence number) as Kafka
    record headers whose VALUE bytes are still AMQP-primitive-encoded, not
    plain UTF-8 -- a blind UTF-8 decode of these produces garbage. This
    reads the leading type-constructor byte and decodes the following
    bytes per the standard AMQP fixed-width/variable-width primitive
    encodings (covers the common scalar types most annotations use: null,
    boolean, uint/int/ulong/long family, float, double, timestamp, and
    UTF-8/symbol/binary variable-width types).

    Falls back to a best-effort UTF-8 (lossy) decode of the raw bytes for
    any constructor byte not covered here (e.g. composite/described types,
    arrays, lists, maps -- not expected for simple annotation values) or
    malformed/truncated input, rather than raising -- this is a display/
    interpretation aid, not a strict protocol validator.
    """
    if not data:
        return None
    ctor = data[0]
    rest = data[1:]
    try:
        if ctor in (0x40,):  # null
            return None
        if ctor == 0x41:  # true
            return "true"
        if ctor == 0x42:  # false
            return "false"
        if ctor in (0x43, 0x44):  # uint0, ulong0
            return "0"
        if ctor == 0x50:  # ubyte
            return str(rest[0])
        if ctor in (0x51, 0x54, 0x55):  # byte, smallint, smalllong (signed 1 byte)
            return str(_struct.unpack(">b", rest[:1])[0])
        if ctor in (0x52, 0x53):  # smalluint, smallulong (unsigned 1 byte)
            return str(rest[0])
        if ctor == 0x56:  # boolean (1 byte: 0 = false, else true)
            return "true" if rest[0] else "false"
        if ctor == 0x60:  # ushort
            return str(_struct.unpack(">H", rest[:2])[0])
        if ctor == 0x61:  # short
            return str(_struct.unpack(">h", rest[:2])[0])
        if ctor == 0x70:  # uint
            return str(_struct.unpack(">I", rest[:4])[0])
        if ctor == 0x71:  # int
            return str(_struct.unpack(">i", rest[:4])[0])
        if ctor == 0x72:  # float (IEEE 754 binary32)
            return str(_struct.unpack(">f", rest[:4])[0])
        if ctor == 0x80:  # ulong
            return str(_struct.unpack(">Q", rest[:8])[0])
        if ctor == 0x81:  # long
            return str(_struct.unpack(">q", rest[:8])[0])
        if ctor == 0x82:  # double (IEEE 754 binary64)
            return str(_struct.unpack(">d", rest[:8])[0])
        if ctor == 0x83:  # timestamp: signed 8-byte ms since Unix epoch
            return str(_struct.unpack(">q", rest[:8])[0])
        if ctor == 0xA0:  # vbin8: 1-byte length + binary
            length = rest[0]
            return rest[1 : 1 + length].hex()
        if ctor == 0xA1:  # str8-utf8: 1-byte length + UTF-8
            length = rest[0]
            return rest[1 : 1 + length].decode("utf-8", "replace")
        if ctor == 0xA3:  # sym8: 1-byte length + ASCII symbol
            length = rest[0]
            return rest[1 : 1 + length].decode("ascii", "replace")
        if ctor == 0xB0:  # vbin32: 4-byte length + binary
            length = _struct.unpack(">I", rest[:4])[0]
            return rest[4 : 4 + length].hex()
        if ctor == 0xB1:  # str32-utf8: 4-byte length + UTF-8
            length = _struct.unpack(">I", rest[:4])[0]
            return rest[4 : 4 + length].decode("utf-8", "replace")
        if ctor == 0xB3:  # sym32: 4-byte length + ASCII symbol
            length = _struct.unpack(">I", rest[:4])[0]
            return rest[4 : 4 + length].decode("ascii", "replace")
    except (_struct.error, IndexError):
        pass
    # Unrecognized constructor or truncated payload: best-effort fallback
    # rather than failing the whole read over one header value.
    return data.decode("utf-8", "replace")


# Event Hubs' Kafka protocol head exposes its own AMQP-derived system
# properties (enqueue time, sequence number, offset, partition key,
# publisher, ...) as Kafka headers with this documented prefix -- see
# https://learn.microsoft.com/azure/event-hubs/apache-kafka-migration-guide.
# These are ALWAYS AMQP-encoded, so they always go through the best-effort
# decoder below (_decode_amqp_primitive) regardless of structure.
_AMQP_SYSTEM_PROPERTY_PREFIX = "x-opt-"

# Sentinel distinguishing "not AMQP-encoded" from a real decoded value of
# None (AMQP's own null type) in _try_decode_amqp_primitive_strict.
_NOT_AMQP = object()


def _try_decode_amqp_primitive_strict(data: Optional[bytes]):
    """Attempt to decode ``data`` as an AMQP 1.0 primitive, but only return a
    value when the encoding is STRUCTURALLY EXACT: the type constructor is
    recognized AND (for fixed-width types) the byte count matches exactly,
    or (for variable-width types) the declared length exactly accounts for
    every remaining byte, with zero leftover. Returns the ``_NOT_AMQP``
    sentinel otherwise.

    This is the opportunistic counterpart to ``_decode_amqp_primitive``
    (which is only ever applied to Event Hubs' own confirmed-AMQP ``x-opt-``
    system properties). Some upstream producers -- observed with Azure IoT
    Hub's Kafka-compatible endpoint -- AMQP-encode their OWN custom
    application headers too, not just the well-known system properties, so
    gating decoding on the ``x-opt-`` prefix alone leaves those headers
    showing raw AMQP framing bytes (e.g. a str8-utf8 value's 2-byte
    constructor+length prefix decoding as garbage before the correct text).
    Exact-length structural validation is what makes it safe to attempt this
    on ANY header without the prefix restriction: a plain UTF-8 value's
    first byte can coincidentally match a real AMQP type-constructor byte
    (e.g. ASCII 'a' is 0x61, AMQP's "short" 2-byte-int constructor), but for
    that FALSE match to also survive the exact-length check, the value's
    total remaining byte count would have to exactly equal that type's
    required width -- a coincidence that becomes vanishingly unlikely as
    values get longer than a couple of bytes.
    """
    if not data:
        return _NOT_AMQP
    ctor = data[0]
    rest = data[1:]
    try:
        if ctor == 0x40 and len(rest) == 0:  # null
            return None
        if ctor == 0x41 and len(rest) == 0:  # true
            return "true"
        if ctor == 0x42 and len(rest) == 0:  # false
            return "false"
        if ctor in (0x43, 0x44) and len(rest) == 0:  # uint0, ulong0
            return "0"
        if ctor == 0x50 and len(rest) == 1:  # ubyte
            return str(rest[0])
        if ctor in (0x51, 0x54, 0x55) and len(rest) == 1:  # byte/smallint/smalllong
            return str(_struct.unpack(">b", rest)[0])
        if ctor in (0x52, 0x53) and len(rest) == 1:  # smalluint, smallulong
            return str(rest[0])
        if ctor == 0x56 and len(rest) == 1:  # boolean
            return "true" if rest[0] else "false"
        if ctor == 0x60 and len(rest) == 2:  # ushort
            return str(_struct.unpack(">H", rest)[0])
        if ctor == 0x61 and len(rest) == 2:  # short
            return str(_struct.unpack(">h", rest)[0])
        if ctor == 0x70 and len(rest) == 4:  # uint
            return str(_struct.unpack(">I", rest)[0])
        if ctor == 0x71 and len(rest) == 4:  # int
            return str(_struct.unpack(">i", rest)[0])
        if ctor == 0x72 and len(rest) == 4:  # float
            return str(_struct.unpack(">f", rest)[0])
        if ctor == 0x80 and len(rest) == 8:  # ulong
            return str(_struct.unpack(">Q", rest)[0])
        if ctor == 0x81 and len(rest) == 8:  # long
            return str(_struct.unpack(">q", rest)[0])
        if ctor == 0x82 and len(rest) == 8:  # double
            return str(_struct.unpack(">d", rest)[0])
        if ctor == 0x83 and len(rest) == 8:  # timestamp
            return str(_struct.unpack(">q", rest)[0])
        if ctor == 0xA0 and len(rest) >= 1 and rest[0] == len(rest) - 1:  # vbin8
            return rest[1:].hex()
        if ctor == 0xA1 and len(rest) >= 1 and rest[0] == len(rest) - 1:  # str8-utf8
            return rest[1:].decode("utf-8")
        if ctor == 0xA3 and len(rest) >= 1 and rest[0] == len(rest) - 1:  # sym8
            return rest[1:].decode("ascii")
        if (
            ctor == 0xB0 and len(rest) >= 4 and _struct.unpack(">I", rest[:4])[0] == len(rest) - 4
        ):  # vbin32
            return rest[4:].hex()
        if (
            ctor == 0xB1 and len(rest) >= 4 and _struct.unpack(">I", rest[:4])[0] == len(rest) - 4
        ):  # str32-utf8
            return rest[4:].decode("utf-8")
        if (
            ctor == 0xB3 and len(rest) >= 4 and _struct.unpack(">I", rest[:4])[0] == len(rest) - 4
        ):  # sym32
            return rest[4:].decode("ascii")
    except (_struct.error, UnicodeDecodeError, IndexError):
        return _NOT_AMQP
    return _NOT_AMQP


def _decode_amqp_headers_map(headers) -> Optional[Dict[str, Optional[str]]]:
    """Decode a whole Kafka ``headers`` array (list of key/binary-value Row
    entries) into a ``dict``. Keys under the ``x-opt-`` system-property
    convention (see ``_AMQP_SYSTEM_PROPERTY_PREFIX``) are always
    best-effort AMQP-decoded (``_decode_amqp_primitive``), since Event Hubs
    guarantees those are genuinely AMQP-encoded. Every other key is
    opportunistically checked against the STRICT structural decoder
    (``_try_decode_amqp_primitive_strict``) first -- covering producers
    (e.g. Azure IoT Hub) that AMQP-encode their own custom headers too --
    and falls back to a plain UTF-8 decode when that check doesn't confirm
    a structurally exact AMQP encoding."""
    if headers is None:
        return None
    decoded: Dict[str, Optional[str]] = {}
    for entry in headers:
        key = entry["key"]
        value = entry["value"]
        if key is not None and key.startswith(_AMQP_SYSTEM_PROPERTY_PREFIX):
            decoded[key] = _decode_amqp_primitive(value)
            continue
        if value is None:
            decoded[key] = None
            continue
        strict_result = _try_decode_amqp_primitive_strict(value)
        if strict_result is not _NOT_AMQP:
            decoded[key] = strict_result
        else:
            decoded[key] = value.decode("utf-8", "replace")
    return decoded


_decode_amqp_headers_udf = udf(_decode_amqp_headers_map, MapType(StringType(), StringType()))


def _flatten_kafka_headers(df: DataFrame, amqp_headers: bool = False) -> DataFrame:
    """Flatten Kafka's ``headers`` column (``array<struct<key,value:binary>>``,
    present only when ``provider.kafka.includeHeaders=true``) into a
    ``map<string,string>`` -- the same shape as the provider's existing
    ``properties``/``systemProperties`` metadata columns. Transport-level,
    not payload-codec-specific, so both preprocess modes apply it.

    ``amqp_headers=True`` (``provider.amqp_headers: true``) decodes
    ``x-opt-``-prefixed header values as an AMQP 1.0 primitive (see
    ``_decode_amqp_primitive``/``_AMQP_SYSTEM_PROPERTY_PREFIX``) instead of
    plain UTF-8 -- needed because Event Hubs' Kafka protocol head surfaces
    its own AMQP system-property annotations (e.g. enqueue time, sequence
    number) under that prefix with AMQP-primitive-encoded values, which a
    plain UTF-8 decode would corrupt into garbage. Headers outside that
    prefix are also checked, but only decoded when the encoding is
    structurally exact (``_try_decode_amqp_primitive_strict``) -- some
    producers (observed with Azure IoT Hub's Kafka-compatible endpoint)
    AMQP-encode their own custom headers too, not just Event Hubs' system
    properties. Either way the output stays a uniform
    ``map<string,string>``; interpreting what a given header NAME means
    (e.g. that it should be parsed further as a timestamp) is the
    consuming pipe's job, not this provider's.

    The two modes use different execution strategies: plain UTF-8 uses
    native Catalyst higher-order functions (``transform``/``map_from_entries``),
    but a Python UDF cannot be nested inside a higher-order function's
    lambda (Catalyst rejects it: "Cannot evaluate expression" at runtime,
    not at planning time) -- so the AMQP path applies one UDF to the whole
    headers array at once instead.

    No-op when ``headers`` is absent (native Event Hubs connector, or Kafka
    without ``includeHeaders``) or already a different shape.
    """
    if "headers" in df.columns and isinstance(df.schema["headers"].dataType, ArrayType):
        if amqp_headers:
            df = df.withColumn("headers", _decode_amqp_headers_udf(col("headers")))
        else:
            df = df.withColumn(
                "headers",
                map_from_entries(
                    sql_transform(
                        col("headers"),
                        lambda entry: struct(
                            entry["key"].alias("key"),
                            spark_decode(entry["value"], "UTF-8").alias("value"),
                        ),
                    )
                ),
            )
    return df


def _preprocess_kafka(df: DataFrame, amqp_headers: bool = False) -> DataFrame:
    """``provider.preprocess: kafka`` -- for text-payload producers (JSON,
    delimited text, etc.): decode the binary ``body`` to UTF-8 text and
    flatten Kafka headers. Do NOT use this for binary-schema payloads
    (Avro, Protobuf) -- decoding non-text bytes as UTF-8 is lossy and will
    corrupt them; use ``avro`` for Avro single-object-encoded payloads.

    No-op on ``body`` when it isn't binary, so applying this twice, or to a
    DataFrame that already has a text ``body``, is safe.
    """
    if "body" in df.columns and isinstance(df.schema["body"].dataType, BinaryType):
        df = df.withColumn("body", spark_decode(col("body"), "UTF-8"))
    return _flatten_kafka_headers(df, amqp_headers=amqp_headers)


def _preprocess_avro(df: DataFrame, amqp_headers: bool = False) -> DataFrame:
    """``provider.preprocess: avro`` -- for Avro single-object-encoded
    payloads (2-byte marker + 16-byte schema fingerprint + Avro-encoded
    body; see ``_AVRO_SINGLE_OBJECT_MARKER``). Extracts the fingerprint into
    a new ``avro_schema_fingerprint`` column (hex string) and strips the
    18-byte header from ``body``, leaving the Avro-encoded bytes as binary --
    this framework does not deserialize Avro (no schema-registry client
    exists here), so full decoding is the consuming pipe's job once it
    resolves the fingerprint to a schema.

    Rows whose ``body`` does not start with the single-object marker are
    left untouched (``avro_schema_fingerprint`` is null, ``body``
    unstripped) rather than corrupted -- not every row need be conforming
    for this to be a safe, partial win.
    """
    if "body" not in df.columns or not isinstance(df.schema["body"].dataType, BinaryType):
        return _flatten_kafka_headers(df, amqp_headers=amqp_headers)

    is_single_object = substring(col("body"), 1, 2) == lit(_AVRO_SINGLE_OBJECT_MARKER)
    # Spark SQL's substring(bin, pos) 2-arg form means "from pos to the end"
    # -- the Python substring() function requires a literal int length, so
    # a variable, expression-based length (total length minus the header)
    # needs the SQL expression form instead.
    rest_of_body = expr(f"substring(body, {_AVRO_SINGLE_OBJECT_HEADER_LEN + 1})")
    df = df.withColumn(
        "avro_schema_fingerprint",
        when(is_single_object, spark_hex(substring(col("body"), 3, 16))).otherwise(
            lit(None).cast(StringType())
        ),
    ).withColumn(
        "body",
        when(is_single_object, rest_of_body).otherwise(col("body")),
    )
    return _flatten_kafka_headers(df, amqp_headers=amqp_headers)


_PREPROCESS_MODES: Dict[str, Callable[..., DataFrame]] = {
    "kafka": _preprocess_kafka,
    "avro": _preprocess_avro,
}


@GlobalInjector.singleton_autobind()
class EventHubEntityProvider(BaseEntityProvider, StreamableEntityProvider):
    """
    Azure Event Hub entity provider (read-only batch and streaming operations).

    Implements BaseEntityProvider and StreamableEntityProvider interfaces for reading
    from Azure Event Hubs. Does not support write operations (Event Hubs are typically
    used as streaming sources).

    **Platform Support:**
    - Fabric/Synapse: Event Hubs Spark connector is the default
    - Databricks: Kafka transport is the default

    Provider configuration options (via entity tags with 'provider.' prefix):
    - provider.eventhub.connectionString: Event Hub connection string (required)
    - provider.eventhub.name: Event Hub name (required)
    - provider.transport: "auto" (default), "eventhubs", or "kafka"
    - provider.startingPosition: Where to start reading (default: "latest")
      Values: "earliest", "latest", or JSON offset specification
    - provider.eventhub.consumerGroup: Consumer group (default: "$Default")
    - provider.maxEventsPerTrigger: Max events per micro-batch (streaming only)
    - provider.receiverTimeout: Receiver timeout in milliseconds
    - provider.operationTimeout: Operation timeout in milliseconds
    - provider.preprocess: Opt-in payload mode, applied identically to batch
      and streaming reads after transport normalization. Unset by default,
      so raw transport-shaped output (binary body, Kafka header arrays) is
      unaffected -- a bronze entity capturing the untouched wire format
      simply never sets this tag. Supported values:
        - "kafka": for text payloads (JSON, delimited text) -- decodes the
          binary body to UTF-8 text and flattens Kafka headers into a
          map<string,string>.
        - "avro": for Avro single-object-encoded payloads -- extracts the
          16-byte schema fingerprint into an avro_schema_fingerprint column
          and strips it from body (still binary; this framework does not
          deserialize Avro), and flattens Kafka headers the same as "kafka".
    - provider.amqp_headers: Opt-in boolean (default false), only relevant
      alongside provider.preprocess. Event Hubs' Kafka protocol head
      surfaces its own AMQP message annotations (e.g. enqueue time,
      sequence number) as x-opt-prefixed Kafka headers whose values are
      still AMQP-1.0-primitive-encoded, not plain UTF-8 -- true decodes
      those x-opt- headers per the AMQP primitive type system instead of
      blind UTF-8 (which would otherwise corrupt them into garbage).
      Headers outside that prefix are also checked, but only decoded when
      the encoding is structurally exact (the declared length exactly
      accounts for every remaining byte) -- some producers (observed with
      Azure IoT Hub's Kafka-compatible endpoint) AMQP-encode their own
      custom headers too, not just Event Hubs' system properties. Output
      stays map<string,string> either way; interpreting a given header
      NAME's meaning (e.g. "this one is a timestamp") is still the
      consuming pipe's job.

    Example entity definition:
    ```python
    @DataEntities.entity(
        entityid="stream.user_events",
        name="user_events",
        partition_columns=[],
        merge_columns=["event_id"],
        tags={
            "provider_type": "eventhub",
            "provider.eventhub.connectionString": "Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...",
            "provider.eventhub.name": "user-events-hub",
            "provider.startingPosition": "latest",
            "provider.eventhub.consumerGroup": "$Default"
        },
        schema=None
    )
    ```

    **Event Hub Message Format:**
    Events are returned with the following schema:
    - body: bytes (event payload)
    - partition: string
    - offset: string
    - sequenceNumber: long
    - enqueuedTime: timestamp
    - publisher: string
    - partitionKey: string
    - properties: map<string, string>
    - systemProperties: map<string, string>

    Use `.selectExpr("cast(body as string) as json")` to parse JSON payloads.
    """

    TRANSPORT_AUTO = "auto"
    TRANSPORT_EVENTHUBS = "eventhubs"
    TRANSPORT_KAFKA = "kafka"

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider, config_service: ConfigService):
        self.logger = logger_provider.get_logger("EventHubEntityProvider")
        self.config_service = config_service
        self.spark = get_or_create_spark_session()
        self.platform = str(
            self.config_service.get("kindling.platform.name", "fabric") or "fabric"
        ).lower()

    def resolve_transport(self, entity_metadata: EntityMetadata) -> str:
        """Resolve the runtime transport for an Event Hub entity."""
        config = self._get_provider_config(entity_metadata)
        return self._resolve_transport(config)

    def _resolve_transport(self, provider_config: dict) -> str:
        configured_transport = (
            str(provider_config.get("transport", self.TRANSPORT_AUTO) or self.TRANSPORT_AUTO)
            .strip()
            .lower()
        )

        if configured_transport not in {
            self.TRANSPORT_AUTO,
            self.TRANSPORT_EVENTHUBS,
            self.TRANSPORT_KAFKA,
        }:
            raise ValueError("Event Hub provider transport must be one of: auto, eventhubs, kafka")

        if configured_transport != self.TRANSPORT_AUTO:
            return configured_transport

        if self.platform == "databricks":
            return self.TRANSPORT_KAFKA

        return self.TRANSPORT_EVENTHUBS

    def _parse_connection_string(self, connection_string: str) -> dict:
        parts: dict[str, str] = {}
        for segment in connection_string.split(";"):
            if not segment or "=" not in segment:
                continue
            key, value = segment.split("=", 1)
            parts[key.strip()] = value.strip()

        required = ("Endpoint=", "SharedAccessKeyName=", "SharedAccessKey=")
        if not all(token in connection_string for token in required):
            raise ValueError("Event Hub connection string missing required segments")

        return parts

    def _with_entity_path(self, connection_string: str, eventhub_name: str) -> str:
        if "EntityPath=" in connection_string:
            return connection_string
        return f"{connection_string.rstrip(';')};EntityPath={eventhub_name}"

    def _build_eventhub_config(self, provider_config: dict) -> dict:
        """
        Build Event Hub configuration dictionary from provider_config.

        Args:
            provider_config: Entity provider configuration

        Returns:
            Dictionary of Event Hub options

        Raises:
            ValueError: If required configuration is missing
        """
        # Required fields
        connection_string = provider_config.get("eventhub.connectionString")
        eventhub_name = provider_config.get("eventhub.name")

        if not connection_string:
            raise ValueError(
                "Event Hub provider requires 'eventhub.connectionString' in provider_config"
            )

        if not eventhub_name:
            raise ValueError("Event Hub provider requires 'eventhub.name' in provider_config")

        encrypted_connection_string = self._encrypt_connection_string(connection_string)

        # Build configuration
        eh_config = {
            "eventhubs.connectionString": encrypted_connection_string,
            "eventhubs.eventHubName": eventhub_name,
        }

        # Optional fields
        starting_position = provider_config.get("startingPosition", "latest")
        consumer_group = provider_config.get("eventhub.consumerGroup", "$Default")

        # Handle starting position
        if starting_position == "earliest":
            eh_config["eventhubs.startingPosition"] = (
                '{"offset": "-1", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}'
            )
        elif starting_position == "latest":
            eh_config["eventhubs.startingPosition"] = (
                '{"offset": "@latest", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}'
            )
        else:
            # Assume it's a custom JSON offset specification
            eh_config["eventhubs.startingPosition"] = starting_position

        eh_config["eventhubs.consumerGroup"] = consumer_group

        # Additional optional parameters
        if "maxEventsPerTrigger" in provider_config:
            eh_config["eventhubs.maxEventsPerTrigger"] = str(provider_config["maxEventsPerTrigger"])

        if "receiverTimeout" in provider_config:
            eh_config["eventhubs.receiverTimeout"] = str(provider_config["receiverTimeout"])

        if "operationTimeout" in provider_config:
            eh_config["eventhubs.operationTimeout"] = str(provider_config["operationTimeout"])

        return eh_config

    def _build_kafka_config(self, provider_config: dict, *, streaming: bool) -> dict:
        connection_string = provider_config.get("eventhub.connectionString")
        eventhub_name = provider_config.get("eventhub.name")

        if not connection_string:
            raise ValueError(
                "Event Hub provider requires 'eventhub.connectionString' in provider_config"
            )

        if not eventhub_name:
            raise ValueError("Event Hub provider requires 'eventhub.name' in provider_config")

        parts = self._parse_connection_string(connection_string)
        namespace_host = parts["Endpoint"].replace("sb://", "", 1).rstrip("/")
        kafka_password = self._with_entity_path(connection_string, eventhub_name)
        escaped_password = kafka_password.replace("\\", "\\\\").replace('"', '\\"')

        kafka_config = {
            "kafka.bootstrap.servers": f"{namespace_host}:9093",
            "subscribe": eventhub_name,
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": (
                "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
                f'required username="$ConnectionString" password="{escaped_password}";'
            ),
            "failOnDataLoss": "false",
        }

        consumer_group = provider_config.get("eventhub.consumerGroup", "$Default")
        if consumer_group:
            kafka_config["kafka.group.id"] = str(consumer_group)

        starting_position = str(provider_config.get("startingPosition", "latest") or "latest")
        if starting_position not in {"earliest", "latest"}:
            raise ValueError(
                "Kafka transport supports only 'earliest' and 'latest' startingPosition values"
            )
        kafka_config["startingOffsets"] = starting_position

        if not streaming:
            kafka_config["endingOffsets"] = "latest"

        if "maxEventsPerTrigger" in provider_config:
            kafka_config["maxOffsetsPerTrigger"] = str(provider_config["maxEventsPerTrigger"])

        kafka_request_timeout = provider_config.get("operationTimeout")
        if kafka_request_timeout is not None:
            timeout_ms = str(kafka_request_timeout)
            kafka_config["kafka.request.timeout.ms"] = timeout_ms
            kafka_config["kafka.session.timeout.ms"] = timeout_ms

        # Generic passthrough: any provider.kafka.<option> tag is forwarded
        # verbatim as a Kafka connector option (prefix stripped), e.g.
        # provider.kafka.includeHeaders=true -> .option("includeHeaders", "true").
        # This covers any Kafka reader/writer option without each one
        # needing its own named mapping above.
        kafka_config.update(self._extract_prefixed_options(provider_config, "kafka"))

        return kafka_config

    def _build_source_config(self, provider_config: dict, *, streaming: bool) -> tuple[str, dict]:
        transport = self._resolve_transport(provider_config)
        if transport == self.TRANSPORT_KAFKA:
            return transport, self._build_kafka_config(provider_config, streaming=streaming)
        return transport, self._build_eventhub_config(provider_config)

    def _normalize_dataframe(self, df: DataFrame, transport: str) -> DataFrame:
        if transport != self.TRANSPORT_KAFKA or SparkContext._active_spark_context is None:
            return df

        return (
            df.withColumnRenamed("value", "body")
            .withColumnRenamed("timestamp", "enqueuedTime")
            .withColumn("sequenceNumber", lit(None).cast("long"))
            .withColumn("publisher", lit(None).cast("string"))
            .withColumn("partitionKey", lit(None).cast("string"))
            .withColumn("properties", lit(None).cast(MapType(StringType(), StringType())))
            .withColumn("systemProperties", lit(None).cast(MapType(StringType(), StringType())))
        )

    def _apply_preprocessing(
        self, df: DataFrame, entity_metadata: EntityMetadata, provider_config: dict
    ) -> DataFrame:
        """Apply the entity's opt-in ``provider.preprocess`` mode, if any
        (see ``_PREPROCESS_MODES``: "kafka" or "avro").

        Runs after transport-specific reading and ``_normalize_dataframe``,
        identically for batch and streaming reads, so the same mode always
        sees the same unified column shape regardless of transport. Entities
        that never set ``provider.preprocess`` are entirely unaffected --
        this is a no-op returning ``df`` unchanged -- preserving today's
        raw, transport-shaped output for anything that doesn't opt in (e.g.
        a bronze entity capturing the untouched wire format).

        ``provider_config`` is read from ``entity_metadata.tags`` via
        ``_get_provider_config`` at call time, i.e. after bootstrap's
        config-overlay and @secret resolution have already run (see
        ``kindling.bootstrap``) -- there is no separate, earlier read of
        this tag that could race ahead of that resolution.
        """
        mode = provider_config.get("preprocess")
        if not mode:
            return df

        preprocessor = _PREPROCESS_MODES.get(mode)
        if preprocessor is None:
            raise ValueError(
                f"Event Hub entity '{entity_metadata.entityid}' declares "
                f"provider.preprocess='{mode}', which is not a supported mode. "
                f"Supported values: {', '.join(sorted(_PREPROCESS_MODES))}."
            )

        amqp_headers = bool(provider_config.get("amqp_headers"))

        try:
            return preprocessor(df, amqp_headers=amqp_headers)
        except Exception as exc:
            raise RuntimeError(
                f"Event Hub entity '{entity_metadata.entityid}' preprocessing "
                f"'{mode}' failed: {type(exc).__name__}: {exc}"
            ) from exc

    def _encrypt_connection_string(self, connection_string: str) -> str:
        """
        Encrypt Event Hub connection string for Spark connector when possible.

        Spark Event Hubs connector expects an encrypted connection string value.
        If encryption helper is not available, return raw value as fallback.
        """
        # Already encrypted or non-standard format; leave as-is.
        if "Endpoint=" not in connection_string or "SharedAccessKey=" not in connection_string:
            return connection_string

        try:
            jvm = getattr(self.spark, "_jvm", None)
            if jvm is None:
                return connection_string
            encrypted = jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string)
            return str(encrypted)
        except Exception:
            self.logger.warning(
                "Event Hubs connection string encryption helper unavailable; using raw connection string"
            )
            return connection_string

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """
        Read Event Hub as batch DataFrame (snapshot of current messages).

        Note: Batch reads from Event Hubs may have limited retention.
        Streaming reads are typically preferred for Event Hubs.

        Args:
            entity_metadata: Entity metadata with tags containing Event Hub options

        Returns:
            DataFrame containing Event Hub messages

        Raises:
            ValueError: If required configuration is missing
            Exception: If Event Hub read fails
        """
        config = self._get_provider_config(entity_metadata)

        self.logger.info(f"Reading Event Hub entity '{entity_metadata.entityid}' (batch mode)")

        try:
            transport, source_config = self._build_source_config(config, streaming=False)

            # Log configuration (without sensitive data)
            safe_config = {
                k: v
                for k, v in source_config.items()
                if "connectionString" not in k.lower() and "jaas" not in k.lower()
            }
            self.logger.debug(
                f"Event Hub configuration: transport={transport} options={safe_config}"
            )

            df = self.spark.read.format(transport).options(**source_config).load()
            df = self._normalize_dataframe(df, transport)
            df = self._apply_preprocessing(df, entity_metadata, config)

            self.logger.info(
                "Successfully read Event Hub entity "
                f"'{entity_metadata.entityid}' (batch, transport={transport}): {len(df.columns)} columns"
            )

            return df

        except Exception as e:
            self.logger.error(
                f"Failed to read Event Hub entity '{entity_metadata.entityid}': {e}",
                include_traceback=True,
            )
            raise

    def read_entity_as_stream(
        self,
        entity_metadata: EntityMetadata,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> DataFrame:
        """
        Read Event Hub as streaming DataFrame.

        Args:
            entity_metadata: Entity metadata with tags containing Event Hub options
            format: Ignored (Event Hub format is always used)
            options: Optional additional Event Hub options (merged with provider config from tags)

        Returns:
            Streaming DataFrame containing Event Hub messages

        Raises:
            ValueError: If required configuration is missing
            Exception: If Event Hub stream read fails
        """
        config = self._get_provider_config(entity_metadata)

        # Merge with additional options if provided
        if options:
            config = {**config, **options}

        self.logger.info(f"Reading Event Hub entity '{entity_metadata.entityid}' (streaming mode)")

        try:
            transport, source_config = self._build_source_config(config, streaming=True)

            # Log configuration (without sensitive data)
            safe_config = {
                k: v
                for k, v in source_config.items()
                if "connectionString" not in k.lower() and "jaas" not in k.lower()
            }
            self.logger.debug(
                f"Event Hub streaming configuration: transport={transport} options={safe_config}"
            )

            stream_df = self.spark.readStream.format(transport).options(**source_config).load()
            stream_df = self._normalize_dataframe(stream_df, transport)
            stream_df = self._apply_preprocessing(stream_df, entity_metadata, config)

            self.logger.info(
                "Successfully created Event Hub stream for entity "
                f"'{entity_metadata.entityid}' (transport={transport})"
            )

            return stream_df

        except Exception as e:
            self.logger.error(
                f"Failed to read Event Hub stream '{entity_metadata.entityid}': {e}",
                include_traceback=True,
            )
            raise

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """
        Check if Event Hub configuration is valid.

        Event Hubs are streaming resources, so an "exists" check should avoid
        triggering a full batch load that can fail for transient runtime reasons
        unrelated to metadata validity.

        Args:
            entity_metadata: Entity metadata with tags containing provider config

        Returns:
            True if Event Hub configuration is valid, False otherwise
        """
        config = self._get_provider_config(entity_metadata)

        try:
            connection_string = str(config.get("eventhub.connectionString", ""))
            has_entity_path = "EntityPath=" in connection_string
            has_eventhub_name = bool(config.get("eventhub.name"))
            required_segments = ("Endpoint=", "SharedAccessKeyName=", "SharedAccessKey=")

            if not all(segment in connection_string for segment in required_segments):
                self.logger.warning(
                    f"Event Hub entity '{entity_metadata.entityid}' check failed: connection string missing required segments"
                )
                return False

            if not has_eventhub_name and not has_entity_path:
                self.logger.warning(
                    f"Event Hub entity '{entity_metadata.entityid}' check failed: missing event hub name and EntityPath"
                )
                return False

            # Validate config shape and connector options construction.
            self._build_source_config(config, streaming=False)

            self.logger.debug(
                f"Event Hub entity '{entity_metadata.entityid}' configuration is valid"
            )
            return True

        except Exception as e:
            self.logger.warning(f"Event Hub entity '{entity_metadata.entityid}' check failed: {e}")
            return False
