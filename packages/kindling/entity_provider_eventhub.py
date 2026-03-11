"""Azure Event Hub entity provider with Event Hubs and Kafka transports."""

from typing import Any, Dict, Optional

from injector import inject
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import MapType, StringType

from .data_entities import EntityMetadata
from .entity_provider import BaseEntityProvider, StreamableEntityProvider
from .injection import GlobalInjector
from .spark_config import ConfigService, get_or_create_spark_session
from .spark_log_provider import PythonLoggerProvider


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
        configured_transport = str(
            provider_config.get("transport", self.TRANSPORT_AUTO) or self.TRANSPORT_AUTO
        ).strip().lower()

        if configured_transport not in {
            self.TRANSPORT_AUTO,
            self.TRANSPORT_EVENTHUBS,
            self.TRANSPORT_KAFKA,
        }:
            raise ValueError(
                "Event Hub provider transport must be one of: auto, eventhubs, kafka"
            )

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
            .withColumn(
                "systemProperties", lit(None).cast(MapType(StringType(), StringType()))
            )
        )

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
