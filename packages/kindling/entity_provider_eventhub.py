"""
Azure Event Hub Entity Provider

Entity provider for Azure Event Hubs using the Event Hubs Spark connector.
Supports both batch and streaming reads.
"""

from typing import Any, Dict, Optional

from injector import inject
from pyspark.sql import DataFrame

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
    - Fabric/Synapse: Event Hub connector pre-installed ✅
    - Databricks: May require adding azure-eventhubs-spark package

    Provider configuration options (via entity tags with 'provider.' prefix):
    - provider.eventhub.connectionString: Event Hub connection string (required)
    - provider.eventhub.name: Event Hub name (required)
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

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider, config_service: ConfigService):
        self.logger = logger_provider.get_logger("EventHubEntityProvider")
        self.config_service = config_service
        self.spark = get_or_create_spark_session()
        self.platform = self.config_service.get("platform", "fabric").lower()

    def _get_provider_config(self, entity_metadata: EntityMetadata) -> Dict[str, Any]:
        """
        Extract provider configuration from entity tags.

        Looks for tags with 'provider.' prefix and converts them to a config dict.

        Args:
            entity_metadata: Entity metadata

        Returns:
            Dict with provider configuration (keys without 'provider.' prefix)
        """
        config = {}
        for key, value in entity_metadata.tags.items():
            if key.startswith("provider."):
                config_key = key[9:]  # Remove 'provider.' prefix
                # Convert string values to appropriate types
                if value.isdigit():
                    config[config_key] = int(value)
                else:
                    config[config_key] = value
        return config

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

        # Build configuration
        eh_config = {
            "eventhubs.connectionString": connection_string,
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
            eh_config = self._build_eventhub_config(config)

            # Log configuration (without sensitive data)
            safe_config = {
                k: v for k, v in eh_config.items() if "connectionString" not in k.lower()
            }
            self.logger.debug(f"Event Hub configuration: {safe_config}")

            # Read from Event Hub (batch)
            df = self.spark.read.format("eventhubs").options(**eh_config).load()

            self.logger.info(
                f"Successfully read Event Hub entity '{entity_metadata.entityid}' (batch): {len(df.columns)} columns"
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
            eh_config = self._build_eventhub_config(config)

            # Log configuration (without sensitive data)
            safe_config = {
                k: v for k, v in eh_config.items() if "connectionString" not in k.lower()
            }
            self.logger.debug(f"Event Hub streaming configuration: {safe_config}")

            # Read from Event Hub (streaming)
            stream_df = self.spark.readStream.format("eventhubs").options(**eh_config).load()

            self.logger.info(
                f"Successfully created Event Hub stream for entity '{entity_metadata.entityid}'"
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
        Check if Event Hub connection is valid.

        Note: This performs a lightweight check by attempting to create a reader.
        It does not verify message availability.

        Args:
            entity_metadata: Entity metadata with tags containing provider config

        Returns:
            True if Event Hub configuration is valid, False otherwise
        """
        config = self._get_provider_config(entity_metadata)

        try:
            # Validate configuration can be built
            eh_config = self._build_eventhub_config(config)

            # Try to create a reader (this validates connection string format)
            self.spark.read.format("eventhubs").options(**eh_config).load()

            self.logger.debug(
                f"Event Hub entity '{entity_metadata.entityid}' configuration is valid"
            )
            return True

        except Exception as e:
            self.logger.debug(f"Event Hub entity '{entity_metadata.entityid}' check failed: {e}")
            return False
