"""
Entity Provider Interface Composition

Defines the core interfaces for entity providers using interface composition pattern.
Providers can implement different combinations of interfaces based on their capabilities:

- BaseEntityProvider: Core interface (required) - batch read and metadata
- StreamableEntityProvider: Optional streaming read capability
- WritableEntityProvider: Optional batch write capability
- StreamWritableEntityProvider: Optional streaming write capability

Examples:
- Delta: Implements all 4 interfaces (full-featured)
- CSV: Implements only BaseEntityProvider (read-only batch)
- EventHub: Implements BaseEntityProvider + StreamableEntityProvider (streaming read)
- Memory: Implements all 4 interfaces (testing/temporary data)
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from .data_entities import EntityMetadata


class BaseEntityProvider(ABC):
    """
    Base interface for all entity providers.

    All providers MUST implement this interface to support basic batch read operations.
    """

    @abstractmethod
    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """
        Read entity as a batch DataFrame.

        Args:
            entity_metadata: Metadata describing the entity to read

        Returns:
            Batch DataFrame containing the entity data
        """
        pass

    @abstractmethod
    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """
        Check if the entity exists.

        Args:
            entity_metadata: Metadata describing the entity

        Returns:
            True if entity exists, False otherwise
        """
        pass

    # ===== Common Helper Methods (Concrete implementations) =====

    def _get_provider_config(self, entity_metadata: EntityMetadata) -> Dict[str, Any]:
        """
        Extract configuration from entity tags with type conversion.

        Returns ALL entity tags (not just provider.*), with provider.* tags
        having their prefix stripped for convenience.

        Example:
            Input tags: {
                "provider.path": "/data/sales.csv",
                "provider.header": "true",
                "region": "us-west",
                "pii": "true"
            }
            Returns: {
                "path": "/data/sales.csv",    # provider.* prefix stripped
                "header": True,                # type converted
                "region": "us-west",           # non-provider tags included
                "pii": True                    # type converted
            }

        Args:
            entity_metadata: Entity metadata with tags

        Returns:
            Dictionary with all tags, type-converted, provider.* prefix stripped
        """
        config = {}

        # Add all non-provider tags as-is (with type conversion)
        for key, value in entity_metadata.tags.items():
            if not key.startswith("provider."):
                config[key] = self._convert_tag_type(value)

        # Add provider tags with prefix stripped (with type conversion)
        for key, value in entity_metadata.tags.items():
            if key.startswith("provider."):
                config_key = key[9:]  # Remove 'provider.' prefix
                config[config_key] = self._convert_tag_type(value)

        return config

    def _convert_tag_type(self, value: str) -> Any:
        """
        Convert string tag values to appropriate Python types.

        Args:
            value: String value from entity tags

        Returns:
            Converted value (bool for "true"/"false", int for digits, str otherwise)
        """
        if isinstance(value, str):
            if value.lower() in ("true", "false"):
                return value.lower() == "true"
            elif value.isdigit():
                return int(value)
        return value


class StreamableEntityProvider(ABC):
    """
    Optional interface for providers that support streaming reads.

    Providers implementing this interface can read entities as streaming DataFrames,
    enabling real-time data processing patterns.
    """

    @abstractmethod
    def read_entity_as_stream(
        self,
        entity_metadata: EntityMetadata,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> DataFrame:
        """
        Read entity as a streaming DataFrame.

        Args:
            entity_metadata: Metadata describing the entity to read
            format: Optional format override (e.g., "delta", "eventhubs")
            options: Optional format-specific options

        Returns:
            Streaming DataFrame containing the entity data
        """
        pass


class WritableEntityProvider(ABC):
    """
    Optional interface for providers that support batch writes.

    Providers implementing this interface can write DataFrames to entities,
    supporting both full writes and append operations.
    """

    @abstractmethod
    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """
        Write DataFrame to entity (overwrites existing data).

        Args:
            df: DataFrame to write
            entity_metadata: Metadata describing the destination entity
        """
        pass

    @abstractmethod
    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """
        Append DataFrame to entity (preserves existing data).

        Args:
            df: DataFrame to append
            entity_metadata: Metadata describing the destination entity
        """
        pass


class StreamWritableEntityProvider(ABC):
    """
    Optional interface for providers that support streaming writes.

    Providers implementing this interface can write streaming DataFrames to entities,
    enabling continuous data ingestion patterns.
    """

    @abstractmethod
    def append_as_stream(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        checkpoint_location: str,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> StreamingQuery:
        """
        Append streaming DataFrame to entity.

        Args:
            df: Streaming DataFrame to write
            entity_metadata: Metadata describing the destination entity
            checkpoint_location: Path for streaming checkpoint
            format: Optional format override
            options: Optional format-specific options

        Returns:
            StreamingQuery object for monitoring and control
        """
        pass


# Type aliases for checking capabilities
def is_streamable(provider: BaseEntityProvider) -> bool:
    """Check if provider supports streaming reads."""
    return isinstance(provider, StreamableEntityProvider)


def is_writable(provider: BaseEntityProvider) -> bool:
    """Check if provider supports batch writes."""
    return isinstance(provider, WritableEntityProvider)


def is_stream_writable(provider: BaseEntityProvider) -> bool:
    """Check if provider supports streaming writes."""
    return isinstance(provider, StreamWritableEntityProvider)
