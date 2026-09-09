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
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Union

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from .data_entities import EntityMetadata

#: Provider-neutral read option used by declarative engines when evaluating
#: provider-owned streaming sources. Providers may ignore it.
DECLARATIVE_SOURCE_OPTION = "declarativeSource"


@dataclass(frozen=True)
class SourceValidationIssue:
    """Secret-safe validation issue returned by a declarable source provider."""

    tag: str
    constraint: str
    remediation: str = ""

    def __str__(self) -> str:
        detail = f"tag '{self.tag}': {self.constraint}"
        if self.remediation:
            detail = f"{detail} ({self.remediation})"
        return detail


@dataclass(frozen=True)
class PreprocessingSpec:
    """Secret-safe description of provider-owned source preprocessing."""

    mode: str
    amqp_headers: bool = False
    kafka_headers_included: bool = False


@dataclass(frozen=True)
class StreamingSourceSpec:
    """Inert, secret-safe declaration metadata for a provider-owned stream."""

    provider_type: str
    source_format: str
    source_identity: str
    supported_option_names: Tuple[str, ...] = ()
    applied_option_names: Tuple[str, ...] = ()
    preprocessing: Optional[PreprocessingSpec] = None
    validation_issues: Tuple[SourceValidationIssue, ...] = ()

    @property
    def is_valid(self) -> bool:
        """Whether provider-side declaration validation found no issues."""
        return not self.validation_issues


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

    @staticmethod
    def _extract_prefixed_options(config: Dict[str, Any], prefix: str) -> Dict[str, str]:
        """
        Extract config entries under `{prefix}.` as connector options, prefix stripped.

        For generic passthrough to an underlying connector (Spark reader/writer
        options) that provider-specific config doesn't otherwise name explicitly --
        e.g. provider.kafka.includeHeaders=true in entity tags becomes
        config["kafka.includeHeaders"] = True via _get_provider_config(), and
        _extract_prefixed_options(config, "kafka") returns
        {"includeHeaders": "true"} ready to merge into a Kafka options dict.

        Args:
            config: Config dict, e.g. from _get_provider_config()
            prefix: Prefix to match, without trailing dot (e.g. "kafka")

        Returns:
            Dict of stripped-key -> stringified-value for every entry under the prefix
        """
        full_prefix = f"{prefix}."

        def _stringify(value: Any) -> str:
            if isinstance(value, bool):
                return "true" if value else "false"
            return str(value)

        return {
            key[len(full_prefix) :]: _stringify(value)
            for key, value in config.items()
            if key.startswith(full_prefix) and len(key) > len(full_prefix)
        }


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


class DeclarableStreamingSource(ABC):
    """Declaration-only capability for provider-owned streaming sources.

    Implementations must also implement :class:`StreamableEntityProvider`.
    Building a spec must be inert: no Spark read, network call, JVM call,
    DataFrame creation, or secret lookup. The returned spec carries option
    names and structural metadata only, never connector option values.
    """

    @abstractmethod
    def streaming_source_spec(self, entity_metadata: EntityMetadata) -> StreamingSourceSpec:
        """Return secret-safe declaration metadata and validation issues."""
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
    ) -> "Union[DataStreamWriter, StreamingQuery]":
        """
        Append streaming DataFrame to entity.

        Args:
            df: Streaming DataFrame to write
            entity_metadata: Metadata describing the destination entity
            checkpoint_location: Path for streaming checkpoint
            format: Optional format override
            options: Optional format-specific options

        Returns:
            Either an **unstarted** ``DataStreamWriter`` the caller
            finalizes (``toTable(name)`` for catalog sinks, ``start(path)``
            /``start()`` otherwise — how ``SimplePipeStreamStarter``
            resolves the destination), or an already-started
            ``StreamingQuery`` when the provider resolves the destination
            itself (e.g. the memory provider). Contrast with
            ``StreamMergeableEntityProvider.merge_as_stream``, which always
            starts the query.
        """
        pass


class StreamMergeableEntityProvider(ABC):
    """
    Optional interface for providers that can merge a streaming DataFrame
    into an entity.

    Unlike ``append_as_stream`` — which returns an unstarted writer for the
    caller to finish (``toTable``/``start``) — a stream merge is executed
    per micro-batch (e.g. Spark ``foreachBatch`` driving the provider's
    batch merge), so the provider starts the query itself and returns the
    running ``StreamingQuery``. The merge semantics (SCD1 upsert, SCD2
    staged updates, ...) are the provider's: the same rules that govern its
    batch ``merge_to_entity`` apply to every micro-batch.
    """

    @abstractmethod
    def merge_as_stream(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        checkpoint_location: str,
        options: Optional[dict] = None,
    ) -> StreamingQuery:
        """
        Merge streaming DataFrame into entity, one micro-batch at a time.

        Args:
            df: Streaming DataFrame to merge
            entity_metadata: Metadata describing the destination entity;
                its merge/business keys define the match condition
            checkpoint_location: Path for streaming checkpoint
            options: Optional provider-specific options (e.g. trigger config)

        Returns:
            The started StreamingQuery for monitoring and control
        """
        pass


class ReplaceWritableEntityProvider(ABC):
    """
    Optional interface for providers that can atomically replace an entity's
    contents.

    This is the materialization contract for derived datasets
    (``dataset.kind: derived``): the entity's contents are a pure function
    of its inputs, so a write replaces rather than evolves. The replacement
    must be atomic — readers see the old contents until the swap commits —
    and idempotent: replaying the same batch converges to the same table.

    The replacement scope comes from the entity's own declaration
    (``derived.replace_keys``): unset means the whole table; set means only
    the slices present in the incoming DataFrame (its distinct values of
    those columns) are swapped.
    """

    @abstractmethod
    def replace_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """
        Atomically replace entity contents (full table or declared slices).

        Args:
            df: DataFrame holding the new contents
            entity_metadata: Metadata describing the destination entity;
                its tags declare the replacement scope
        """
        pass


class DestinationEnsuringProvider(ABC):
    """
    Optional interface for providers that can ensure a write destination exists.

    This is intentionally separated from write interfaces so it remains an opt-in
    capability: most sinks don't need pre-creation, and some platforms forbid DDL.
    """

    @abstractmethod
    def ensure_destination(self, entity_metadata: EntityMetadata) -> None:
        """Ensure the destination exists (provider-specific semantics)."""
        pass


class IncrementalReadableEntityProvider(ABC):
    """
    Optional interface for providers that can read only what changed since a
    watermark cursor.

    The cursor is an **opaque string the provider defines and interprets** —
    the watermark framework stores and returns it verbatim, never inspects
    it. A Delta provider encodes a table version; a REST provider might
    encode the max ``updated_at``/``created_at`` timestamp it has served; a
    queue-backed provider might encode offsets as JSON. All comparison
    semantics (inclusive vs. exclusive boundaries, same-timestamp
    collisions, lookback/overlap windows for late-arriving records, clock
    skew of a remote system) are the provider's policy, configured per
    entity via tags — not the framework's.

    Contract:

    - ``read_entity_changes(entity, None)`` is the initial load: return all
      current data and the cursor covering it.
    - The returned cursor must cover **exactly the data in the returned
      DataFrame** at the time of the call. If the DataFrame is lazy, bound
      it (e.g. Delta's ``endingVersion``) or materialize eagerly (typical
      for REST responses) so later-arriving data cannot silently ride along
      uncovered.
    - ``(None, None)`` means no new data.
    - Consumers persist the cursor only after the derived output is durably
      written, and may re-read the same slice after a failure — providers
      must tolerate at-least-once delivery (downstream merges are
      idempotent by key).
    """

    @abstractmethod
    def read_entity_changes(
        self, entity_metadata: EntityMetadata, cursor: Optional[str]
    ) -> "tuple[Optional[DataFrame], Optional[str]]":
        """Read changes after ``cursor``; return ``(df, new_cursor)``."""
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


def is_stream_mergeable(provider: BaseEntityProvider) -> bool:
    """Check if provider supports streaming merges."""
    return isinstance(provider, StreamMergeableEntityProvider)


def is_replace_writable(provider: BaseEntityProvider) -> bool:
    """Check if provider supports atomic replace writes (derived datasets)."""
    return isinstance(provider, ReplaceWritableEntityProvider)


def can_ensure_destination(provider: BaseEntityProvider) -> bool:
    """Check if provider supports destination ensuring."""
    return isinstance(provider, DestinationEnsuringProvider)


def is_incremental_readable(provider: BaseEntityProvider) -> bool:
    """Check if provider supports cursor-based incremental reads."""
    return isinstance(provider, IncrementalReadableEntityProvider)


def unwrap_provider(provider: Any, max_depth: int = 8) -> Any:
    """Return the inner provider behind decorator wrappers such as SDP's guard."""
    current = provider
    seen = set()
    for _ in range(max_depth):
        marker = id(current)
        if marker in seen:
            break
        seen.add(marker)
        inner = getattr(current, "_inner", None)
        if inner is None:
            break
        current = inner
    return current


def is_declarable_streaming_source(provider: Any) -> bool:
    """Check if provider can declare and evaluate a streaming source."""
    unwrapped = unwrap_provider(provider)
    return isinstance(unwrapped, DeclarableStreamingSource) and isinstance(
        unwrapped, StreamableEntityProvider
    )
