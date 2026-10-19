import dataclasses
import logging
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import MISSING, dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from kindling.config_patterns import ConfigPatternMatcher, TagRuleMatcher
from kindling.declaration_derivations import (
    DerivationError,
    EntityDerivation,
    apply_entity_derivation,
    derivation_entries,
    strip_derivation_keys,
)
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_log_provider import *
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_ENTITY_LOGGER = logging.getLogger("kindling.data_entities")

ROUTING_KEY_METHODS: tuple[str, ...] = ("hash", "concat")


# [implementer] add clear initialization error — TASK-20260430-001
class KindlingNotInitializedError(RuntimeError):
    """Raised when an entity or pipe decorator fires before initialize() is called."""


def _raise_if_not_initialized(decorator_name: str, module_kind: str) -> None:
    try:
        from kindling.platform_provider import PlatformServiceProvider
    except Exception as exc:
        raise KindlingNotInitializedError(
            f"A @{decorator_name} decorator fired before initialize() was called. "
            f"Call initialize() before importing {module_kind} modules. "
            "See your app.py register_all() for the correct order."
        ) from exc

    try:
        platform_service = GlobalInjector.get_injector().get(PlatformServiceProvider).get_service()
    except Exception:
        platform_service = None

    if platform_service is None:
        raise KindlingNotInitializedError(
            f"A @{decorator_name} decorator fired before initialize() was called. "
            f"Call initialize() before importing {module_kind} modules. "
            "See your app.py register_all() for the correct order."
        )


# [implementer] add tag-derived SCD configuration surface — TASK-20260429-001
@dataclass(frozen=True)
class SCDConfig:
    """Parsed SCD configuration derived from an entity's tags."""

    enabled: bool
    tracked_columns: Optional[List[str]]
    effective_from_column: str
    effective_to_column: str
    is_current_column: str
    current_entity_id: str
    routing_key_method: str
    close_on_missing: bool = False
    optimize_unchanged: bool = False
    # Declared-flow additions (see declarative_pipelines_engine.md, "SCD2
    # as a Declared Flow"):
    # sequence_by — ordering authority from a data column instead of
    # merge-time current_timestamp(). effective_from/effective_to carry
    # the sequence values; out-of-order rows (sequence not strictly later
    # than the current version's effective_from) are ignored.
    sequence_by: Optional[str] = None
    # source_kind — the input contract, stated explicitly:
    # "snapshot": each batch is the complete current state; absence of a
    #   key closes it (the explicit form of scd.close_on_missing).
    # "change_feed": rows are change events; absence means nothing.
    source_kind: str = "change_feed"
    # delete_when — change-feed only: a SQL predicate marking rows that
    # CLOSE the current version without inserting a new one.
    delete_when: Optional[str] = None
    # untracked_columns (scd.untracked) — the exclude-list counterpart of
    # tracked_columns; the two are mutually exclusive. Columns outside the
    # tracked set never count as a change: under SCD2 a change only to them
    # (and the sequence_by column) updates the current version in place;
    # under the default SCD1 merge a matched row whose only differences are
    # untracked is not rewritten. Parsed for non-SCD2 entities too.
    untracked_columns: Optional[List[str]] = None


@dataclass
class SqlSource:
    """Locates SQL text for a sql_entity — resolved once at registration time.

    Exactly one of ``inline``, ``resource``, or ``file`` must be provided.

    Args:
        inline:   A literal SQL string.
        resource: A ``"package:path/to/file.sql"`` reference resolved via
                  ``importlib.resources``.  The SQL file must be included as
                  package data in the installed wheel.
        file:     A filesystem path (absolute, or relative to the caller's
                  working directory).  Convenient during local development;
                  for deployment bundle the file as a package resource instead.
    """

    inline: Optional[str] = None
    resource: Optional[str] = None
    file: Optional[str] = None

    def __post_init__(self):
        provided = sum(x is not None for x in [self.inline, self.resource, self.file])
        if provided != 1:
            raise ValueError(
                "SqlSource requires exactly one of: inline, resource, file. "
                f"Got {provided} argument(s)."
            )

    def load(self) -> str:
        """Return the SQL text, reading from the source if necessary."""
        if self.inline is not None:
            return self.inline
        if self.resource is not None:
            package, _, path = self.resource.partition(":")
            if not package or not path:
                raise ValueError(
                    f"SqlSource resource must be 'package:path/to/file.sql', got: {self.resource!r}"
                )
            import importlib.resources

            return importlib.resources.files(package).joinpath(path).read_text(encoding="utf-8")
        # file path
        return Path(self.file).read_text(encoding="utf-8")


class EntityPathLocator(ABC):
    @abstractmethod
    def get_table_path(self, entity):
        pass


class EntityNameMapper(ABC):
    @abstractmethod
    def get_table_name(self, entity):
        pass


class EntityProvider(ABC):
    """Abstract base for entity storage operations.

    Implementations MUST emit these signals:
        - entity.before_ensure_table: Before table creation check
        - entity.after_ensure_table: After table created/verified
        - entity.ensure_failed: Table creation fails
        - entity.before_merge: Before merge operation
        - entity.after_merge: After merge completes
        - entity.merge_failed: Merge fails
        - entity.before_append: Before append operation
        - entity.after_append: After append completes
        - entity.append_failed: Append fails
        - entity.before_write: Before write operation
        - entity.after_write: After write completes
        - entity.write_failed: Write fails
        - entity.before_read: Before read operation
        - entity.after_read: After read completes
    """

    EMITS = [
        "entity.before_ensure_table",
        "entity.after_ensure_table",
        "entity.ensure_failed",
        "entity.before_merge",
        "entity.after_merge",
        "entity.merge_failed",
        "entity.before_append",
        "entity.after_append",
        "entity.append_failed",
        "entity.before_write",
        "entity.after_write",
        "entity.write_failed",
        "entity.before_read",
        "entity.after_read",
    ]

    @abstractmethod
    def ensure_entity_table(self, entity):
        pass

    @abstractmethod
    def check_entity_exists(self, entity):
        pass

    @abstractmethod
    def merge_to_entity(self, df, entity):
        pass

    @abstractmethod
    def append_to_entity(self, df, entity):
        pass

    @abstractmethod
    def read_entity(self, entity):
        pass

    @abstractmethod
    def read_entity_as_stream(self, entity):
        pass

    @abstractmethod
    def read_entity_since_version(self, entity, since_version, end_version=None):
        pass

    @abstractmethod
    def write_to_entity(self, df, entity):
        pass

    @abstractmethod
    def get_entity_version(self, entity):
        pass

    @abstractmethod
    def append_as_stream(self, entity, df, checkpointLocation, format=None, options=None):
        pass


@dataclass
class EntityMetadata:
    entityid: str
    name: str
    merge_columns: List[str]
    tags: Dict[str, str]
    schema: Any
    # Optional: physical file partitioning columns (Delta partitionBy).
    # If omitted, defaults to no file partitioning.
    partition_columns: List[str] = field(default_factory=list)
    # Optional: Databricks liquid clustering (or best-effort on other platforms).
    # If set, Delta writes should generally avoid file partitioning (partition_columns).
    cluster_columns: List[str] = field(default_factory=list)
    # Optional: resolved SQL body for SQL-defined (view) entities.
    # Set by DataEntities.sql_entity(); None for Delta entities.
    sql: Optional[str] = None

    @property
    def is_sql_entity(self) -> bool:
        return self.sql is not None


def _parse_column_list_tag(tags: Dict[str, Any], key: str) -> Optional[List[str]]:
    """Parse a comma-separated column-list tag; None when absent or empty."""
    raw = str(tags.get(key) or "").strip()
    if not raw:
        return None
    columns = [column.strip() for column in raw.split(",") if column.strip()]
    return columns or None


def scd_config_from_tags(entity: EntityMetadata) -> SCDConfig:
    """Extract and validate SCD configuration from an entity's tags.

    ``scd.untracked`` is also parsed for entities without ``scd.type``: it
    configures change detection for the default SCD1 keyed merge.
    """
    tags = entity.tags or {}
    scd_type = tags.get("scd.type", "").strip()
    untracked_columns = _parse_column_list_tag(tags, "scd.untracked")
    default_config = SCDConfig(
        enabled=False,
        tracked_columns=None,
        effective_from_column="__effective_from",
        effective_to_column="__effective_to",
        is_current_column="__is_current",
        current_entity_id=f"{entity.entityid}.current",
        routing_key_method="hash",
        untracked_columns=untracked_columns,
    )

    if not scd_type:
        return default_config

    if scd_type != "2":
        raise ValueError(
            f"Entity '{entity.entityid}': scd.type must be '2' "
            f"(only SCD Type 2 is supported), got '{scd_type}'"
        )

    tracked_columns = _parse_column_list_tag(tags, "scd.tracked")

    routing_key_method = tags.get("scd.routing_key", "hash").strip().lower()
    if routing_key_method not in ROUTING_KEY_METHODS:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.routing_key must be one of "
            f"{ROUTING_KEY_METHODS}, got '{routing_key_method}'"
        )

    close_on_missing = tags.get("scd.close_on_missing", "").strip().lower() == "true"

    # source_kind states the input contract explicitly; scd.close_on_missing
    # survives as sugar for source_kind=snapshot.
    source_kind_raw = tags.get("scd.source_kind", "").strip().lower()
    if source_kind_raw and source_kind_raw not in ("snapshot", "change_feed"):
        raise ValueError(
            f"Entity '{entity.entityid}': scd.source_kind must be 'snapshot' "
            f"or 'change_feed', got '{source_kind_raw}'"
        )
    if source_kind_raw == "change_feed" and close_on_missing:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.close_on_missing=true is snapshot "
            "semantics and contradicts scd.source_kind=change_feed"
        )
    source_kind = source_kind_raw or ("snapshot" if close_on_missing else "change_feed")
    if source_kind == "snapshot":
        close_on_missing = True

    delete_when = tags.get("scd.delete_when", "").strip() or None
    if delete_when and source_kind == "snapshot":
        raise ValueError(
            f"Entity '{entity.entityid}': scd.delete_when applies to change-feed "
            "sources only — a snapshot expresses deletion by absence"
        )

    sequence_by = tags.get("scd.sequence_by", "").strip() or None

    return SCDConfig(
        enabled=True,
        tracked_columns=tracked_columns,
        effective_from_column=tags.get("scd.effective_from_col", "__effective_from"),
        effective_to_column=tags.get("scd.effective_to_col", "__effective_to"),
        is_current_column=tags.get("scd.current_col", "__is_current"),
        current_entity_id=tags.get("scd.current_entity_id", f"{entity.entityid}.current"),
        routing_key_method=routing_key_method,
        close_on_missing=close_on_missing,
        optimize_unchanged=tags.get("scd.optimize_unchanged", "").strip().lower() == "true",
        sequence_by=sequence_by,
        source_kind=source_kind,
        delete_when=delete_when,
        untracked_columns=untracked_columns,
    )


def quote_sql_identifier(name: str, alias: Optional[str] = None) -> str:
    """Backtick-quote a column name for a raw SQL expression, optionally alias-qualified."""
    escaped = name.replace("`", "``")
    if alias:
        return f"{alias}.`{escaped}`"
    return f"`{escaped}`"


def build_null_safe_change_condition(
    source_alias: str, target_alias: str, tracked_columns: List[str], schema=None
) -> str:
    """Null-safe OR-of-changed-columns SQL condition for MERGE/join change detection."""
    if not tracked_columns:
        return "false"

    unorderable = {
        field.name
        for field in (schema.fields if schema else [])
        if isinstance(field.dataType, MapType)
    }

    def _comparable(column: str, alias: str) -> str:
        ident = quote_sql_identifier(column, alias)
        # Spark's binary comparison does not support ordering on MAP types;
        # compare their JSON projection instead (same approach as the
        # optimize_unchanged hash path).
        if column in unorderable:
            return f"to_json({ident})"
        return ident

    # NOT (a <=> b) is never NULL (both-null compares equal), so the
    # condition can be negated safely: the earlier `a != b OR (a IS NULL) !=
    # (b IS NULL)` form evaluated to NULL when both sides were null, and its
    # negation then classified the row as neither changed nor unchanged.
    return " OR ".join(
        [
            f"(NOT ({_comparable(column, source_alias)} <=> "
            f"{_comparable(column, target_alias)}))"
            for column in tracked_columns
        ]
    )


def spark_case_sensitive(spark) -> bool:
    """Whether ``spark`` resolves column names case-sensitively.

    Reads ``spark.sql.caseSensitive`` (Spark's default is false: ``STATUS``
    and ``status`` name the same column).
    """
    try:
        return str(spark.conf.get("spark.sql.caseSensitive", "false")).strip().lower() == "true"
    except Exception:
        return False


def column_name_key(case_sensitive: bool) -> Callable[[str], str]:
    """Key function under which two column names are the same column."""
    if case_sensitive:
        return lambda name: name
    return lambda name: name.lower()


def scd_change_columns(
    cfg: SCDConfig,
    columns: List[str],
    business_keys: List[str],
    case_sensitive: bool = True,
    insert_only_columns: Optional[List[str]] = None,
) -> Tuple[List[str], List[str]]:
    """Split a batch's columns into ``(tracked, untracked)`` for change detection.

    A difference in a tracked column is a change: under SCD2 it closes the
    current version and opens a new one; under the SCD1 merge it rewrites
    the matched row. Untracked columns never count as a change: SCD2
    updates them on the current version in place, SCD1 leaves them alone.

    - ``scd.tracked`` (allow-list): tracked is the list as declared;
      every other non-key, non-temporal column of the batch is untracked.
    - otherwise: every non-key, non-temporal column of the batch is
      tracked except ``scd.untracked`` and the ``scd.sequence_by`` column
      (ordering authority, not content).

    Untracked columns absent from the batch are skipped. Temporal columns
    are only excluded for SCD2 entities (``cfg.enabled``). Names match
    exactly unless ``case_sensitive`` is False; providers pass the session's
    ``spark_case_sensitive`` so ``STATUS`` and ``status`` are one column.

    ``insert_only_columns`` (surrogate key columns) are a third class: they
    are neither tracked nor untracked, so no merge clause ever rewrites
    them.
    """
    key = column_name_key(case_sensitive)
    temporal_columns = (
        {cfg.effective_from_column, cfg.effective_to_column, cfg.is_current_column}
        if cfg.enabled
        else set()
    )
    insert_only = {key(name) for name in insert_only_columns or ()}
    non_candidates = {key(name) for name in list(business_keys) + list(temporal_columns)}
    candidates = [
        column
        for column in columns
        if key(column) not in non_candidates and key(column) not in insert_only
    ]
    if cfg.tracked_columns:
        tracked = [name for name in cfg.tracked_columns if key(name) not in insert_only]
        tracked_keys = {key(name) for name in tracked}
        untracked = [column for column in candidates if key(column) not in tracked_keys]
        return tracked, untracked

    excluded = {key(name) for name in cfg.untracked_columns or ()}
    if cfg.sequence_by:
        excluded.add(key(cfg.sequence_by))
    tracked = [column for column in candidates if key(column) not in excluded]
    untracked = [column for column in candidates if key(column) in excluded]
    return tracked, untracked


def augment_schema_for_scd2(schema: StructType, cfg: SCDConfig) -> StructType:
    """Add SCD2 temporal columns (effective_from/to, is_current) to a schema when enabled."""
    schema_struct = schema if isinstance(schema, StructType) else StructType(schema)
    if not cfg.enabled:
        return schema_struct

    existing_names = {field.name for field in schema_struct.fields}
    extra_fields = []
    if cfg.effective_from_column not in existing_names:
        extra_fields.append(StructField(cfg.effective_from_column, TimestampType(), False))
    if cfg.effective_to_column not in existing_names:
        extra_fields.append(StructField(cfg.effective_to_column, TimestampType(), True))
    if cfg.is_current_column not in existing_names:
        extra_fields.append(StructField(cfg.is_current_column, BooleanType(), False))

    if not extra_fields:
        return schema_struct
    return StructType(schema_struct.fields + extra_fields)


# ── Surrogate keys (tags-first; see docs/guide/data_entities.md) ─────────────
#
# Tag parsing and registration-time validation live here, beside the SCD
# configuration they interact with. The encoder, the strategies, custom
# generators and the key placements are in kindling.surrogate_keys.

_SURROGATE_STRATEGIES = ("hash", "uuid5", "custom")
_SURROGATE_KNOWN_TAGS = (
    "surrogate.column",
    "surrogate.durable_column",
    "surrogate.strategy",
    "surrogate.from",
    "surrogate.include_effective_from",
    "surrogate.namespace",
    "surrogate.normalize",
    "surrogate.hash.algo",
    "surrogate.hash.format",
    "surrogate.uuid.namespace",
    "surrogate.generator",
    "surrogate.require_reproducible",
    "surrogate.encoding",
)


@dataclass(frozen=True)
class SurrogateKeyConfig:
    """Parsed surrogate key configuration derived from an entity's tags.

    ``column`` is the key column (the *version* key on an SCD2 entity);
    ``durable_column`` is the SCD2 durable key. ``from_columns`` defaults
    to the entity's ``merge_columns``. Parsing never raises; registration
    (``_validate_surrogate_config``) rejects invalid or unsupported values.
    """

    column: Optional[str] = None
    durable_column: Optional[str] = None
    strategy: str = "hash"
    from_columns: Tuple[str, ...] = ()
    include_effective_from: bool = True
    namespace: Optional[str] = None
    normalize: str = "none"
    hash_algo: str = "sha256"
    hash_format: str = "hex"
    uuid_namespace: Optional[str] = None
    generator: Optional[str] = None
    # (option, value) pairs from surrogate.generator.<option> tags.
    generator_options: Tuple[Tuple[str, str], ...] = ()
    require_reproducible: bool = False
    encoding: str = "v1"

    @property
    def enabled(self) -> bool:
        return bool(self.column or self.durable_column)

    @property
    def key_columns(self) -> List[str]:
        """The declared key columns (version/plain key first, then durable)."""
        return [name for name in (self.column, self.durable_column) if name]


def _tag_text(tags: Dict[str, Any], key: str) -> str:
    value = tags.get(key)
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip()


def surrogate_config_from_tags(entity: EntityMetadata) -> SurrogateKeyConfig:
    """Extract surrogate key configuration from an entity's tags (never raises)."""
    tags = entity.tags or {}
    column = _tag_text(tags, "surrogate.column") or None
    durable_column = _tag_text(tags, "surrogate.durable_column") or None
    if (
        not column
        and not durable_column
        and not any(str(key).startswith("surrogate.") for key in tags)
    ):
        return SurrogateKeyConfig()
    from_columns = _parse_column_list_tag(tags, "surrogate.from")
    if from_columns is None:
        from_columns = list(entity.merge_columns or [])
    include_raw = _tag_text(tags, "surrogate.include_effective_from").lower()
    namespace_raw = tags.get("surrogate.namespace")
    namespace = None if namespace_raw is None or str(namespace_raw) == "" else str(namespace_raw)
    options = tuple(
        sorted(
            (key[len("surrogate.generator.") :], str(value))
            for key, value in tags.items()
            if key.startswith("surrogate.generator.") and key != "surrogate.generator."
        )
    )
    return SurrogateKeyConfig(
        column=column,
        durable_column=durable_column,
        strategy=_tag_text(tags, "surrogate.strategy").lower() or "hash",
        from_columns=tuple(from_columns),
        include_effective_from=include_raw != "false",
        namespace=namespace,
        normalize=_tag_text(tags, "surrogate.normalize").lower() or "none",
        hash_algo=_tag_text(tags, "surrogate.hash.algo").lower() or "sha256",
        hash_format=_tag_text(tags, "surrogate.hash.format").lower() or "hex",
        uuid_namespace=_tag_text(tags, "surrogate.uuid.namespace") or None,
        generator=_tag_text(tags, "surrogate.generator") or None,
        generator_options=options,
        require_reproducible=_tag_text(tags, "surrogate.require_reproducible").lower() == "true",
        encoding=_tag_text(tags, "surrogate.encoding") or "v1",
    )


def surrogate_key_columns(entity: EntityMetadata) -> List[str]:
    """The entity's surrogate key column names (insert-only), or ``[]``."""
    return surrogate_config_from_tags(entity).key_columns


def _stamp_surrogate_encoding(entity: EntityMetadata) -> EntityMetadata:
    """Record the key encoding (``surrogate.encoding``) on an entity with keys."""
    tags = entity.tags or {}
    if "surrogate.encoding" in tags or not surrogate_config_from_tags(entity).enabled:
        return entity
    return replace(entity, tags={**tags, "surrogate.encoding": "v1"})


def _validate_surrogate_config(entity: EntityMetadata) -> None:
    """Validate ``surrogate.*`` tags at registration time.

    Column names compare case-insensitively (Spark's default resolution),
    so a collision that differs only in case is still rejected. Custom
    generator names are not resolved here: they resolve at
    ``ensure_destination``, after bootstrap has imported every module.
    """
    tags = entity.tags or {}
    surrogate_tags = [key for key in tags if key.startswith("surrogate.")]
    if not surrogate_tags:
        return
    eid = entity.entityid

    def fail(message: str) -> None:
        raise ValueError(f"Entity '{eid}': {message}")

    for key in surrogate_tags:
        if key == "surrogate.unknown_member":
            fail(
                "surrogate.unknown_member is not yet supported (unknown-member "
                "seeding arrives in a later release)"
            )
        if key not in _SURROGATE_KNOWN_TAGS and not key.startswith("surrogate.generator."):
            fail(f"unknown surrogate tag '{key}' (known: {sorted(_SURROGATE_KNOWN_TAGS)})")

    cfg = surrogate_config_from_tags(entity)
    if not cfg.enabled:
        fail(
            "surrogate.* tags are set but neither surrogate.column nor "
            "surrogate.durable_column names a key column"
        )
    if entity.is_sql_entity:
        fail("surrogate keys do not apply to a SQL entity (a read-only view)")

    if cfg.strategy == "uuid4":
        fail(
            "surrogate.strategy 'uuid4' is not yet supported (random keys arrive "
            "in a later release); use 'uuid5' for a deterministic UUID-shaped key"
        )
    if cfg.strategy == "identity":
        fail(
            "surrogate.strategy 'identity' is not yet supported (Delta IDENTITY "
            "columns arrive in a later release)"
        )
    if cfg.strategy not in _SURROGATE_STRATEGIES:
        fail(
            f"invalid surrogate.strategy '{cfg.strategy}' (expected one of "
            f"{list(_SURROGATE_STRATEGIES)})"
        )
    if cfg.hash_algo in ("md5", "xxhash64"):
        fail(
            f"surrogate.hash.algo '{cfg.hash_algo}' is not yet supported "
            "(only 'sha256' in this release)"
        )
    if cfg.hash_algo != "sha256":
        fail(f"invalid surrogate.hash.algo '{cfg.hash_algo}' (expected 'sha256')")
    if cfg.hash_format == "binary":
        fail("surrogate.hash.format 'binary' is not yet supported (only 'hex' in this release)")
    if cfg.hash_format != "hex":
        fail(f"invalid surrogate.hash.format '{cfg.hash_format}' (expected 'hex')")
    if cfg.normalize in ("trim", "trim_upper"):
        fail(
            f"surrogate.normalize '{cfg.normalize}' is not yet supported "
            "(only 'none' in this release)"
        )
    if cfg.normalize != "none":
        fail(f"invalid surrogate.normalize '{cfg.normalize}' (expected 'none')")
    if cfg.encoding != "v1":
        fail(f"unsupported surrogate.encoding '{cfg.encoding}' (expected 'v1')")
    for key in ("surrogate.include_effective_from", "surrogate.require_reproducible"):
        raw = _tag_text(tags, key).lower()
        if raw not in ("", "true", "false"):
            fail(f"{key} must be 'true' or 'false', got '{raw}'")
    if "surrogate.from" in tags and not _parse_column_list_tag(tags, "surrogate.from"):
        fail(
            f"surrogate.from is set but contains no usable column names: "
            f"'{tags.get('surrogate.from')}'"
        )

    # Strategy companions.
    if cfg.strategy == "uuid5":
        if not cfg.uuid_namespace:
            fail("surrogate.strategy 'uuid5' requires surrogate.uuid.namespace (a UUID)")
        import uuid as _uuid

        try:
            _uuid.UUID(cfg.uuid_namespace)
        except ValueError:
            fail(f"surrogate.uuid.namespace is not a valid UUID: '{cfg.uuid_namespace}'")
    elif cfg.uuid_namespace:
        fail("surrogate.uuid.namespace applies to surrogate.strategy 'uuid5' only")
    if cfg.strategy == "custom":
        if not cfg.generator:
            fail("surrogate.strategy 'custom' requires surrogate.generator (a registered name)")
    elif cfg.generator or cfg.generator_options:
        fail("surrogate.generator and surrogate.generator.* apply to surrogate.strategy 'custom'")
    if cfg.strategy != "custom" and not cfg.from_columns:
        fail("surrogate keys need input columns: declare merge_columns or " "surrogate.from")

    scd = scd_config_from_tags(entity)
    if cfg.durable_column and not scd.enabled:
        fail("surrogate.durable_column requires an SCD2 entity (scd.type: '2')")
    if "surrogate.include_effective_from" in tags and not (scd.enabled and cfg.column):
        fail("surrogate.include_effective_from applies to an SCD2 version key only")

    def norm(name: str) -> str:
        return name.lower()

    key_names = cfg.key_columns
    if len({norm(name) for name in key_names}) != len(key_names):
        fail("surrogate.column and surrogate.durable_column must be different columns")
    key_set = {norm(name) for name in key_names}
    from_set = {norm(name) for name in cfg.from_columns}
    for label, names in (
        ("merge_columns", entity.merge_columns or []),
        ("surrogate.from", cfg.from_columns),
        ("scd.tracked", scd.tracked_columns or []),
        ("scd.untracked", scd.untracked_columns or []),
    ):
        overlap = sorted(name for name in names if norm(name) in key_set)
        if overlap:
            fail(
                f"surrogate key columns must not appear in {label} (keys are "
                f"insert-only): {overlap}"
            )
    if scd.enabled:
        temporal = {
            norm(scd.effective_from_column): scd.effective_from_column,
            norm(scd.effective_to_column): scd.effective_to_column,
            norm(scd.is_current_column): scd.is_current_column,
        }
        clash = sorted(name for name in key_names if norm(name) in temporal)
        if clash:
            fail(f"surrogate key columns must not be SCD2 temporal columns: {clash}")
        if norm(scd.effective_from_column) in from_set:
            fail(
                "surrogate.from must not include the effective_from column; the "
                "version key includes it through surrogate.include_effective_from"
            )
        closing = sorted(
            name
            for name in cfg.from_columns
            if norm(name) in (norm(scd.effective_to_column), norm(scd.is_current_column))
        )
        if closing:
            fail(
                "surrogate.from must not include effective_to or is_current, which "
                f"change when a version is closed: {closing}"
            )
        if scd.sequence_by and norm(scd.sequence_by) in key_set:
            fail("a surrogate key column must not be the scd.sequence_by column")
    reserved = sorted(
        name for name in key_names if name == "__merge_key" or name.startswith("__scd2_")
    )
    if reserved:
        fail(f"surrogate key columns use reserved SCD2 staging names: {reserved}")

    if entity.schema is not None:
        from kindling.surrogate_keys import v1_type_problem

        fields_by_name = {norm(field.name): field for field in entity.schema.fields}
        missing = [name for name in cfg.from_columns if norm(name) not in fields_by_name]
        if missing:
            fail(f"surrogate.from columns not in the entity schema: {missing}")
        for name in cfg.from_columns:
            problem = v1_type_problem(fields_by_name[norm(name)].dataType)
            if problem:
                fail(f"surrogate.from column '{name}' cannot be encoded: {problem}")
        if cfg.strategy in ("hash", "uuid5"):
            for name in key_names:
                declared = fields_by_name.get(norm(name))
                if declared is not None and not isinstance(declared.dataType, StringType):
                    fail(
                        f"surrogate key column '{name}' is declared as "
                        f"{declared.dataType.simpleString()}, but a '{cfg.strategy}' key "
                        "is a STRING"
                    )

    if scd.enabled and cfg.column and cfg.include_effective_from and not scd.sequence_by:
        message = (
            f"Entity '{eid}': the SCD2 version key '{cfg.column}' includes "
            "effective_from, which is processing time without scd.sequence_by, so "
            "version keys are unique and fixed once stored but not reproducible on "
            "a rebuild"
        )
        if cfg.require_reproducible:
            raise ValueError(
                message + "; surrogate.require_reproducible=true requires scd.sequence_by"
            )
        _ENTITY_LOGGER.warning(
            "%s. Declare scd.sequence_by, or set surrogate.require_reproducible=true "
            "to make this an error.",
            message,
        )


@dataclass(frozen=True)
class DerivedConfig:
    """Parsed derived-dataset configuration from an entity's tags.

    A derived dataset has no independent state: its contents are a pure
    function of its inputs. Engines materialize the declaration however
    they natively can — the runner as an atomic overwrite swap (full
    table, or per-slice via ``replace_keys``), declarative engines as a
    materialized view.
    """

    enabled: bool
    replace_keys: Optional[List[str]] = None


def derived_config_from_tags(entity: EntityMetadata) -> DerivedConfig:
    """Extract derived-dataset configuration from an entity's tags.

    Tags:
    - ``dataset.kind``: ``"derived"`` opts in; absent/empty means a state
      dataset (the default — today's append/merge/SCD behavior).
    - ``derived.replace_keys``: comma-separated scoping columns. When set,
      each write atomically replaces only the slices present in the batch
      (the distinct values of these columns) instead of the whole table.
    """
    tags = entity.tags or {}
    kind = str(tags.get("dataset.kind") or "").strip().lower()
    replace_raw = str(tags.get("derived.replace_keys") or "").strip()
    replace_keys = (
        [column.strip() for column in replace_raw.split(",") if column.strip()]
        if replace_raw
        else None
    )
    return DerivedConfig(enabled=kind == "derived", replace_keys=replace_keys)


def _validate_derived_config(entity: EntityMetadata) -> None:
    """Validate derived-dataset tags at registration time.

    Derived and state vocabularies are mutually exclusive: a derived
    dataset is recomputed, so evolution semantics (``write.mode``,
    ``scd.*``) cannot apply to it.
    """
    tags = entity.tags or {}
    kind = str(tags.get("dataset.kind") or "").strip().lower()
    if kind not in ("", "derived"):
        raise ValueError(
            f"Entity '{entity.entityid}': invalid dataset.kind "
            f"'{kind}' (expected 'derived' or unset)"
        )

    cfg = derived_config_from_tags(entity)
    replace_raw = str(tags.get("derived.replace_keys") or "").strip()
    if replace_raw and not cfg.replace_keys:
        raise ValueError(
            f"Entity '{entity.entityid}': derived.replace_keys is set but "
            f"contains no usable column names: '{replace_raw}'"
        )
    if not cfg.enabled:
        if cfg.replace_keys:
            raise ValueError(
                f"Entity '{entity.entityid}': derived.replace_keys requires "
                "dataset.kind='derived'"
            )
        return

    if str(tags.get("write.mode") or "").strip():
        raise ValueError(
            f"Entity '{entity.entityid}': write.mode does not apply to a "
            "derived dataset — its contents are replaced, not evolved"
        )
    if str(tags.get("scd.type") or "").strip():
        raise ValueError(
            f"Entity '{entity.entityid}': scd.type does not apply to a "
            "derived dataset — history semantics require a state dataset"
        )
    if str(tags.get("scd.untracked") or "").strip():
        raise ValueError(
            f"Entity '{entity.entityid}': scd.untracked does not apply to a "
            "derived dataset — its contents are replaced, not merged"
        )

    if cfg.replace_keys and entity.schema is not None:
        schema_names = {field.name for field in entity.schema.fields}
        missing = [column for column in cfg.replace_keys if column not in schema_names]
        if missing:
            raise ValueError(
                f"Entity '{entity.entityid}': derived.replace_keys columns "
                f"not in entity schema: {missing}"
            )


def _validate_schema_drift_tag(entity: EntityMetadata) -> None:
    """Validate the static ``schema.drift`` policy tag at registration time.

    ``evolve`` (default): additive schema evolution, today's behavior.
    ``warn``: log drift (new columns / type conflicts) before writing.
    ``fail``: refuse drifting writes with a SchemaDriftError.
    """
    policy = str((entity.tags or {}).get("schema.drift") or "").strip().lower()
    if policy not in ("", "evolve", "warn", "fail"):
        raise ValueError(
            f"Entity '{entity.entityid}': invalid schema.drift "
            f"'{policy}' (expected 'evolve', 'warn' or 'fail')"
        )


def _validate_write_mode_tag(entity: EntityMetadata) -> None:
    """Validate the static ``write.mode`` tag at registration time.

    Both persist paths honor the tag (batch ``SimpleReadPersistStrategy``
    and streaming ``SimplePipeStreamStarter``). Rejecting bad values here
    surfaces typos when the entity is registered instead of mid-persist,
    where a raise would sit awkwardly inside the persist lifecycle.
    """
    tags = entity.tags or {}
    write_mode = str(tags.get("write.mode") or "").strip().lower()
    if write_mode not in ("", "append", "merge", "insert"):
        raise ValueError(
            f"Entity '{entity.entityid}': invalid write.mode "
            f"'{write_mode}' (expected 'append', 'merge' or 'insert')"
        )
    if write_mode == "insert":
        if not entity.merge_columns:
            raise ValueError(
                f"Entity '{entity.entityid}': write.mode 'insert' requires "
                "merge_columns (dedup keys) to be defined"
            )
        if str(tags.get("scd.type") or "").strip():
            raise ValueError(
                f"Entity '{entity.entityid}': write.mode 'insert' contradicts "
                "scd.type — SCD entities update history on change"
            )
        if str(tags.get("scd.untracked") or "").strip():
            raise ValueError(
                f"Entity '{entity.entityid}': write.mode 'insert' never updates "
                "existing rows, so scd.untracked has no effect"
            )


def _validate_scd_untracked(entity: EntityMetadata, cfg: SCDConfig) -> None:
    """Validate ``scd.untracked`` (SCD2 and the default SCD1 merge alike)."""
    tags = entity.tags or {}
    untracked_raw = str(tags.get("scd.untracked") or "").strip()
    if not untracked_raw:
        return
    if not cfg.untracked_columns:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.untracked is set but contains no "
            f"usable column names: '{untracked_raw}'"
        )
    if _parse_column_list_tag(tags, "scd.tracked"):
        raise ValueError(
            f"Entity '{entity.entityid}': scd.tracked and scd.untracked are "
            "mutually exclusive — declare the tracked columns or the untracked "
            "ones, not both"
        )
    if not entity.merge_columns:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.untracked requires merge_columns "
            "(business keys) — it configures change detection for keyed merges"
        )

    untracked = set(cfg.untracked_columns)
    key_overlap = untracked & set(entity.merge_columns)
    if key_overlap:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.untracked must not include "
            f"merge_columns (business keys): {sorted(key_overlap)}"
        )

    reserved = set()
    if cfg.enabled:
        reserved = {
            cfg.effective_from_column,
            cfg.effective_to_column,
            cfg.is_current_column,
            "__merge_key",
        }
    reserved_overlap = untracked & reserved
    if reserved_overlap:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.untracked must not include SCD2 "
            f"temporal or reserved columns: {sorted(reserved_overlap)}"
        )

    if entity.schema is None:
        return
    schema_names = [field.name for field in entity.schema.fields]
    missing = sorted(untracked - set(schema_names))
    if missing:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.untracked columns not in the "
            f"entity schema: {missing}"
        )
    excluded = untracked | set(entity.merge_columns) | reserved
    if cfg.sequence_by:
        excluded.add(cfg.sequence_by)
    if not [name for name in schema_names if name not in excluded]:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.untracked leaves no tracked column — "
            "at least one non-key column must be tracked"
            + (" (scd.sequence_by is never tracked)" if cfg.sequence_by else "")
        )


def _validate_scd_config(entity: EntityMetadata) -> None:
    """Validate SCD tag configuration at registration time."""
    cfg = scd_config_from_tags(entity)
    _validate_scd_untracked(entity, cfg)
    if not cfg.enabled:
        return

    if not entity.merge_columns:
        raise ValueError(
            f"Entity '{entity.entityid}': SCD Type 2 requires merge_columns "
            "(business keys) to be defined"
        )

    if cfg.tracked_columns:
        overlap = set(cfg.tracked_columns) & set(entity.merge_columns)
        if overlap:
            raise ValueError(
                f"Entity '{entity.entityid}': scd.tracked must not include "
                f"merge_columns (business keys): {sorted(overlap)}"
            )

    temporal_columns = {
        cfg.effective_from_column,
        cfg.effective_to_column,
        cfg.is_current_column,
    }
    schema_names = {field.name for field in entity.schema.fields} if entity.schema else set()
    collisions = temporal_columns & schema_names
    if collisions:
        raise ValueError(
            f"Entity '{entity.entityid}': temporal column names {sorted(collisions)} "
            "collide with business schema columns. Override via SCD column tags."
        )

    if "__merge_key" in schema_names:
        raise ValueError(
            f"Entity '{entity.entityid}': column '__merge_key' is reserved for SCD2 "
            "merge staging and must not appear in the entity schema."
        )
    staging_names = sorted(name for name in schema_names if name.startswith("__scd2_"))
    if staging_names:
        raise ValueError(
            f"Entity '{entity.entityid}': columns {staging_names} use the '__scd2_' "
            "prefix, which is reserved for SCD2 merge staging, and must not appear "
            "in the entity schema."
        )

    current_id = cfg.current_entity_id.strip()
    if not current_id or current_id == entity.entityid:
        raise ValueError(
            f"Entity '{entity.entityid}': scd.current_entity_id must be non-empty and "
            "differ from the base entity id."
        )

    if cfg.sequence_by:
        if cfg.sequence_by in temporal_columns:
            raise ValueError(
                f"Entity '{entity.entityid}': scd.sequence_by must be a business "
                f"data column, not the temporal column '{cfg.sequence_by}'"
            )
        if schema_names and cfg.sequence_by not in schema_names:
            raise ValueError(
                f"Entity '{entity.entityid}': scd.sequence_by column "
                f"'{cfg.sequence_by}' is not in the entity schema"
            )


_ENTITY_EXTENSION_KEYWORDS = ("tags", "add_columns", "add_partition_columns", "add_cluster_columns")


def _entity_derivation(clone_of, **changes) -> EntityDerivation:
    """Split keyword changes into an EntityDerivation (additive keywords vs
    clone-only overrides); unknown keywords fail here, at declaration."""
    additive = {key: changes.pop(key) for key in _ENTITY_EXTENSION_KEYWORDS if key in changes}
    return EntityDerivation(
        clone_of=clone_of,
        tags=additive.get("tags"),
        add_columns=tuple(additive.get("add_columns") or ()),
        add_partition_columns=tuple(additive.get("add_partition_columns") or ()),
        add_cluster_columns=tuple(additive.get("add_cluster_columns") or ()),
        overrides=dict(changes),
    )


class DataEntities:

    deregistry = None

    @classmethod
    def reset(cls) -> None:
        """Reset the entity registry. Use between tests to prevent state pollution."""
        cls.deregistry = None

    @classmethod
    def sql_entity(
        cls,
        entityid: str,
        name: str,
        tags: Optional[Dict[str, str]] = None,
        sql: Optional[str] = None,
        sql_source: Optional[SqlSource] = None,
    ):
        """Register a SQL-defined (permanent catalog view) entity.

        The entity is read-only and backed by a Spark catalog view.
        Migration manages it via ``CREATE OR REPLACE VIEW``.

        Exactly one of ``sql`` or ``sql_source`` must be provided.

        Example — inline SQL::

            @DataEntities.sql_entity(
                entityid="reporting.recent_sales",
                name="recent_sales",
                sql="SELECT * FROM sales.transactions WHERE event_date >= current_date() - 30",
            )

        Example — package resource::

            @DataEntities.sql_entity(
                entityid="reporting.recent_sales",
                name="recent_sales",
                sql_source=SqlSource(resource="my_app:sql/recent_sales.sql"),
            )
        """
        if cls.deregistry is None:
            try:
                _raise_if_not_initialized("DataEntities.sql_entity", "entity")
                cls.deregistry = GlobalInjector.get(DataEntityRegistry)
            except Exception as exc:
                if isinstance(exc, KindlingNotInitializedError):
                    raise
                raise KindlingNotInitializedError(
                    "A @DataEntities.sql_entity decorator fired before initialize() was called. "
                    "Call initialize() before importing entity modules. "
                    "See your app.py register_all() for the correct order."
                ) from exc

        provided = sum(x is not None for x in [sql, sql_source])
        if provided != 1:
            raise ValueError(
                "sql_entity requires exactly one of: sql, sql_source. "
                f"Got {provided} argument(s)."
            )

        resolved_sql = sql if sql is not None else sql_source.load()
        # Read by running the SQL, so only a single read-only query is allowed.
        from kindling.entity_provider_sql import require_query_sql

        require_query_sql(resolved_sql, entityid)
        provider_type = (tags or {}).get("provider_type", "view")
        if provider_type != "view":
            raise ValueError(
                f"SQL entity '{entityid}' cannot use provider_type '{provider_type}': "
                "SQL entities are read-only views (provider_type 'view')."
            )
        merged_tags = {**(tags or {}), "provider_type": "view"}

        cls.deregistry.register_entity(
            entityid,
            name=name,
            merge_columns=[],
            tags=merged_tags,
            schema=None,
            sql=resolved_sql,
        )
        return lambda x: x

    @classmethod
    def derived_entity(cls, replace_keys=None, **decorator_params):
        """Register a derived dataset — sugar for ``dataset.kind: derived``.

        A derived dataset's contents are a pure function of its inputs, so
        writes replace rather than evolve (see
        docs/guide/derived_datasets.md). Per the declaration convention
        (docs/contributing/declaration_conventions.md) this helper only
        sets the canonical tags as defaults — explicit tags win, and
        engines read tags, never this helper.

        Args:
            replace_keys: Optional slice-scope columns (list or
                comma-separated string) — sets ``derived.replace_keys``.

        Example::

            @DataEntities.derived_entity(
                entityid="gold.run_summary",
                name="run_summary",
                merge_columns=[],
                schema=None,
                replace_keys=["run_id"],
            )
        """
        default_tags: Dict[str, str] = {"dataset.kind": "derived"}
        if replace_keys:
            if isinstance(replace_keys, str):
                default_tags["derived.replace_keys"] = replace_keys
            elif isinstance(replace_keys, (list, tuple)) and all(
                isinstance(key, str) for key in replace_keys
            ):
                # Ordered sequences only: a set would make the tag value —
                # and thus the declaration — nondeterministic.
                default_tags["derived.replace_keys"] = ",".join(replace_keys)
            else:
                raise ValueError(
                    "derived_entity replace_keys must be a comma-separated "
                    "string or a list/tuple of column names, got "
                    f"{type(replace_keys).__name__}"
                )
        decorator_params["tags"] = {**default_tags, **(decorator_params.get("tags") or {})}
        return cls.entity(**decorator_params)

    @classmethod
    def insert_only_entity(cls, **decorator_params):
        """Register an immutable state table — sugar for ``write.mode: insert``.

        Rows are inserted if their ``merge_columns`` keys are absent and
        left untouched otherwise, so replays of already-landed batches
        rewrite nothing. Per the declaration convention this helper only
        sets the canonical tag as a default — explicit tags win.

        Example::

            @DataEntities.insert_only_entity(
                entityid="silver.readings",
                name="readings",
                merge_columns=["reading_id"],
                schema=None,
            )
        """
        decorator_params["tags"] = {
            "write.mode": "insert",
            **(decorator_params.get("tags") or {}),
        }
        return cls.entity(**decorator_params)

    @classmethod
    def clone(cls, entityid, from_entity, **changes):
        """Declare ``entityid`` from ``from_entity``'s raw declaration as a
        template, optionally extended in the same call.

        ``changes`` accepts the extension keywords (``tags``, ``add_columns``,
        ``add_partition_columns``, ``add_cluster_columns``) plus clone-only
        replacements (``name``, ``merge_columns``, ``partition_columns``,
        ``cluster_columns``); ``name`` is copied from the source unless given.
        The clone implies nothing about data: whatever writes to it (a cloned
        pipe, a new pipe, or nothing yet) is a separate declaration. Resolved
        when the source is registered, in any import order.
        """
        cls._registry_for("DataEntities.clone").derive_entity(
            entityid, _entity_derivation(clone_of=from_entity, **changes)
        )
        return lambda x: x

    @classmethod
    def extend(cls, entityid, **changes):
        """Additively extend an already-declared (or later-declared) entity in
        place: ``tags`` (later value wins), ``add_columns``,
        ``add_partition_columns``, ``add_cluster_columns``. Extensions stack
        in registration order; config overlays apply on top."""
        cls._registry_for("DataEntities.extend").derive_entity(
            entityid, _entity_derivation(clone_of=None, **changes)
        )
        return lambda x: x

    @classmethod
    def _registry_for(cls, operation):
        if cls.deregistry is None:
            try:
                _raise_if_not_initialized(operation, "entity")
                cls.deregistry = GlobalInjector.get(DataEntityRegistry)
            except Exception as exc:
                if isinstance(exc, KindlingNotInitializedError):
                    raise
                raise KindlingNotInitializedError(
                    f"{operation} was called before initialize(). Call initialize() first."
                ) from exc
        return cls.deregistry

    @classmethod
    def entity(cls, surrogate_key=None, durable_key=None, **decorator_params):
        """Register an entity.

        ``surrogate_key`` / ``durable_key`` are sugar for the
        ``surrogate.column`` / ``surrogate.durable_column`` tags (see
        docs/guide/data_entities.md, "Surrogate keys"). Like
        ``insert_only_entity``, they only fill in default tags: explicit
        tags win, and the framework reads the tags, never these arguments.
        """
        sugar_tags = {}
        for tag, value in (
            ("surrogate.column", surrogate_key),
            ("surrogate.durable_column", durable_key),
        ):
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{tag} sugar expects a column name, got {value!r}")
            sugar_tags[tag] = value.strip()
        if sugar_tags:
            decorator_params["tags"] = {**sugar_tags, **(decorator_params.get("tags") or {})}
        if cls.deregistry is None:
            try:
                _raise_if_not_initialized("DataEntities.entity", "entity")
                cls.deregistry = GlobalInjector.get(DataEntityRegistry)
            except Exception as exc:
                if isinstance(exc, KindlingNotInitializedError):
                    raise
                raise KindlingNotInitializedError(
                    "A @DataEntities.entity decorator fired before initialize() was called. "
                    "Call initialize() before importing entity modules. "
                    "See your app.py register_all() for the correct order."
                ) from exc
        # Check all required fields are provided (excluding optional fields with defaults)
        all_fields = {field.name for field in fields(EntityMetadata)}
        optional_fields = {
            field.name
            for field in fields(EntityMetadata)
            if field.default is not MISSING or field.default_factory is not MISSING
        }
        required_fields = all_fields - optional_fields
        missing_fields = required_fields - decorator_params.keys()

        if missing_fields:
            raise ValueError(f"Missing required fields in entity decorator: {missing_fields}")

        entityid = decorator_params["entityid"]

        del decorator_params["entityid"]

        cls.deregistry.register_entity(entityid, **decorator_params)

        return lambda x: x


class DataEntityRegistry(ABC):
    """Abstract base for entity registration.

    Implementations MUST emit these signals:
        - entity.registered: When a new entity is registered
    """

    def derive_entity(self, entityid, derivation) -> None:
        """Apply a clone/extend derivation (see kindling.declaration_derivations)."""
        raise NotImplementedError("This registry does not support declaration derivations")

    def pending_derivations(self) -> Dict[str, str]:
        """Derivations whose source is not registered yet, by target id."""
        return {}

    EMITS = [
        "entity.registered",
        "entity.scd2_companion_registered",
    ]

    @abstractmethod
    def register_entity(self, entityid, **decorator_params):
        pass

    @abstractmethod
    def get_entity_ids(self) -> List[str]:
        """Every registered entity id, as a plain list.

        Implementations must return a real ``list``, never a live view
        (e.g. ``dict.keys()``) over internal state -- see
        ``DataPipesRegistry.get_pipe_ids()`` for why this matters even
        when nothing currently mutates the result in place.
        """
        pass

    @abstractmethod
    def get_entity_definition(self, name):
        pass

    @contextmanager
    def tag_overrides(self, overrides):
        """Run-scoped entity tag overrides (JIT parameters).

        Unlike ``ConfigService.set_entity_tags`` — durable config state —
        these apply only inside the context, e.g. for a single
        ``run_datapipes(..., entity_tags=...)`` call. Default: no-op;
        DataEntityManager implements the overlay.
        """
        yield


@GlobalInjector.singleton_autobind()
class DataEntityManager(DataEntityRegistry, SignalEmitter):
    """Manages entity registrations with signal emissions."""

    # Identity and structural fields are never config-overridable; every
    # other EntityMetadata field is, so new fields become overridable
    # without code changes here.
    _NON_OVERRIDABLE_FIELDS = ("entityid", "schema", "sql")

    @inject
    def __init__(
        self, signal_provider: SignalProvider = None, config_service: ConfigService = None
    ):
        self._init_signal_emitter(signal_provider)
        self.registry = {}
        self.config_service = config_service
        # Run-scoped JIT tag overlays (see tag_overrides); win over both
        # declared tags and config-level set_entity_tags overrides.
        self._tag_overlays = {}
        # Original registration params per entity (shallow copies). Config
        # overlay always re-resolves from these, never from overlaid
        # metadata, so re-applying is idempotent and never accumulates.
        # SCD2 current-row companions are derived state and never appear
        # here.
        self._raw_params = {}
        # Compiled dataentities: config section; persisted so entities
        # registered after bootstrap's overlay pass (workspace packages,
        # app register_all, notebook cells) are overlaid at registration.
        self._matcher = None
        # Compiled dataentities-bytag: config section -- same persistence
        # rationale as _matcher, but matches by the entity's own declared
        # tag value instead of its id.
        self._tag_matcher = None
        # Declaration derivations by target id, in registration order (code
        # first, then config-sourced ones re-applied on every overlay pass).
        # A clone target has no _raw_params entry: its raw declaration is
        # the source's, resolved on demand.
        self._derivations = {}
        # Targets whose source is not registered yet -> reason.
        self._pending = {}

    @contextmanager
    def tag_overrides(self, overrides):
        """Apply per-entity tag overrides for the duration of the context.

        ``overrides`` maps entity id -> partial tag dict, merged over the
        declared tags AND config-level overrides at retrieval time, then
        fully restored on exit. This is the JIT-parameter channel for
        per-run values (e.g. a backfill window on a provider entity) —
        distinct from ``ConfigService.set_entity_tags``, which is durable
        config state. Overlays are instance-level: they are visible to
        parallel pipe threads within the run (intended), and to concurrent
        runs on other threads (avoid overlapping concurrent runs that
        override the same entity).
        """
        if not overrides:
            yield
            return
        previous = self._tag_overlays
        merged = {**previous}
        for entity_id, tags in overrides.items():
            merged[entity_id] = {**previous.get(entity_id, {}), **(tags or {})}
        self._tag_overlays = merged
        try:
            yield
        finally:
            self._tag_overlays = previous

    def register_entity(self, entityid, **decorator_params):
        if any(d.is_clone for d in self._derivations.get(entityid, [])):
            raise DerivationError(
                f"Entity '{entityid}' is declared as a clone and cannot also be registered "
                "directly; drop one of the two declarations."
            )
        previous = self._raw_params.get(entityid)
        self._raw_params[entityid] = dict(decorator_params)
        try:
            self._rebuild(entityid)
        except Exception:
            # Roll back to the prior valid declaration (or to none) so later
            # rebuilds of clones still see what the registry actually holds.
            if previous is None:
                del self._raw_params[entityid]
            else:
                self._raw_params[entityid] = previous
            raise

    def derive_entity(self, entityid, derivation) -> None:
        if derivation.is_clone:
            if derivation.clone_of == entityid:
                raise DerivationError(f"Entity '{entityid}' cannot be cloned from itself.")
            if entityid in self._raw_params:
                raise DerivationError(
                    f"Entity '{entityid}' is registered directly and cannot also be a clone; "
                    "use DataEntities.extend to change it in place."
                )
            if any(d.is_clone for d in self._derivations.get(entityid, [])):
                raise DerivationError(f"Entity '{entityid}' already has a clone_of source.")
        # Transactional: a derivation that cannot be applied is not kept, so
        # it never poisons later rebuilds of the same declaration.
        bucket = self._derivations.setdefault(entityid, [])
        bucket.append(derivation)
        try:
            self._rebuild(entityid)
        except Exception:
            bucket.remove(derivation)
            if not bucket:
                del self._derivations[entityid]
            raise

    def derivations_for(self, entityid) -> List[Any]:
        """The derivation chain applied to ``entityid``, in order."""
        return list(self._derivations.get(entityid, []))

    def pending_derivations(self) -> Dict[str, str]:
        return dict(self._pending)

    def _declared_ids(self) -> List[str]:
        """Registered ids plus clone targets, in first-seen order."""
        ids = list(self._raw_params)
        for target, derivations in self._derivations.items():
            if target not in self._raw_params and any(d.is_clone for d in derivations):
                ids.append(target)
        return ids

    def _effective_raw_params(self, entityid, _visiting=None):
        """The raw declaration after derivations: the source's (for a clone)
        or the entity's own registration, then every extension in order.
        None while a dependency is not registered yet."""
        visiting = set(_visiting or ())
        if entityid in visiting:
            raise DerivationError(f"Entity derivations form a cycle through '{entityid}'.")
        visiting.add(entityid)
        derivations = list(self._derivations.get(entityid, []))
        clones = [d for d in derivations if d.is_clone]
        owner = f"entity '{entityid}'"
        if clones:
            source_params = self._effective_raw_params(clones[0].clone_of, visiting)
            if source_params is None:
                return None
            params = apply_entity_derivation(source_params, clones[0], owner)
            rest = [d for d in derivations if not d.is_clone]
        else:
            if entityid not in self._raw_params:
                return None
            params = dict(self._raw_params[entityid])
            rest = derivations
        for derivation in rest:
            params = apply_entity_derivation(params, derivation, owner)
        return params

    def _rebuild(self, entityid) -> None:
        """Rebuild ``entityid`` and every clone derived from it atomically:
        the whole affected graph is built and validated first, and only then
        committed, so a failure anywhere leaves the registry as it was."""
        staged: Dict[str, Any] = {}
        self._stage(entityid, staged)
        for target, entity in staged.items():
            if entity is None:
                # Pending again (source removed or retargeted by a reload):
                # stale metadata must not keep the id alive.
                self.registry.pop(target, None)
                self._pending[target] = self._pending_reason(target)
                continue
            self._pending.pop(target, None)
            newly_registered = target not in self.registry
            self.registry[target] = entity
            if newly_registered:
                self.emit("entity.registered", entity_id=target, entity_name=entity.name)
        # A rebuilt SCD2 base (extended columns, changed current_entity_id)
        # must refresh or retire its companion, not just create it if absent.
        if any(entity is not None for entity in staged.values()):
            self._converge_scd2_companions()

    def _stage(self, entityid, staged: Dict[str, Any]) -> None:
        params = self._effective_raw_params(entityid)
        if params is None:
            staged[entityid] = None
        else:
            entity = _stamp_surrogate_encoding(self._build_metadata(entityid, params))
            self._validate_entity(entity)
            staged[entityid] = entity
        for target, derivations in list(self._derivations.items()):
            if target != entityid and target not in staged:
                if any(d.clone_of == entityid for d in derivations):
                    self._stage(target, staged)

    def _pending_reason(self, entityid) -> str:
        missing = next(
            (d.clone_of for d in self._derivations.get(entityid, []) if d.is_clone), entityid
        )
        if missing != entityid:
            return f"waiting for '{missing}' to be registered"
        return "extension declared before the entity itself"

    def _apply_config_derivations(self, section) -> None:
        """Register derivations declared in the ``dataentities:`` section
        (``clone_of``, ``add_columns``); config-sourced ones are replaced
        wholesale on every pass so re-applying stays idempotent."""
        for target in list(self._derivations):
            self._derivations[target] = [
                d for d in self._derivations[target] if d.source != "config"
            ]
            if not self._derivations[target]:
                del self._derivations[target]
        for entityid, entry in derivation_entries(section).items():
            derivation = EntityDerivation(
                clone_of=entry.get("clone_of"),
                add_columns=tuple(entry.get("add_columns") or ()),
                source="config",
            )
            if derivation.is_clone and entityid in self._raw_params:
                raise DerivationError(
                    f"Config declares entity '{entityid}' as clone_of "
                    f"'{derivation.clone_of}', but it is also registered in code."
                )
            if derivation.is_clone and any(d.is_clone for d in self._derivations.get(entityid, [])):
                raise DerivationError(
                    f"Config declares entity '{entityid}' as clone_of "
                    f"'{derivation.clone_of}', but code already clones it from another source."
                )
            self._derivations.setdefault(entityid, []).append(derivation)

    def apply_config_overrides(self, config_service: ConfigService) -> None:
        """Overlay the ``dataentities:``/``dataentities-bytag:`` config
        sections onto registered entities.

        Rebuilds every explicitly-registered entity from its original
        registration params through ``dataentities-bytag:``'s tag-value
        rules first, then ``dataentities:``'s id-glob patterns
        (``ConfigPatternMatcher``/``TagRuleMatcher`` semantics: mappings
        deep-merge, scalars and lists replace, exact > single-wildcard >
        multi-wildcard for id patterns), re-validates the overlaid metadata,
        and re-derives SCD2 current-row companions from the result
        (desired-state convergence). Tag rules apply first so a specific
        ``dataentities:`` entry can still override a broader tag-based
        default for one entity. A missing or empty section compiles to a
        matcher with zero rules/patterns, making that pass a structural
        no-op. Both matchers persist on the manager so later registrations
        self-overlay.

        Idempotent and safely re-callable (hot reload: ``reload()`` config,
        then call again). Not synchronized with running DAGs — a concurrent
        run may observe a mix of old and new metadata across entities (same
        caveat as ``tag_overrides``).
        """
        section = config_service.get("dataentities")
        self._apply_config_derivations(section)
        self._matcher = ConfigPatternMatcher(strip_derivation_keys(section))
        self._tag_matcher = TagRuleMatcher(config_service.get("dataentities-bytag"))
        declared = set(self._declared_ids())
        # A clone that only config declared and that this pass no longer
        # declares is gone: drop its stale metadata (companions are
        # converged below) and any pending record for ids nothing derives.
        for stale in [
            entityid
            for entityid, metadata in self.registry.items()
            if entityid not in declared and not (metadata.tags or {}).get("scd.companion_of")
        ]:
            del self.registry[stale]
        for target in list(self._pending):
            if target not in self._derivations:
                del self._pending[target]
        for entityid in declared:
            self._rebuild(entityid)
        self._converge_scd2_companions()
        _ENTITY_LOGGER.debug("Config overrides applied to %s entit(y/ies)", len(self._raw_params))

    def resolve_secret_tags(self, secret_provider) -> List[str]:
        """Resolve ``@secret:``/``@secret `` references embedded directly in
        an entity's own ``tags=`` registration param (e.g. ``@entity(...,
        tags={"provider.eventhub.connectionString": "@secret:scope:key"})``).

        These never pass through Dynaconf's config tree, so bootstrap's
        ``_resolve_and_validate_secrets`` (which only walks
        ``config_service.dynaconf``) cannot see or resolve them. Mutates
        ``self._raw_params`` in place so a subsequent
        ``apply_config_overrides()`` call re-derives metadata with the
        resolved values instead of the literal reference.

        Returns a list of ``"<entityid>.tags.<key>"`` paths that failed to
        resolve (never the attempted values, to avoid ever interpolating a
        secret into a log/error message) -- callers should treat any
        non-empty result as a hard failure, mirroring
        ``_resolve_and_validate_secrets``'s post-platform-init contract.
        """
        from kindling.config_loaders import (
            _is_unresolved_secret_reference,
            resolve_secret_value,
        )

        failures: List[str] = []
        for entityid, raw_params in self._raw_params.items():
            tags = raw_params.get("tags")
            if not isinstance(tags, dict):
                continue
            for key, value in list(tags.items()):
                if not _is_unresolved_secret_reference(value):
                    continue
                try:
                    tags[key] = resolve_secret_value(value, secret_provider)
                except Exception:
                    failures.append(f"{entityid}.tags.{key}")
        # Tags supplied through clone()/extend() live on the derivation, not
        # in _raw_params; resolve them the same way and rebuild the targets.
        touched = []
        for entityid, derivations in self._derivations.items():
            for index, derivation in enumerate(derivations):
                if not derivation.tags:
                    continue
                resolved = dict(derivation.tags)
                changed = False
                for key, value in resolved.items():
                    if not _is_unresolved_secret_reference(value):
                        continue
                    try:
                        resolved[key] = resolve_secret_value(value, secret_provider)
                        changed = True
                    except Exception:
                        failures.append(f"{entityid}.tags.{key}")
                if changed:
                    derivations[index] = dataclasses.replace(derivation, tags=resolved)
                    touched.append(entityid)
        for entityid in touched:
            if entityid not in self._pending:
                self._rebuild(entityid)
        return failures

    def _build_metadata(self, entityid, raw_params):
        """Construct EntityMetadata from raw registration params plus any
        config overrides matching the entity's own tags
        (``dataentities-bytag:``) or its id (``dataentities:``). Tag rules
        apply first, id-glob patterns apply on top (no matchers yet -> raw
        as-is)."""
        if self._matcher is None and self._tag_matcher is None:
            return EntityMetadata(entityid, **raw_params)

        base = {
            key: value
            for key, value in raw_params.items()
            if key not in self._NON_OVERRIDABLE_FIELDS
        }
        if self._tag_matcher is not None:
            entity_tags = raw_params.get("tags") or {}
            base = self._tag_matcher.resolve_overrides(entity_tags, base)
        resolved = self._matcher.resolve_overrides(entityid, base) if self._matcher else base
        overridable = {field.name for field in fields(EntityMetadata)} - set(
            self._NON_OVERRIDABLE_FIELDS
        )
        params = {}
        dropped = []
        for key, value in resolved.items():
            if key in self._NON_OVERRIDABLE_FIELDS:
                dropped.append(key)
            elif key in overridable or key in raw_params:
                params[key] = value
            else:
                # Underscore keys (_enabled, _remove_tags, ...) ride along
                # inertly until tag management interprets them; anything else
                # is an unknown config key. Neither reaches the dataclass.
                dropped.append(key)
        if dropped:
            _ENTITY_LOGGER.debug(
                "Entity %s: config override keys not applied to metadata: %s",
                entityid,
                sorted(dropped),
            )
        for key in self._NON_OVERRIDABLE_FIELDS:
            if key in raw_params:
                params[key] = raw_params[key]

        declared_merge_columns = raw_params.get("merge_columns")
        if declared_merge_columns and not params.get("merge_columns"):
            # merge_columns is a plain overridable field (see
            # _NON_OVERRIDABLE_FIELDS) and list values REPLACE rather than
            # merge (ConfigPatternMatcher/TagRuleMatcher semantics), so a
            # dataentities:/dataentities-bytag: rule that happens to carry a
            # merge_columns key -- even incidentally -- silently drops a
            # validly-declared merge key with no other signal. `kindling app
            # validate`'s resulting "delta entity missing merge_columns"
            # failure otherwise looks like the framework ignoring code that
            # is correct, so surface the actual cause here.
            _ENTITY_LOGGER.warning(
                "Entity %s: dataentities:/dataentities-bytag: config overrides "
                "replaced declared merge_columns %s with %r",
                entityid,
                declared_merge_columns,
                params.get("merge_columns"),
            )
        return EntityMetadata(entityid, **params)

    def _validate_entity(self, entity: EntityMetadata) -> None:
        """Run registration-time validations on (possibly overlaid) metadata."""
        try:
            # Derived-vs-state exclusivity first: a derived entity carrying
            # state tags should fail with "does not apply to a derived
            # dataset", not with a downstream state-vocabulary complaint.
            _validate_derived_config(entity)
            _validate_scd_config(entity)
            _validate_surrogate_config(entity)
            _validate_write_mode_tag(entity)
            _validate_schema_drift_tag(entity)
        except ValueError as error:
            id_override = self._matcher is not None and self._matcher.get_matching_overrides(
                entity.entityid
            )
            tag_override = (
                self._tag_matcher is not None
                and self._tag_matcher.get_matching_overrides(entity.tags or {})
            )
            if id_override or tag_override:
                raise ValueError(
                    f"Config overrides applied to entity '{entity.entityid}' "
                    f"produced invalid metadata: {error}"
                ) from error
            raise

    def _register_scd2_current_companion(self, base: EntityMetadata, cfg: SCDConfig) -> None:
        """Register the read-only current-row companion for an SCD2 entity."""
        if cfg.current_entity_id in self.registry:
            return

        companion = self._build_companion_metadata(base, cfg)
        self.registry[cfg.current_entity_id] = companion
        self.emit(
            "entity.registered",
            entity_id=cfg.current_entity_id,
            entity_name=companion.name,
        )
        self.emit(
            "entity.scd2_companion_registered",
            entity_id=base.entityid,
            companion_entity_id=cfg.current_entity_id,
        )

    def _build_companion_metadata(self, base: EntityMetadata, cfg: SCDConfig) -> EntityMetadata:
        """Derive the current-row companion's metadata from its (overlaid) base.

        The companion id itself goes through pattern resolution with the
        derived params as its base, so ``dataentities:`` patterns can target
        companions directly — matching how ``entity_tags`` can target them
        per-read. Companions stay unvalidated (parity with the previous
        ``replace()`` derivation).
        """
        companion_tags = {
            key: value for key, value in (base.tags or {}).items() if not key.startswith("scd.")
        }
        companion_tags.update(
            {
                "scd.companion_of": base.entityid,
                "scd.view_type": "current",
                "provider.read_only": "true",
                "provider_type": "current_view",
            }
        )
        derived_params = {
            metadata_field.name: getattr(base, metadata_field.name)
            for metadata_field in fields(EntityMetadata)
            if metadata_field.name != "entityid"
        }
        derived_params["name"] = f"{base.name} (current)"
        derived_params["tags"] = companion_tags
        return self._build_metadata(cfg.current_entity_id, derived_params)

    def _converge_scd2_companions(self) -> None:
        """Re-derive SCD2 current-row companions after an overlay pass.

        Companions are derived state (never in ``_raw_params``), so each
        pass converges them onto the overlaid bases: SCD newly enabled by
        config creates the companion (with registration signals), SCD
        disabled or a changed ``current_entity_id`` removes the stale
        auto-created one, and a still-desired companion is rebuilt in place
        silently (no signal re-emission).
        """
        desired = {}
        declared = set(self._declared_ids())
        for entityid in declared:
            base = self.registry.get(entityid)
            if base is None:
                continue  # pending derivation
            cfg = scd_config_from_tags(base)
            if cfg.enabled and cfg.current_entity_id not in declared:
                desired[cfg.current_entity_id] = (base, cfg)

        stale = [
            companion_id
            for companion_id, metadata in self.registry.items()
            if companion_id not in declared
            and (metadata.tags or {}).get("scd.companion_of")
            and companion_id not in desired
        ]
        for companion_id in stale:
            del self.registry[companion_id]
            _ENTITY_LOGGER.debug("Removed stale SCD2 companion entity %s", companion_id)

        for companion_id, (base, cfg) in desired.items():
            already_registered = companion_id in self.registry
            self.registry[companion_id] = self._build_companion_metadata(base, cfg)
            if not already_registered:
                self.emit(
                    "entity.registered",
                    entity_id=companion_id,
                    entity_name=self.registry[companion_id].name,
                )
                self.emit(
                    "entity.scd2_companion_registered",
                    entity_id=base.entityid,
                    companion_entity_id=companion_id,
                )

    def get_entity_ids(self) -> List[str]:
        return list(self.registry.keys())

    def get_entity_definition(self, name):
        """Get entity definition with tag overrides applied.

        A target whose derivation is still pending (its clone_of source was
        never registered) raises rather than reading as "unknown entity".

        Tags layer lowest to highest precedence:

        1. declared registration params,
        2. ``dataentities:`` config patterns — durable file config, baked
           into the stored metadata by ``apply_config_overrides`` (and by
           registration itself once the matcher is set),
        3. exact-id config map (``ConfigService.set_entity_tags``), merged
           per-read because it is runtime-mutable (e.g. the ADX provider's
           per-run windowing loop),
        4. run-scoped ``tag_overrides`` context (JIT parameters), merged
           per-read and restored on exit.

        Layers 3 and 4 merge at retrieval time; that also allows config to
        be loaded before or after entity registration.
        """
        if name in self._pending:
            raise DerivationError(f"Entity '{name}' is not available: {self._pending[name]}.")
        base_entity = self.registry.get(name)
        if base_entity is None:
            return None

        merged_tags = dict(base_entity.tags)

        if self.config_service:
            config_tags = self.config_service.get_entity_tags(name)
            if config_tags:
                merged_tags.update(config_tags)

        overlay = self._tag_overlays.get(name)
        if overlay:
            merged_tags.update(overlay)

        if merged_tags == base_entity.tags:
            return base_entity
        return replace(base_entity, tags=merged_tags)
