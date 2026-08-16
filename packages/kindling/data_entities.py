import logging
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import MISSING, dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    MapType,
    StructField,
    StructType,
    TimestampType,
)

from kindling.config_patterns import ConfigPatternMatcher, TagRuleMatcher
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_log_provider import *

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


def scd_config_from_tags(entity: EntityMetadata) -> SCDConfig:
    """Extract and validate SCD configuration from an entity's tags."""
    tags = entity.tags or {}
    scd_type = tags.get("scd.type", "").strip()
    default_config = SCDConfig(
        enabled=False,
        tracked_columns=None,
        effective_from_column="__effective_from",
        effective_to_column="__effective_to",
        is_current_column="__is_current",
        current_entity_id=f"{entity.entityid}.current",
        routing_key_method="hash",
    )

    if not scd_type:
        return default_config

    if scd_type != "2":
        raise ValueError(
            f"Entity '{entity.entityid}': scd.type must be '2' "
            f"(only SCD Type 2 is supported), got '{scd_type}'"
        )

    tracked_raw = tags.get("scd.tracked", "").strip()
    tracked_columns = (
        [column.strip() for column in tracked_raw.split(",") if column.strip()]
        if tracked_raw
        else None
    )

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

    return " OR ".join(
        [
            f"({_comparable(column, source_alias)} != "
            f"{_comparable(column, target_alias)} OR "
            f"({quote_sql_identifier(column, source_alias)} IS NULL) != "
            f"({quote_sql_identifier(column, target_alias)} IS NULL))"
            for column in tracked_columns
        ]
    )


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


def _validate_scd_config(entity: EntityMetadata) -> None:
    """Validate SCD tag configuration at registration time."""
    cfg = scd_config_from_tags(entity)
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
        merged_tags = {"provider_type": "view", **(tags or {})}

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
    def entity(cls, **decorator_params):
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
        self._raw_params[entityid] = dict(decorator_params)
        entity = self._build_metadata(entityid, decorator_params)
        self._validate_entity(entity)

        self.registry[entityid] = entity
        self.emit(
            "entity.registered",
            entity_id=entityid,
            entity_name=entity.name,
        )

        scd_config = scd_config_from_tags(entity)
        if scd_config.enabled:
            self._register_scd2_current_companion(entity, scd_config)

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
        self._matcher = ConfigPatternMatcher(config_service.get("dataentities"))
        self._tag_matcher = TagRuleMatcher(config_service.get("dataentities-bytag"))
        for entityid, raw_params in self._raw_params.items():
            entity = self._build_metadata(entityid, raw_params)
            self._validate_entity(entity)
            self.registry[entityid] = entity
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
        return EntityMetadata(entityid, **params)

    def _validate_entity(self, entity: EntityMetadata) -> None:
        """Run registration-time validations on (possibly overlaid) metadata."""
        try:
            # Derived-vs-state exclusivity first: a derived entity carrying
            # state tags should fail with "does not apply to a derived
            # dataset", not with a downstream state-vocabulary complaint.
            _validate_derived_config(entity)
            _validate_scd_config(entity)
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
        for entityid in self._raw_params:
            base = self.registry.get(entityid)
            cfg = scd_config_from_tags(base)
            if cfg.enabled and cfg.current_entity_id not in self._raw_params:
                desired[cfg.current_entity_id] = (base, cfg)

        stale = [
            companion_id
            for companion_id, metadata in self.registry.items()
            if companion_id not in self._raw_params
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
