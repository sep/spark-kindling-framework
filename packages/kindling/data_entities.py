import logging
import time
from abc import ABC, abstractmethod
from dataclasses import MISSING, dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_log_provider import *
from pyspark.sql import DataFrame

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
            else:
                default_tags["derived.replace_keys"] = ",".join(replace_keys)
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
    def get_entity_ids(self):
        pass

    @abstractmethod
    def get_entity_definition(self, name):
        pass


@GlobalInjector.singleton_autobind()
class DataEntityManager(DataEntityRegistry, SignalEmitter):
    """Manages entity registrations with signal emissions."""

    @inject
    def __init__(
        self, signal_provider: SignalProvider = None, config_service: ConfigService = None
    ):
        self._init_signal_emitter(signal_provider)
        self.registry = {}
        self.config_service = config_service

    def register_entity(self, entityid, **decorator_params):
        entity = EntityMetadata(entityid, **decorator_params)
        # Derived-vs-state exclusivity first: a derived entity carrying
        # state tags should fail with "does not apply to a derived
        # dataset", not with a downstream state-vocabulary complaint.
        _validate_derived_config(entity)
        _validate_scd_config(entity)
        _validate_write_mode_tag(entity)

        self.registry[entityid] = entity
        self.emit(
            "entity.registered",
            entity_id=entityid,
            entity_name=decorator_params.get("name", entityid),
        )

        scd_config = scd_config_from_tags(entity)
        if scd_config.enabled:
            self._register_scd2_current_companion(entity, scd_config)

    def _register_scd2_current_companion(self, base: EntityMetadata, cfg: SCDConfig) -> None:
        """Register the read-only current-row companion for an SCD2 entity."""
        if cfg.current_entity_id in self.registry:
            return

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
        companion = replace(
            base,
            entityid=cfg.current_entity_id,
            name=f"{base.name} (current)",
            tags=companion_tags,
        )
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

    def get_entity_ids(self):
        return self.registry.keys()

    def get_entity_definition(self, name):
        """Get entity definition with config-based tag overrides applied.

        Returns entity with tags merged at retrieval time, allowing config
        to be loaded before or after entity registration.
        """
        base_entity = self.registry.get(name)
        if base_entity is None:
            return None

        # Merge config-based tag overrides if config service is available
        if self.config_service:
            config_tags = self.config_service.get_entity_tags(name)
            if config_tags:
                # Merge tags: config overrides base
                merged_tags = {**base_entity.tags, **config_tags}
                return replace(base_entity, tags=merged_tags)

        return base_entity
