import logging
import time
from abc import ABC, abstractmethod
from dataclasses import MISSING, dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from pyspark.sql import DataFrame

from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_log_provider import *

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
    def read_entity_since_version(self, entity, since_version):
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

    return SCDConfig(
        enabled=True,
        tracked_columns=tracked_columns,
        effective_from_column=tags.get("scd.effective_from_col", "__effective_from"),
        effective_to_column=tags.get("scd.effective_to_col", "__effective_to"),
        is_current_column=tags.get("scd.current_col", "__is_current"),
        current_entity_id=tags.get("scd.current_entity_id", f"{entity.entityid}.current"),
        routing_key_method=routing_key_method,
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


class DataEntities:

    deregistry = None

    # [implementer] expose public test reset API — TASK-20260430-001
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
        return None

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

        return None


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
        _validate_scd_config(entity)

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
