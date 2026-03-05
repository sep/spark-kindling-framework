from __future__ import annotations

from typing import Optional, Tuple

from injector import inject

from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.injection import GlobalInjector
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_session import get_or_create_spark_session


def _normalize_table_leaf(name: str) -> str:
    # Keep this simple and predictable across platforms.
    return str(name).replace(".", "_").replace("-", "_")


def _get_entity_id(entity) -> str:
    return getattr(entity, "entityid", None) or getattr(entity, "name", None) or str(entity)


def _quote_ident(part: str) -> str:
    # Spark SQL identifier quoting. Safe for catalog/schema/table parts.
    return f"`{part.replace('`', '``')}`"


def _get_current_namespace() -> Tuple[Optional[str], Optional[str]]:
    """Best-effort current (catalog, schema) for engines that support it."""
    spark = get_or_create_spark_session()
    catalog = None
    schema = None

    try:
        row = (
            spark.sql("SELECT current_catalog() AS catalog, current_database() AS schema")
            .select("catalog", "schema")
            .first()
        )
        if row is not None:
            try:
                catalog = row["catalog"]
                schema = row["schema"]
            except Exception:
                catalog = getattr(row, "catalog", None)
                schema = getattr(row, "schema", None)
    except Exception:
        try:
            row = spark.sql("SELECT current_database() AS schema").select("schema").first()
            if row is not None:
                try:
                    schema = row["schema"]
                except Exception:
                    schema = getattr(row, "schema", None)
        except Exception:
            pass

    return catalog, schema


@GlobalInjector.singleton_autobind(EntityNameMapper)
class ConfigDrivenEntityNameMapper(EntityNameMapper):
    """Config-driven default EntityNameMapper for all platforms.

    Conventions:
    - Per-entity override via tag: `provider.table_name`
    - Default leaf name is derived from entityid: dots and hyphens -> underscores
    - Optional namespace overrides via config:
      - `kindling.storage.table_catalog`
      - `kindling.storage.table_schema`
      - `kindling.storage.table_name_prefix`
    """

    @inject
    def __init__(self, config: ConfigService, tp: PythonLoggerProvider):
        self.config = config
        self.logger = tp.get_logger("ConfigDrivenEntityNameMapper")

    def _config_namespace(self) -> Tuple[Optional[str], Optional[str]]:
        catalog = self.config.get("kindling.storage.table_catalog")
        schema = self.config.get("kindling.storage.table_schema")

        # Backward-compatible fallbacks (older platform-specific keys).
        if catalog is None:
            catalog = self.config.get("kindling.databricks.catalog") or self.config.get(
                "kindling.fabric.catalog"
            )
        if schema is None:
            schema = (
                self.config.get("kindling.databricks.schema")
                or self.config.get("kindling.fabric.schema")
                or self.config.get("kindling.synapse.schema")
            )

        if catalog is None and schema is None:
            # Last resort: ask Spark session for current namespace.
            return _get_current_namespace()

        return catalog, schema

    def get_table_name(self, entity):
        entity_tags = getattr(entity, "tags", {}) or {}
        explicit_table_name = entity_tags.get("provider.table_name")
        if explicit_table_name:
            return explicit_table_name

        leaf = _normalize_table_leaf(_get_entity_id(entity))
        prefix = self.config.get("kindling.storage.table_name_prefix") or ""
        if prefix:
            leaf = f"{prefix}{leaf}"

        catalog, schema = self._config_namespace()

        if catalog and schema:
            return f"{catalog}.{schema}.{leaf}"
        if schema:
            return f"{schema}.{leaf}"
        return leaf


@GlobalInjector.singleton_autobind(EntityPathLocator)
class ConfigDrivenEntityPathLocator(EntityPathLocator):
    """Config-driven default EntityPathLocator for all platforms.

    Conventions:
    - Per-entity override via tag: `provider.path`
    - Default root via config: `kindling.storage.table_root` (fallback "Tables")
    - Default suffix uses entityid path segmentation: dots -> slashes
    """

    @inject
    def __init__(self, config: ConfigService, tp: PythonLoggerProvider):
        self.config = config
        self.logger = tp.get_logger("ConfigDrivenEntityPathLocator")

    def get_table_path(self, entity):
        entity_tags = getattr(entity, "tags", {}) or {}
        provider_path = entity_tags.get("provider.path")
        if provider_path:
            return provider_path

        table_root = self.config.get("kindling.storage.table_root")
        if not table_root:
            # Backward-compatible fallbacks.
            table_root = (
                self.config.get("kindling.fabric.table_root")
                or self.config.get("kindling.synapse.table_root")
                or self.config.get("kindling.databricks.table_root")
                or "Tables"
            )

        entity_id = _get_entity_id(entity)
        parts = [part.strip() for part in str(entity_id).split(".") if part.strip()]
        suffix = "/".join(parts) if parts else str(entity_id).replace(".", "/")

        return f"{str(table_root).rstrip('/')}/{suffix}"
