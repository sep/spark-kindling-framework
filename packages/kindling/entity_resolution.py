from __future__ import annotations

from typing import Optional, Tuple

from injector import inject

from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.features import get_feature_bool
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
    """Best-effort current (catalog, schema) for engines that support it.

    When ``kindling.features.databricks.uc_enabled`` is explicitly ``False``
    we skip the ``current_catalog()`` query (it is meaningless without Unity
    Catalog) and only resolve the current database/schema.
    """
    spark = get_or_create_spark_session()
    catalog = None
    schema = None

    # Check whether UC is available.  We grab the ConfigService from the DI
    # container (best-effort — during very early bootstrap it may not exist yet).
    uc_enabled: Optional[bool] = None
    try:
        cs = GlobalInjector.get(ConfigService)
        uc_enabled = get_feature_bool(cs, "databricks.uc_enabled")
    except Exception:
        pass

    if uc_enabled is not False:
        # UC is available (or unknown) — try the full catalog + schema query.
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
            return catalog, schema
        except Exception:
            pass

    # UC disabled or catalog query failed — resolve database/schema only.
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
    - If storage namespace config is present, leaf name is derived from entityid: dots and hyphens -> underscores
    - If no storage namespace config is present, entity IDs are treated as already-qualified names:
      - x.y.z -> catalog.schema.table
      - y.z   -> schema.table (uses current catalog if available)
    - Optional namespace overrides via config:
      - `kindling.storage.table_catalog`
      - `kindling.storage.table_schema`
      - `kindling.storage.table_name_prefix`
    """

    @inject
    def __init__(self, config: ConfigService, tp: PythonLoggerProvider):
        self.config = config
        self.logger = tp.get_logger("ConfigDrivenEntityNameMapper")

    def _clean_config_value(self, value: object) -> Optional[str]:
        if value is None:
            return None
        if not isinstance(value, str):
            value = str(value)
        cleaned = value.strip()
        if not cleaned:
            return None
        if cleaned.lower() in {"auto", "none", "null"}:
            return None
        return cleaned

    def _has_storage_namespace_config(self) -> bool:
        # Treat platform fallbacks as config too. The key behavior change is that we no longer
        # infer catalog/schema from Spark when config is absent; instead we interpret entity IDs
        # as qualified names.
        for key in (
            "kindling.storage.table_catalog",
            "kindling.storage.table_schema",
            "kindling.storage.table_name_prefix",
            "kindling.databricks.catalog",
            "kindling.databricks.schema",
            "kindling.fabric.catalog",
            "kindling.fabric.schema",
            "kindling.synapse.schema",
        ):
            if self._clean_config_value(self.config.get(key)) is not None:
                return True
        return False

    def _config_namespace(self) -> Tuple[Optional[str], Optional[str]]:
        catalog = self._clean_config_value(self.config.get("kindling.storage.table_catalog"))
        schema = self._clean_config_value(self.config.get("kindling.storage.table_schema"))

        # Backward-compatible fallbacks (older platform-specific keys).
        if catalog is None:
            catalog = self._clean_config_value(self.config.get("kindling.databricks.catalog")) or (
                self._clean_config_value(self.config.get("kindling.fabric.catalog"))
            )
        if schema is None:
            schema = (
                self._clean_config_value(self.config.get("kindling.databricks.schema"))
                or self._clean_config_value(self.config.get("kindling.fabric.schema"))
                or self._clean_config_value(self.config.get("kindling.synapse.schema"))
            )

        # Important: do not fall back to Spark current namespace here. If config is absent,
        # we should interpret entity IDs as already-qualified names.
        return catalog, schema

    def _infer_namespace_from_volume_path(self) -> Tuple[Optional[str], Optional[str]]:
        """Infer catalog and schema from a /Volumes/{catalog}/{schema}/... table_root.

        Databricks Unity Catalog Volumes use the path structure
        /Volumes/{catalog}/{schema}/{volume}/...  When table_root points into a
        Volume, the catalog and schema can be derived from the path.  This lets
        us qualify unqualified 1-part table names so that saveAsTable and
        spark.read.table resolve to the same catalog regardless of whatever the
        SparkSession's active catalog happens to be.
        """
        root = None
        for key in (
            "kindling.storage.table_root",
            "kindling.databricks.table_root",
            "kindling.fabric.table_root",
            "kindling.synapse.table_root",
        ):
            root = self._clean_config_value(self.config.get(key))
            if root:
                break
        if not root:
            return None, None
        parts = root.strip("/").split("/")
        if len(parts) >= 3 and parts[0].lower() == "volumes":
            return parts[1], parts[2]
        return None, None

    def get_table_name(self, entity):
        entity_tags = getattr(entity, "tags", {}) or {}
        explicit_table_name = entity_tags.get("provider.table_name")
        if explicit_table_name:
            return explicit_table_name

        entity_id = _get_entity_id(entity)

        # If no namespace config is provided, treat entity IDs as already-qualified names.
        # x.y.z -> catalog.schema.table; y.z -> schema.table (default catalog if available).
        if not self._has_storage_namespace_config():
            raw = str(entity_id).strip()
            parts = [p.strip() for p in raw.split(".") if p.strip()]
            if len(parts) == 3:
                return ".".join(parts)
            if len(parts) == 2:
                catalog, _schema = _get_current_namespace()
                # spark_catalog is the built-in Hive catalog, not a real UC catalog.
                # Treat it as absent so entity IDs stay as schema.table (matches
                # features.py which also excludes spark_catalog from UC detection).
                if catalog and catalog.lower() != "spark_catalog":
                    return f"{catalog}.{parts[0]}.{parts[1]}"
                return ".".join(parts)
            # One-part name: qualify with catalog.schema inferred from the Volume
            # path when table_root is a /Volumes/{catalog}/{schema}/... path.
            # Databricks UC resolves unqualified names against the session's current
            # catalog, which can differ between write (saveAsTable) and read
            # (spark.read.table), causing TABLE_OR_VIEW_NOT_FOUND on reads even
            # when the write succeeded.  A fully-qualified 3-part name is always
            # deterministic regardless of the session's active catalog.
            vol_catalog, vol_schema = self._infer_namespace_from_volume_path()
            if vol_catalog and vol_schema:
                leaf = _normalize_table_leaf(raw)
                return f"{vol_catalog}.{vol_schema}.{leaf}"
            return raw

        leaf = _normalize_table_leaf(entity_id)
        prefix = (
            self._clean_config_value(self.config.get("kindling.storage.table_name_prefix")) or ""
        )
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
