from __future__ import annotations

import re
from typing import Any, Optional

from kindling.spark_session import get_or_create_spark_session


def _coerce_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)

    lowered = str(value).strip().lower()
    if lowered in {"true", "1", "yes", "y", "on"}:
        return True
    if lowered in {"false", "0", "no", "n", "off"}:
        return False
    return None


def get_feature_bool(config_service, key: str, default: Optional[bool] = None) -> Optional[bool]:
    """Resolve a feature flag from config.

    Resolution order:
    1. Static override:  kindling.features.<key>
    2. Computed value:   kindling.runtime.features.<key>
    3. Default
    """
    static_raw = config_service.get(f"kindling.features.{key}")
    static_val = _coerce_bool(static_raw)
    if static_val is not None:
        return static_val

    runtime_raw = config_service.get(f"kindling.runtime.features.{key}")
    runtime_val = _coerce_bool(runtime_raw)
    if runtime_val is not None:
        return runtime_val

    return default


def set_runtime_feature(config_service, key: str, value: Any) -> None:
    config_service.set(f"kindling.runtime.features.{key}", value)


def _supports_databricks_volumes(spark) -> bool:
    """Best-effort detection for Databricks UC volume path support."""
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
        dbutils.fs.ls("/Volumes")
        return True
    except Exception:
        return False


def _detect_databricks_uc_enabled(spark) -> bool:
    """Best-effort detection for Unity Catalog support on Databricks."""
    try:
        current_catalog_rows = spark.sql("SELECT current_catalog() AS catalog").collect()
        current_catalog = (
            str(current_catalog_rows[0][0]).strip().lower() if current_catalog_rows else ""
        )
    except Exception:
        current_catalog = ""

    try:
        catalog_rows = spark.sql("SHOW CATALOGS").collect()
        catalogs = [str(row[0]).strip().lower() for row in catalog_rows if row and row[0]]
    except Exception:
        catalogs = []

    if current_catalog and current_catalog != "spark_catalog":
        return True
    if catalogs and any(catalog != "spark_catalog" for catalog in catalogs):
        return True
    return False


def discover_runtime_features(config_service, logger=None) -> None:
    """Populate kindling.runtime.features.* with best-effort runtime detection.

    This should run once during startup. All keys written here are optional and
    can be overridden by kindling.features.*.
    """
    try:
        spark = get_or_create_spark_session()
    except Exception as e:
        if logger:
            logger.debug(f"Skipping runtime feature discovery (Spark unavailable): {e}")
        return

    try:
        set_runtime_feature(config_service, "spark.version", getattr(spark, "version", None))
    except Exception:
        pass

    # Detect whether Spark SQL parser understands ALTER TABLE ... CLUSTER BY syntax.
    try:
        spark.sql("EXPLAIN ALTER TABLE `__kindling__nonexistent__` CLUSTER BY (`x`)").collect()
        set_runtime_feature(config_service, "delta.cluster_by", True)
    except Exception as e:
        msg = str(e).lower()
        if "parseexception" in msg or ("mismatched input" in msg and "cluster" in msg):
            set_runtime_feature(config_service, "delta.cluster_by", False)
        else:
            # If we got past parsing, other failures (table not found, etc.) still
            # imply the engine understands the syntax.
            set_runtime_feature(config_service, "delta.cluster_by", True)

    # Databricks-specific: detect runtime version and whether CLUSTER BY AUTO parses.
    # This enables liquid clustering "auto" mode, where the engine chooses clustering columns.
    #
    # NOTE: Databricks exposes the runtime in spark.conf "spark.databricks.clusterUsageTags.sparkVersion"
    # with values like "15.4.x-scala2.12" (format varies by cloud/cluster type).
    dbr_version_raw = None
    try:
        dbr_version_raw = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
    except Exception:
        dbr_version_raw = None

    major_minor = None
    if isinstance(dbr_version_raw, str) and dbr_version_raw.strip():
        match = re.search(r"(\\d+)\\.(\\d+)", dbr_version_raw)
        if match:
            try:
                major_minor = (int(match.group(1)), int(match.group(2)))
            except Exception:
                major_minor = None
        set_runtime_feature(config_service, "databricks.runtime_version", dbr_version_raw.strip())
        if major_minor:
            set_runtime_feature(config_service, "databricks.runtime_major", major_minor[0])
            set_runtime_feature(config_service, "databricks.runtime_minor", major_minor[1])

        uc_enabled = _detect_databricks_uc_enabled(spark)
        volumes_enabled = uc_enabled and _supports_databricks_volumes(spark)
        set_runtime_feature(config_service, "databricks.uc_enabled", uc_enabled)
        set_runtime_feature(config_service, "databricks.volumes_enabled", volumes_enabled)
        set_runtime_feature(
            config_service,
            "databricks.any_file_required_for_bootstrap",
            not volumes_enabled,
        )
        set_runtime_feature(
            config_service,
            "databricks.name_mode_catalog_qualified",
            uc_enabled,
        )

    # Default to False unless we can positively detect support.
    auto_ok = False
    if major_minor is not None and major_minor >= (15, 2):
        try:
            spark.sql("EXPLAIN ALTER TABLE `__kindling__nonexistent__` CLUSTER BY AUTO").collect()
            auto_ok = True
        except Exception as e:
            msg = str(e).lower()
            if "parseexception" in msg or ("mismatched input" in msg and "cluster" in msg):
                auto_ok = False
            else:
                # Non-parse failures still imply syntax support.
                auto_ok = True
    set_runtime_feature(config_service, "delta.auto_clustering", auto_ok)
