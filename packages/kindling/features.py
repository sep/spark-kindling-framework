from __future__ import annotations

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

