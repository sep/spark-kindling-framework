import logging
import os
import shutil
from pathlib import Path

from pyspark.sql import SparkSession

# Can't use DI at this point as this is needed before DI is available
_logger = logging.getLogger(__name__)

_TRUE_VALUES = {"1", "true", "yes", "on"}
_DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
_ABFSS_LOCAL_AUTH_JAR = Path("/tmp/hadoop-jars/kindling-abfss-local-auth.jar")


def safe_get_global(var_name: str, default=None):
    try:
        import __main__

        return getattr(__main__, var_name, default)
    except Exception:
        return default


def _env_flag_enabled(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in _TRUE_VALUES


def _abfss_az_cli_jar() -> str | None:
    """Return the local auth JAR path if az is on PATH and the JAR exists, else None."""
    if shutil.which("az") and _ABFSS_LOCAL_AUTH_JAR.exists():
        return str(_ABFSS_LOCAL_AUTH_JAR)
    return None


def _apply_abfss_jar(builder: "SparkSession.Builder") -> "SparkSession.Builder":
    """Add the ABFSS local auth JAR to the builder if az is available."""
    jar = _abfss_az_cli_jar()
    if jar:
        existing = builder._options.get("spark.jars", "")
        jars = ",".join(filter(None, [existing, jar]))
        builder = builder.config("spark.jars", jars)
    return builder


def _create_delta_enabled_session():
    """Create a local Spark session with Delta Lake JVM support configured."""
    try:
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:
        raise RuntimeError(
            "Local Delta Spark support was requested, but delta-spark is not installed. "
            "Install Kindling with the standalone extra or run `pip install delta-spark`."
        ) from exc

    builder = (
        SparkSession.builder.config("spark.sql.extensions", _DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", _DELTA_CATALOG)
        .config("spark.ui.enabled", os.getenv("KINDLING_SPARK_UI_ENABLED", "false"))
    )
    builder = _apply_abfss_jar(builder)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def create_session():
    _logger.debug("Creating new Spark session")

    # Get existing session or create with default settings.
    # Note: In notebook/job environments, session typically already exists.
    if _env_flag_enabled("KINDLING_SPARK_ENABLE_DELTA"):
        spark_session = _create_delta_enabled_session()
    else:
        builder = _apply_abfss_jar(SparkSession.builder)
        spark_session = builder.getOrCreate()

    # Store in __main__ so other code can find it
    try:
        import __main__

        __main__.spark = spark_session
    except Exception:
        pass

    return spark_session


def get_or_create_spark_session():
    return safe_get_global("spark") or create_session()
