import logging

from pyspark.sql import SparkSession

# Can't use DI at this point as this is needed before DI is available
_logger = logging.getLogger(__name__)


def safe_get_global(var_name: str, default=None):
    try:
        import __main__

        return getattr(__main__, var_name, default)
    except Exception:
        return default


def create_session():
    _logger.debug("Creating new Spark session")

    # Get existing session or create with default settings
    # Note: In notebook/job environments, session typically already exists
    spark_session = SparkSession.builder.getOrCreate()

    # Store in __main__ so other code can find it
    try:
        import __main__

        __main__.spark = spark_session
    except Exception:
        pass

    return spark_session


def get_or_create_spark_session():
    return safe_get_global("spark") or create_session()
