# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Can't use DI at this point as this is needed before DI is available

def safe_get_global(var_name: str, default = None):
    try:
        import __main__
        return getattr(__main__, var_name, default)
    except Exception:
        return default

def create_session():
    print("Creating new spark session ...")

    #TODO -- should be configurable and should be done early, and should set as global

    return SparkSession.builder \
        .appName("kindling") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

def get_or_create_spark_session():
    return safe_get_global('spark') or create_session()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
