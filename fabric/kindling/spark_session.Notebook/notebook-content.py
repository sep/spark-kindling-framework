# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

def get_or_create_spark_session():
    """
    Ensures a SparkSession is available as a global 'spark' variable.
    - If running in Synapse, uses the existing 'spark' variable
    - If running standalone, creates a new SparkSession
    """
    import sys
  
    # Check if spark variable exists in globals
    if 'spark' not in globals():
        # Not in Synapse, need to create our own session
        try:
            from pyspark.sql import SparkSession
            
            # Create and configure a new SparkSession
            print("Creating new SparkSession...")
            spark_session = SparkSession.builder \
                .appName("ExportedSynapseCode") \
                .config("spark.driver.memory", "2g") \
                .getOrCreate()
            
            # Add to globals so code can access it as 'spark'
            globals()['spark'] = spark_session
            print("SparkSession created and assigned to global 'spark' variable")
            
        except ImportError:
            print("PySpark not installed. Please install with: pip install pyspark")
            sys.exit(1)
    else:
        pass

    return globals()['spark']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
