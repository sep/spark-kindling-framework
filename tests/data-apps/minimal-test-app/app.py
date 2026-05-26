#!/usr/bin/env python3
"""
Minimal Test - No Kindling Dependencies

This is the simplest possible test to verify Spark job execution works.
Uses NO kindling imports, just pure Python and Spark.
"""

import sys
from datetime import datetime

print(f"====== MINIMAL TEST STARTED: {datetime.now()} ======")
print("Hello from Spark job!")
print(f"Python version: {sys.version}")
print(f"Python path: {sys.path}")

# Try to get Spark session
try:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    print(f"✅ Got Spark session: {spark.version}")

    # Create simple DataFrame
    data = [(1, "test")]
    df = spark.createDataFrame(data, ["id", "value"])
    count = df.count()
    print(f"✅ DataFrame created, count: {count}")

except Exception as e:
    print(f"❌ Spark error: {e}")
    import traceback

    traceback.print_exc()

print("====== MINIMAL TEST COMPLETED ======")
sys.exit(0)
