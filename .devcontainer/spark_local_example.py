#!/usr/bin/env python3
"""
Example: Using Spark in Local Mode
This demonstrates how to use Spark locally within the devcontainer.
"""

from pyspark.sql import SparkSession

# Create a Spark session in local mode
# local[*] uses all available CPU cores
# local[4] would use exactly 4 cores
spark = SparkSession.builder \
    .appName("Local Mode Example") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "/spark-warehouse") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

print(f"Spark Version: {spark.version}")
print(f"Spark Master: {spark.sparkContext.master}")
print(f"App Name: {spark.sparkContext.appName}")

# Create a simple DataFrame
data = [
    ("Alice", 34, "Engineering"),
    ("Bob", 45, "Sales"),
    ("Charlie", 28, "Engineering"),
    ("Diana", 32, "Marketing"),
    ("Eve", 29, "Sales")
]

df = spark.createDataFrame(data, ["name", "age", "department"])

print("\nOriginal DataFrame:")
df.show()

print("\nAverage age by department:")
df.groupBy("department").avg("age").show()

print("\nPeople in Engineering:")
df.filter(df.department == "Engineering").show()

# Access Spark UI at http://localhost:4040 while job is running
print("\n✅ Spark is running in LOCAL mode - all in one container!")
print("📊 View Spark UI at: http://localhost:4040")

spark.stop()
