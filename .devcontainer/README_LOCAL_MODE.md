# Spark Kindling Framework - Local Development Setup

## Architecture

This devcontainer uses **Spark Local Mode** for development:

```
┌────────────────────────────────────────┐
│      Dev Container (Python 3.11)       │
│                                        │
│  ┌──────────────────────────────────┐ │
│  │  Your Python Code (Driver)       │ │
│  │  + PySpark 3.4.3                 │ │
│  │  + Spark Engine (embedded)       │ │
│  │  + Worker Threads                │ │
│  └──────────────────────────────────┘ │
│                                        │
│  All processing happens in-memory     │
│  No network, no cluster needed        │
└────────────────────────────────────────┘
```

## Why Local Mode?

✅ **Matches Azure Platforms**: Synapse and Fabric use Spark 3.4 - you write code the same way
✅ **Simple**: No cluster management, no networking issues
✅ **Fast**: No network overhead, direct memory access
✅ **Easy Debugging**: All logs in one place, breakpoints work
✅ **No Dependencies**: No Bitnami images or external services

## Getting Started

### 1. Open in Dev Container

1. Open this folder in VS Code
2. Click "Reopen in Container" when prompted
3. Wait for container to build

### 2. Run the Example

```bash
cd /workspace/.devcontainer
python spark_local_example.py
```

### 3. View Spark UI

While a Spark job is running, visit: http://localhost:4040

## Using Spark in Your Code

### Basic Pattern

```python
from pyspark.sql import SparkSession

# Create session in local mode
spark = SparkSession.builder \
    .appName("My App") \
    .master("local[*]") \
    .getOrCreate()

# Use Spark normally
df = spark.read.csv("/workspace/data/myfile.csv")
df.show()

spark.stop()
```

### Configuration Options

```python
# Use all CPU cores
.master("local[*]")

# Use specific number of cores
.master("local[4]")

# Set memory
.config("spark.driver.memory", "4g")

# Set warehouse location
.config("spark.sql.warehouse.dir", "/spark-warehouse")
```

## Migrating from Cluster Mode

If you have code that connected to a cluster:

**Before (Cluster):**
```python
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \  # Remote cluster
    .getOrCreate()
```

**After (Local):**
```python
spark = SparkSession.builder \
    .master("local[*]") \  # Local execution
    .getOrCreate()
```

**Everything else stays the same!** DataFrames, SQL, transformations, actions - all identical.

## Framework Development

Your framework code in `/workspace/src/kindling/` should work the same way:

```python
from kindling import SparkSessionProvider

# The framework can detect local vs cluster mode automatically
# Or use environment variables to control behavior
spark = SparkSessionProvider.get_session()
```

## Data Files

Place data files in `/workspace/data/` and they'll be accessible:

```python
df = spark.read.parquet("/workspace/data/my_data.parquet")
```

## Testing

Run tests normally:
```bash
pytest /workspace/test/
```

The same PySpark code works in tests as in production.

## Performance Notes

- **Small/Medium datasets** (< 10GB): Local mode is often FASTER than cluster
- **Large datasets** (> 10GB): You might need more RAM or switch to cluster
- **Development**: Local mode is perfect - fast iteration, easy debugging
- **Production**: Deploy to Azure Synapse or Fabric with same code

## Troubleshooting

### "Java heap space" error
Increase driver memory:
```python
.config("spark.driver.memory", "4g")
```

### "Unable to load native-hadoop library"
This warning is normal and can be ignored in local mode.

### Spark UI not accessible
Make sure port 4040 is forwarded (it should be automatic).

## Next Steps

1. ✅ Container builds successfully
2. ✅ Run the example script
3. ✅ Install your framework: `pip install -e .[dev]`
4. ✅ Start developing!
