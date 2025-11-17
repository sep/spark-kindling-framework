# DevContainer Java Version Update

## Changes Made

Updated `.devcontainer/Dockerfile` to use **Java 11** instead of Java 21 for Spark 3.4.3 compatibility.

### What Changed

```diff
- openjdk-21-jdk-headless
+ openjdk-11-jdk-headless

- # Set Java environment (Java 21 for Spark 3.5)
- ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
+ # Set Java environment (Java 11 for Spark 3.4 compatibility)
+ ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

## Why Java 11?

- **PySpark 3.4.3** (used in this project) is fully compatible with Java 11
- Java 21 causes `NoSuchMethodException` errors due to strong encapsulation of internal APIs
- Java 11 is an LTS (Long-Term Support) version, stable for production use
- Matches deployment environments (Azure Synapse, Microsoft Fabric, Databricks)

## How to Apply Changes

### Option 1: Rebuild DevContainer (Recommended)

1. In VS Code, open Command Palette (Ctrl+Shift+P / Cmd+Shift+P)
2. Select: **"Dev Containers: Rebuild Container"**
3. Wait for rebuild to complete (~2-5 minutes)

### Option 2: Manual Rebuild

```bash
# From host machine (outside container)
cd .devcontainer
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## Verification

After rebuilding, verify Java version:

```bash
java -version
# Should show: openjdk version "11.x.x"
```

Test integration tests:

```bash
pytest tests/integration/test_data_pipes_integration.py -v
# Should now work without Java compatibility errors
```

## Impact

- ✅ Integration tests will now run successfully
- ✅ Spark sessions can be created without errors
- ✅ All Spark DataFrame operations will work
- ✅ Compatible with production environments
- ⚠️ Requires container rebuild to take effect

## Version Compatibility

| Component | Version | Java Support |
|-----------|---------|--------------|
| PySpark | 3.4.3 | Java 8, 11, 17 |
| Delta Spark | 2.4.0 | Java 8, 11, 17 |
| **DevContainer Java** | **11** | ✅ **Optimal** |

## Next Steps

1. Rebuild the devcontainer
2. Run integration tests to verify: `pytest tests/integration/ -v`
3. Confirm Spark UI is accessible at http://localhost:4040 (when Spark session is active)

---

**Date**: October 15, 2025
**Change**: Java 21 → Java 11
**Reason**: Spark 3.4.3 compatibility
