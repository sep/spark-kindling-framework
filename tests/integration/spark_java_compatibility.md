# Spark/Java Compatibility Issues - Root Cause Analysis

## 🔴 Problem Summary

The integration tests fail with the following error:

```
py4j.protocol.Py4JJavaError: An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext.
: java.lang.ExceptionInInitializerError
    at org.apache.spark.unsafe.array.ByteArrayMethods.<clinit>(ByteArrayMethods.java:52)
    ...
Caused by: java.lang.IllegalStateException: java.lang.NoSuchMethodException:
    java.nio.DirectByteBuffer.<init>(long,int)
```

## 🎯 Root Cause

### Current Environment
- **Java Version**: OpenJDK 21.0.8
- **PySpark Version**: 3.4.3
- **Spark Version**: 3.4.x

### The Issue

**Apache Spark 3.4.x is NOT compatible with Java 21.**

The error occurs because:

1. **Java Internal API Changes**: Starting with Java 9, and significantly in Java 17+, Java restricted access to internal APIs
2. **DirectByteBuffer Constructor**: Spark's `Platform.java` tries to access `java.nio.DirectByteBuffer.<init>(long,int)` using reflection
3. **Strong Encapsulation**: Java 21 has strong encapsulation of JDK internals, blocking this access
4. **Spark's Unsafe API**: Spark heavily uses `sun.misc.Unsafe` and internal Java APIs for performance optimization

### Specific Error Breakdown

```java
// Spark tries to do this in Platform.java:
Constructor<?> directBufferConstructor =
    Class.forName("java.nio.DirectByteBuffer")
         .getDeclaredConstructor(long.class, int.class);

// Java 21 response: NoSuchMethodException
// The constructor exists but is inaccessible due to module encapsulation
```

## 📊 Spark Version Compatibility Matrix

| Spark Version | Java 8 | Java 11 | Java 17 | Java 21 |
|--------------|--------|---------|---------|---------|
| 3.0.x        | ✅ Yes  | ⚠️ Partial | ❌ No   | ❌ No   |
| 3.1.x        | ✅ Yes  | ✅ Yes    | ⚠️ Partial | ❌ No   |
| 3.2.x        | ✅ Yes  | ✅ Yes    | ⚠️ Partial | ❌ No   |
| 3.3.x        | ✅ Yes  | ✅ Yes    | ✅ Yes*   | ❌ No   |
| 3.4.x        | ✅ Yes  | ✅ Yes    | ✅ Yes    | ❌ No   |
| 3.5.x        | ❌ No   | ✅ Yes    | ✅ Yes    | ⚠️ Partial** |
| 4.0.x        | ❌ No   | ✅ Yes    | ✅ Yes    | ✅ Yes    |

\* Requires JVM options
\*\* Experimental support with workarounds

## 🔧 Solutions

### Solution 1: Use Java 11 (Recommended for Spark 3.4.x)

**Why Java 11?**
- Fully supported by Spark 3.4.x
- LTS (Long-Term Support) version
- Stable and widely used in production

**How to switch:**

```bash
# Install Java 11
apt-get update
apt-get install -y openjdk-11-jdk

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Verify
java -version
# Should show: openjdk version "11.x.x"
```

### Solution 2: Use Java 17 with JVM Options

Java 17 works with Spark 3.4.x but requires opening internal modules:

```bash
export SPARK_SUBMIT_OPTS="
  --add-opens=java.base/java.lang=ALL-UNNAMED
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED
  --add-opens=java.base/java.io=ALL-UNNAMED
  --add-opens=java.base/java.net=ALL-UNNAMED
  --add-opens=java.base/java.nio=ALL-UNNAMED
  --add-opens=java.base/java.util=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED
  --add-opens=java.base/sun.security.action=ALL-UNNAMED
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
"
```

### Solution 3: Upgrade to Spark 4.0.x (Future)

Apache Spark 4.0 will have native Java 21 support. However:
- Still in preview/development (as of October 2025)
- Requires code changes for API compatibility
- Not recommended for production yet

### Solution 4: Java 21 Workarounds (Not Recommended)

**Temporary workaround for development only:**

```python
# In pytest fixture
import os
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[1]") \
    .config("spark.driver.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions",
            "--add-opens=java.base/java.nio=ALL-UNNAMED "
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
    .getOrCreate()
```

**Problems with this approach:**
- Still may not work fully
- Defeats Java's security improvements
- Not portable across environments
- Will break in future Java versions

## 🐳 Docker-Based Solution

### Option A: Multi-stage Dockerfile with Java 11

```dockerfile
FROM python:3.11-slim

# Install Java 11
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application
COPY . /app
WORKDIR /app

# Run tests
CMD ["pytest", "tests/integration/", "-v"]
```

### Option B: Use Spark Official Docker Images

```bash
# Pull official Spark image with compatible Java
docker pull apache/spark:3.4.3-java11

# Run tests in container
docker run -v $(pwd):/workspace apache/spark:3.4.3-java11 \
    spark-submit --master local[*] /workspace/tests/integration/test_data_pipes_integration.py
```

## 🔬 Technical Deep Dive

### Why Spark Needs Internal Java APIs

1. **Memory Management**: Direct buffer allocation for zero-copy operations
2. **Unsafe Operations**: Fast memory access bypassing Java safety checks
3. **Serialization**: Custom serializers for performance
4. **Off-heap Memory**: Managing memory outside JVM heap

### What Changed in Java 17+

```java
// Pre-Java 17: This worked
Constructor<?> ctor = DirectByteBuffer.class.getDeclaredConstructor(long.class, int.class);
ctor.setAccessible(true);  // Worked fine

// Java 17+: Strong encapsulation
// Need: --add-opens java.base/java.nio=ALL-UNNAMED
// Or it throws IllegalAccessException
```

### Spark's Fix Timeline

- **Spark 3.0-3.2**: No Java 17 support
- **Spark 3.3**: Added `--add-opens` detection
- **Spark 3.4**: Better Java 17 support
- **Spark 3.5**: Experimental Java 21 with flags
- **Spark 4.0**: Native Java 21 support planned

## ✅ Recommended Actions

### For Development

1. **Switch to Java 11** in dev container:
   ```bash
   apt-get install -y openjdk-11-jdk
   update-alternatives --config java  # Select Java 11
   ```

2. **Update devcontainer.json**:
   ```json
   {
     "image": "mcr.microsoft.com/devcontainers/python:3.11",
     "features": {
       "ghcr.io/devcontainers/features/java:1": {
         "version": "11"
       }
     }
   }
   ```

### For CI/CD

1. **GitHub Actions** example:
   ```yaml
   - name: Set up JDK 11
     uses: actions/setup-java@v3
     with:
       java-version: '11'
       distribution: 'temurin'

   - name: Run Integration Tests
     run: pytest tests/integration/ -v
   ```

2. **Azure DevOps** example:
   ```yaml
   - task: JavaToolInstaller@0
     inputs:
       versionSpec: '11'
       jdkArchitectureOption: 'x64'
       jdkSourceOption: 'PreInstalled'
   ```

### For Production

- **Use Java 11** (most stable for Spark 3.4.x)
- **Databricks**: Handles this automatically
- **Azure Synapse**: Uses compatible Java versions
- **EMR/HDInsight**: Configure via cluster settings

## 📚 References

- [Apache Spark & Java Compatibility](https://spark.apache.org/docs/latest/#downloading)
- [SPARK-37890: Support Java 17](https://issues.apache.org/jira/browse/SPARK-37890)
- [SPARK-43831: Java 21 Support](https://issues.apache.org/jira/browse/SPARK-43831)
- [JEP 403: Strongly Encapsulate JDK Internals](https://openjdk.org/jeps/403)

## 🎓 Key Takeaways

1. **Java 21 + Spark 3.4.x = Incompatible** ❌
2. **Java 11 + Spark 3.4.x = Fully Compatible** ✅
3. **Java 17 + Spark 3.4.x = Works with JVM flags** ⚠️
4. **Solution**: Downgrade Java to 11 or upgrade Spark to 4.0+ (when available)
5. **Production**: Always check Spark's supported Java versions before deployment

## 🔄 Quick Fix for This Dev Container

```bash
# Install Java 11
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk

# Switch to Java 11
sudo update-alternatives --config java
# Select the Java 11 option

# Verify
java -version
# Should show: openjdk version "11.x.x"

# Re-run integration tests
pytest tests/integration/test_data_pipes_integration.py -v
```

---

**Status**: The integration test code is correct. The issue is purely environmental (Java 21 vs Spark 3.4.3 compatibility).
