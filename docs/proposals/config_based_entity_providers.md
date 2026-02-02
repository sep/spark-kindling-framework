# Config-Based Entity Provider Architecture

**Status:** Proposal
**Created:** 2026-02-02
**Updated:** 2026-02-02 - Added Option F (Provider-Internal Naming)
**Related:** package_config_architecture.md, dag_execution_implementation_plan.md

## Executive Summary

This proposal introduces **named entity providers** with an **interface composition architecture** that:

1. **Simplifies provider implementation** - providers only implement what they actually support
2. **Uses adapter pattern for fallbacks** - `EntityProviderAdapter` wraps any provider and provides all interfaces
3. **Leverages Delta Lake's power** through direct interface implementation (no fallbacks needed)
4. **Enables graceful degradation** - adapter provides fallback implementations when provider doesn't support a feature
5. **🆕 Respects storage model differences** - each provider uses naming conventions appropriate to its storage type

**Key Insight:** Use **adapter pattern** to separate concerns:
- **Concrete Providers** (Delta, CSV, Parquet): Implement only interfaces they actually support natively
- **EntityProviderAdapter**: Wraps any provider, implements all interfaces with smart fallbacks
- **Framework Services**: Just call methods without checking - adapter handles everything

**🚨 Critical Architectural Decision: Provider-Internal Naming Conventions**

**EntityNameMapper and EntityPathLocator are Delta-specific services** that don't translate to other providers:

| Provider | Naming Model | Path Model |
|----------|--------------|------------|
| **Delta** | ✅ Catalog table names | ✅ ABFSS paths |
| **CSV** | ❌ No catalog | ✅ File paths |
| **Parquet** | ❌ No catalog | ✅ Directory paths |
| **InMemory** | ❌ No persistence | ❌ No paths |

**Solution:** Each provider handles its own naming/path resolution internally:
- **DeltaEntityProvider**: Continues using `EntityNameMapper`/`EntityPathLocator` (injected via DI)
- **CsvEntityProvider**: Uses internal file path conventions (layer-based)
- **ParquetEntityProvider**: Uses internal directory path conventions
- **InMemoryEntityProvider**: Uses simple entity name as key (no paths)

**Benefits:**
- ✅ **No leaky abstractions** - CSV doesn't pretend to have "table names"
- ✅ **Provider-specific logic** - Each provider uses concepts natural to its storage model
- ✅ **Delta keeps current services** - EntityNameMapper/EntityPathLocator work as-is
- ✅ **Clean interfaces** - No shared naming services that don't fit all providers

**Example Architecture:**
```python
# Delta implements all interfaces natively AND uses table-centric naming services
class DeltaEntityProvider(EntityProvider, MergeableProvider, VersionedProvider, ...):
    @inject
    def __init__(
        self,
        entity: DataEntity,
        config: ConfigService,
        entity_name_mapper: EntityNameMapper,  # Delta-specific
        path_locator: EntityPathLocator        # Delta-specific
    ):
        self.name_mapper = entity_name_mapper
        self.path_locator = path_locator

    def _get_table_name(self):
        return self.name_mapper.get_table_name(self.entity)

    def merge(self, df, keys):
        # Native Delta merge - efficient!

# CSV only implements base interface, uses file-centric internal naming
class CsvEntityProvider(EntityProvider):
    def __init__(
        self,
        entity: DataEntity,
        config: ConfigService  # Only needs config, NO naming services
    ):
        pass

    def _get_file_path(self):
        # Internal CSV convention: {base_path}/{layer}/{name}.csv
        layer = self.entity.name.split('.')[0]
        base = self.config.get(f"{layer.upper()}_BASE_PATH")
        return f"{base}/{layer}/{self.entity.name.split('.', 1)[1]}.csv"

# Adapter wraps any provider - does NOT inject naming services
adapter = EntityProviderAdapter(csv_provider)
adapter.merge(df, keys)  # Falls back to read-union-overwrite

# Framework just calls methods
entity_mgr.upsert_entity("customers", df, ["id"])  # Works with any provider!
```

**Benefits:**
- ✅ **No duplication** - fallback logic lives in one place (adapter)
- ✅ **Consistent API** - framework treats all providers the same
- ✅ **Simple providers** - CSV/Parquet stay minimal
- ✅ **Optimal when possible** - Delta uses native features, others fall back
- ✅ **No leaky abstractions** - Each provider uses concepts appropriate to storage model

## Problem Statement

### Current State

The framework currently assumes all data entities use Delta Lake tables:
- **Hardcoded Provider:** `DeltaEntityProvider` is the only implementation
- **Testing Challenges:** Tests require real Delta tables and cloud infrastructure
- **Local Development:** Developers need cloud access just to run code locally
- **External Systems:** No way to read from Snowflake, APIs, or other sources
- **Inflexible:** Can't use CSV fixtures, Parquet staging, or in-memory test data

### Pain Points

1. **Slow, Flaky Tests**
   - Unit tests spin up Delta tables (slow)
   - Tests require Azure/Databricks connectivity (flaky)
   - Test isolation is difficult (shared Delta state)

2. **Developer Experience**
   - Can't run code locally without cloud access
   - Can't test with simple CSV files
   - Requires full production-like environment for development

3. **Integration Limitations**
   - Can't read from external systems (Snowflake, BigQuery, REST APIs)
   - Can't use mixed storage formats (Parquet staging + Delta prod)
   - Can't optimize for different access patterns per layer

4. **Hidden Complexity**
   - Provider abstraction exists but isn't exposed to users
   - Configuration is hardcoded rather than data-driven
   - No way to swap providers per environment

## Proposed Solution

### Named Entity Provider Registry

Introduce a **named provider registry** where providers are registered by string name and selected via configuration tags:

```python
# Built-in providers
EntityProviders.register("delta", DeltaEntityProvider)
EntityProviders.register("csv", CsvEntityProvider)
EntityProviders.register("parquet", ParquetEntityProvider)
EntityProviders.register("in_memory", InMemoryEntityProvider)

# Custom user providers
EntityProviders.register("snowflake", SnowflakeEntityProvider)
EntityProviders.register("rest_api", RestApiEntityProvider)
```

### Configuration via Tags

Use the existing tag system from package configuration architecture to specify providers:

```yaml
my_package:
  data_entities:
    # Default: everything uses Delta in production
    "*":
      tags:
        provider: delta
        base_path: /mnt/prod/data

    # Test entities use CSV files
    test.*:
      tags:
        provider: csv
        base_path: /tmp/test_data

    # External system integration
    external.customer_master:
      tags:
        provider: snowflake
        database: PROD_DB
        schema: CUSTOMER
```

### Environment-Specific Providers

Leverage hierarchical config to change providers per environment:

```yaml
# env_dev.yaml - developers use local CSV files
my_package:
  data_entities:
    "*":
      tags:
        provider: csv
        base_path: ./local_data

# env_test.yaml - unit tests use in-memory
my_package:
  data_entities:
    "*":
      tags:
        provider: in_memory

# env_prod.yaml - production uses Delta Lake
my_package:
  data_entities:
    "*":
      tags:
        provider: delta
        base_path: abfss://prod@storage.dfs.core.windows.net/data
```

## Architecture Design

### 1. Base Provider Interface

```python
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Optional

class EntityProvider(ABC):
    """Base interface - all providers must implement"""

    def __init__(self, entity_entry: DataEntity):
        self.entity = entity_entry
        self.tags = entity_entry.tags

    @abstractmethod
    def read(self) -> DataFrame:
        """Read entity data"""
        pass

    @abstractmethod
    def write(self, df: DataFrame, mode: str = "append"):
        """Write data to entity"""
        pass

    @abstractmethod
    def exists(self) -> bool:
        """Check if entity exists"""
        pass

    @abstractmethod
    def delete(self):
        """Delete entity"""
        pass
```

### 2. Optional Provider Interfaces

```python
class VersionedProvider(ABC):
    """Providers that support versioning (Delta, Iceberg, Hudi)"""

    @abstractmethod
    def get_version(self) -> int:
        """Get current version number"""
        pass

    @abstractmethod
    def get_history(self, limit: int = 20) -> DataFrame:
        """Get version history"""
        pass

    @abstractmethod
    def read_version(self, version: int) -> DataFrame:
        """Read specific version (time travel)"""
        pass

    @abstractmethod
    def read_timestamp(self, timestamp: str) -> DataFrame:
        """Read at specific timestamp (time travel)"""
        pass

class ChangeStreamProvider(ABC):
    """Providers that support change data capture (Delta CDF)"""

    @abstractmethod
    def read_changes(
        self,
        from_version: int,
        to_version: Optional[int] = None
    ) -> DataFrame:
        """Read changes between versions"""
        pass

class MergeableProvider(ABC):
    """Providers that support merge/upsert operations"""

    @abstractmethod
    def merge(
        self,
        df: DataFrame,
        merge_keys: List[str],
        update_all: bool = True,
        insert_all: bool = True
    ):
        """Merge data with upsert logic"""
        pass

class OptimizableProvider(ABC):
    """Providers that support storage optimization"""

    @abstractmethod
    def optimize(self, where: Optional[str] = None, zorder_by: Optional[List[str]] = None):
        """Optimize storage (compaction, Z-ordering)"""
        pass

    @abstractmethod
    def vacuum(self, retention_hours: int = 168):
        """Clean up old files"""
        pass

class SchemaEvolvableProvider(ABC):
    """Providers that support automatic schema evolution"""

    @abstractmethod
    def write_with_schema_evolution(self, df: DataFrame, mode: str = "append"):
        """Write with automatic schema merge"""
        pass

class PartitionedProvider(ABC):
    """Providers that support partition management"""

    @abstractmethod
    def add_partition(self, partition_spec: Dict[str, Any]):
        """Add partition"""
        pass

    @abstractmethod
    def drop_partition(self, partition_spec: Dict[str, Any]):
        """Drop partition"""
        pass

    @abstractmethod
    def list_partitions(self) -> List[Dict[str, Any]]:
        """List all partitions"""
        pass
```

### 3. Framework Services Check Interfaces

```python
# Example: Watermark Manager
def read_incremental_changes(self, provider: EntityProvider, last_version: int) -> DataFrame:
    """Adapt strategy based on provider interfaces"""

    # Strategy 1: Use change stream (optimal)
    if isinstance(provider, ChangeStreamProvider):
        return provider.read_changes(from_version=last_version)

    # Strategy 2: Use versioning (good)
    elif isinstance(provider, VersionedProvider):
        df = provider.read()
        if "version" in df.columns:
            return df.filter(f"version > {last_version}")
        else:
            return df  # Full table scan fallback

    # Strategy 3: Timestamp-based (acceptable)
    else:
        df = provider.read()
        if "updated_at" in df.columns:
            last_timestamp = self._get_timestamp(last_version)
            return df.filter(f"updated_at > '{last_timestamp}'")
        else:
            raise ValueError("No incremental read strategy available")

# Example: Entity Manager
def optimize_entity(self, entity_name: str):
    provider = self._get_provider(entity_name)

    if isinstance(provider, OptimizableProvider):
        provider.optimize()
    else:
        self.logger.debug(f"Optimize not supported for {entity_name}")
```
```

### 2. Provider Registry

```python
class EntityProviderRegistry:
    """Global registry for entity providers"""

    _providers: Dict[str, Type[EntityProvider]] = {}

    @classmethod
    def register(cls, name: str, provider_class: Type[EntityProvider]):
        """
        Register a provider by name.

        Args:
            name: Provider name (e.g., 'delta', 'csv', 'snowflake')
            provider_class: Provider class inheriting from EntityProvider
        """
        cls._providers[name] = provider_class

    @classmethod
    def get_provider(cls, name: str) -> Type[EntityProvider]:
        """
        Get provider class by name.

        Args:
            name: Provider name

        Returns:
            Provider class

        Raises:
            ValueError: If provider not found
        """
        if name not in cls._providers:
            raise ValueError(f"Unknown entity provider: {name}")
        return cls._providers[name]

    @classmethod
    def list_providers(cls) -> List[str]:
        """Get list of registered provider names."""
        return list(cls._providers.keys())

# Convenience alias
EntityProviders = EntityProviderRegistry
```

### 3. Entity Manager Integration

```python
from kindling.injection import inject
from kindling.platform_provider import PlatformServiceProvider
from kindling.spark_log_provider import SparkLoggerProvider

@inject
class DataEntityManager:
    """Manages data entity lifecycle with pluggable providers"""

    def __init__(
        self,
        platform: PlatformServiceProvider,
        logger_provider: SparkLoggerProvider
    ):
        self.platform = platform.get_service()
        self.logger = logger_provider.get_logger("entity_manager")

    def read_entity(self, entity_name: str) -> DataFrame:
        """
        Read entity using configured provider.

        Args:
            entity_name: Name of entity to read

        Returns:
            DataFrame with entity data
        """
        entity_entry = DataEntities.get_registered_entity(entity_name)

        # Get provider from tags (default: delta)
        provider_name = entity_entry.tags.get("provider", "delta")
        self.logger.info(f"Reading entity {entity_name} using provider: {provider_name}")

        # Instantiate provider with entity config
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)

        # Read and return data
        return provider.read()

    def write_entity(
        self,
        entity_name: str,
        df: DataFrame,
        mode: str = "append",
        validate_schema: bool = True
    ):
        """
        Write DataFrame to entity using configured provider.

        Args:
            entity_name: Name of entity to write
            df: DataFrame to write
            mode: Write mode ('append', 'overwrite', 'merge')
            validate_schema: Whether to validate schema before writing
        """
        entity_entry = DataEntities.get_registered_entity(entity_name)

        # Get provider from tags (default: delta)
        provider_name = entity_entry.tags.get("provider", "delta")
        self.logger.info(f"Writing entity {entity_name} using provider: {provider_name}")

        # Instantiate provider
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)

        # Validate schema if requested
        if validate_schema and not provider.validate_schema(df):
            raise ValueError(f"Schema validation failed for entity {entity_name}")

        # Write data
        provider.write(df, mode)

    def entity_exists(self, entity_name: str) -> bool:
        """Check if entity exists."""
        entity_entry = DataEntities.get_registered_entity(entity_name)
        provider_name = entity_entry.tags.get("provider", "delta")
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)
        return provider.exists()

    def delete_entity(self, entity_name: str):
        """Delete entity from storage."""
        entity_entry = DataEntities.get_registered_entity(entity_name)
        provider_name = entity_entry.tags.get("provider", "delta")
        self.logger.warning(f"Deleting entity {entity_name}")
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)
        provider.delete()
```

### 4. Built-In Provider Implementations

#### Delta Lake Provider (Implements All Interfaces)

```python
from delta.tables import DeltaTable

class DeltaEntityProvider(
    EntityProvider,
    VersionedProvider,
    ChangeStreamProvider,
    MergeableProvider,
    OptimizableProvider,
    SchemaEvolvableProvider,
    PartitionedProvider
):
    """Delta provider implements all optional interfaces"""

    def _get_path(self) -> str:
        base_path = self.tags.get("base_path", "/mnt/data")
        return f"{base_path}/{self.entity.name.replace('.', '/')}"

    # === EntityProvider (base interface) ===

    def read(self) -> DataFrame:
        return spark.read.format("delta").load(self._get_path())

    def write(self, df: DataFrame, mode: str = "append"):
        df.write.format("delta").mode(mode).save(self._get_path())

    def exists(self) -> bool:
        try:
            DeltaTable.forPath(spark, self._get_path())
            return True
        except:
            return False

    def delete(self):
        # Use platform service for deletion
        pass

    # === VersionedProvider ===

    def get_version(self) -> int:
        delta_table = DeltaTable.forPath(spark, self._get_path())
        return delta_table.history(1).select("version").first()[0]

    def get_history(self, limit: int = 20) -> DataFrame:
        return DeltaTable.forPath(spark, self._get_path()).history(limit)

    def read_version(self, version: int) -> DataFrame:
        return spark.read.format("delta") \
            .option("versionAsOf", version).load(self._get_path())

    def read_timestamp(self, timestamp: str) -> DataFrame:
        return spark.read.format("delta") \
            .option("timestampAsOf", timestamp).load(self._get_path())

    # === ChangeStreamProvider ===

    def read_changes(self, from_version: int, to_version: Optional[int] = None) -> DataFrame:
        reader = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", from_version + 1)
        if to_version:
            reader = reader.option("endingVersion", to_version)
        return reader.load(self._get_path())

    # === MergeableProvider ===

    def merge(self, df: DataFrame, merge_keys: List[str], **kwargs):
        if not self.exists():
            self.write(df, "overwrite")
            return

        delta_table = DeltaTable.forPath(spark, self._get_path())
        condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        delta_table.alias("target") \
            .merge(df.alias("source"), condition) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()

    # === OptimizableProvider ===

    def optimize(self, where: Optional[str] = None, zorder_by: Optional[List[str]] = None):
        delta_table = DeltaTable.forPath(spark, self._get_path())
        builder = delta_table.optimize()
        if where:
            builder = builder.where(where)
        if zorder_by:
            builder.executeZOrderBy(*zorder_by)
        else:
            builder.executeCompaction()

    def vacuum(self, retention_hours: int = 168):
        DeltaTable.forPath(spark, self._get_path()).vacuum(retention_hours)

    # === SchemaEvolvableProvider ===

    def write_with_schema_evolution(self, df: DataFrame, mode: str = "append"):
        df.write.format("delta") \
            .option("mergeSchema", "true") \
            .mode(mode) \
            .save(self._get_path())

    # === PartitionedProvider ===

    def add_partition(self, partition_spec: Dict[str, Any]):
        pass  # Delta handles automatically

    def drop_partition(self, partition_spec: Dict[str, Any]):
        condition = " AND ".join([f"{k} = '{v}'" for k, v in partition_spec.items()])
        DeltaTable.forPath(spark, self._get_path()).delete(condition)

    def list_partitions(self) -> List[Dict[str, Any]]:
        partition_cols = self.tags.get("partition_by", [])
        if not partition_cols:
            return []
        return self.read().select(*partition_cols).distinct().collect()
```

**Key Point:** Delta implements 7 interfaces - base + 6 optional. Framework services use `isinstance()` to check what's available.

#### CSV Provider (Base Interface Only)

```python
class CsvEntityProvider(EntityProvider):
    """CSV only implements base EntityProvider interface"""

    def _get_path(self) -> str:
        """Get full path to Delta table."""
        base_path = self.tags.get("base_path", "/mnt/data")
        return f"{base_path}/{self.entity.name.replace('.', '/')}"

    def read(self, version: Optional[int] = None, timestamp: Optional[str] = None) -> DataFrame:
        """
        Read Delta table with optional time travel.

        Args:
            version: Specific version to read (time travel)
            timestamp: Timestamp to read (time travel)
        """
        path = self._get_path()
        reader = spark.read.format("delta")

        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)

        return reader.load(path)

    def write(self, df: DataFrame, mode: str = "append"):
        path = self._get_path()

        if mode == "merge":
            # Use Delta merge operation
            merge_keys = self.tags.get("merge_keys", [])
            if not merge_keys:
                raise ValueError(f"merge_keys required for merge mode on {self.entity.name}")

            if self.exists():
                delta_table = DeltaTable.forPath(spark, path)
                merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])

                delta_table.alias("target") \
                    .merge(df.alias("source"), merge_condition) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            else:
                df.write.format("delta").mode("overwrite").save(path)
        else:
            # Enable schema evolution if configured
            writer = df.write.format("delta").mode(mode)
            if self.tags.get("schema_evolution", False):
                writer = writer.option("mergeSchema", "true")
            writer.save(path)

    def exists(self) -> bool:
        try:
            DeltaTable.forPath(spark, self._get_path())
            return True
        except:
            return False

    def delete(self):
        path = self._get_path()
        from kindling.platform_provider import SparkPlatformServiceProvider
        platform = SparkPlatformServiceProvider().get_service()
        platform.delete_path(path, recursive=True)

    # === Delta-Specific Extended Methods ===

    def get_version(self) -> int:
        """Get current version number of Delta table."""
        if not self.supports_capability(ProviderCapability.VERSIONING):
            raise NotImplementedError("Provider does not support versioning")

        delta_table = DeltaTable.forPath(spark, self._get_path())
        history = delta_table.history(1)
        return history.select("version").first()[0]

    def get_history(self, limit: int = 20) -> DataFrame:
        """Get version history of Delta table."""
        if not self.supports_capability(ProviderCapability.VERSIONING):
            raise NotImplementedError("Provider does not support versioning")

        delta_table = DeltaTable.forPath(spark, self._get_path())
        return delta_table.history(limit)

    def read_changes_since_version(self, from_version: int, to_version: Optional[int] = None) -> DataFrame:
        """
        Read changes between versions (requires Change Data Feed).

        Args:
            from_version: Starting version (exclusive)
            to_version: Ending version (inclusive), or latest if None
        """
        if not self.supports_capability(ProviderCapability.CHANGE_DATA_FEED):
            raise NotImplementedError("Provider does not support change data feed")

        path = self._get_path()
        reader = spark.read.format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", from_version + 1)

        if to_version is not None:
            reader = reader.option("endingVersion", to_version)

        return reader.load(path)

    def optimize(self, where: Optional[str] = None):
        """
        Optimize Delta table (compaction, Z-ordering).

        Args:
            where: Optional partition filter
        """
        if not self.supports_capability(ProviderCapability.OPTIMIZE):
            raise NotImplementedError("Provider does not support optimize")

        delta_table = DeltaTable.forPath(spark, self._get_path())
        optimize_builder = delta_table.optimize()

        if where:
            optimize_builder = optimize_builder.where(where)

        # Z-order by columns if configured
        zorder_cols = self.tags.get("zorder_by", [])
        if zorder_cols:
            optimize_builder.executeZOrderBy(*zorder_cols)
        else:
            optimize_builder.executeCompaction()

    def vacuum(self, retention_hours: int = 168):
        """
        Vacuum old files from Delta table.

        Args:
            retention_hours: Hours to retain (default 7 days)
        """
        if not self.supports_capability(ProviderCapability.VACUUM):
            raise NotImplementedError("Provider does not support vacuum")

        delta_table = DeltaTable.forPath(spark, self._get_path())
        delta_table.vacuum(retention_hours)

    def restore_to_version(self, version: int):
        """Restore table to a specific version."""
        if not self.supports_capability(ProviderCapability.TIME_TRAVEL):
            raise NotImplementedError("Provider does not support time travel")

        delta_table = DeltaTable.forPath(spark, self._get_path())
        delta_table.restoreToVersion(version)
```

#### CSV Provider

```python
class CsvEntityProvider(EntityProvider):
    """Provider for CSV files"""

    def _get_path(self) -> str:
        base_path = self.tags.get("base_path", "/tmp/data")
        return f"{base_path}/{self.entity.name.replace('.', '/')}.csv"

    def read(self) -> DataFrame:
        path = self._get_path()
        reader = spark.read.format("csv") \
            .option("header", "true")

        # Use schema if available
        if self.entity.schema:
            reader = reader.schema(self.entity.schema)
        else:
            reader = reader.option("inferSchema", "true")

        return reader.load(path)

    def write(self, df: DataFrame, mode: str = "overwrite"):
        # CSV typically uses overwrite mode
        path = self._get_path()
        df.coalesce(1).write.format("csv") \
            .option("header", "true") \
            .mode(mode) \
            .save(path)

    def exists(self) -> bool:
        from pathlib import Path
        return Path(self._get_path()).exists()

    def delete(self):
        import shutil
        from pathlib import Path
        path = Path(self._get_path())
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
```

#### Parquet Provider

```python
class ParquetEntityProvider(EntityProvider):
    """Provider for Parquet files"""

    @classmethod
    def get_capabilities(cls) -> List[ProviderCapability]:
        """Parquet supports schema evolution and partitioning"""
        return [
            ProviderCapability.SCHEMA_EVOLUTION,
            ProviderCapability.PARTITIONING,
        ]

    def _get_path(self) -> str:
        base_path = self.tags.get("base_path", "/mnt/data")
        return f"{base_path}/{self.entity.name.replace('.', '/')}"

    def read(self) -> DataFrame:
        return spark.read.format("parquet").load(self._get_path())

    def write(self, df: DataFrame, mode: str = "append"):
        path = self._get_path()

        # Apply partitioning if specified
        partition_cols = self.tags.get("partition_by", [])
        writer = df.write.format("parquet").mode(mode)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        # Enable schema merge for append
        if mode == "append" and self.tags.get("schema_evolution", False):
            writer = writer.option("mergeSchema", "true")

        writer.save(path)

    def exists(self) -> bool:
        try:
            spark.read.format("parquet").load(self._get_path())
            return True
        except:
            return False

    def delete(self):
        from kindling.platform_provider import SparkPlatformServiceProvider
        platform = SparkPlatformServiceProvider().get_service()
        platform.delete_path(self._get_path(), recursive=True)
```

#### In-Memory Provider

```python
class InMemoryEntityProvider(EntityProvider):
    """Provider for in-memory DataFrames (testing)"""

    # Class-level storage shared across all instances
    _storage: Dict[str, DataFrame] = {}

    def read(self) -> DataFrame:
        if self.entity.name not in self._storage:
            # Return empty DataFrame with schema if defined
            if self.entity.schema:
                return spark.createDataFrame([], self.entity.schema)
            else:
                raise ValueError(f"Entity {self.entity.name} not found in memory and no schema defined")

        return self._storage[self.entity.name]

    def write(self, df: DataFrame, mode: str = "append"):
        if mode == "overwrite":
            self._storage[self.entity.name] = df
        elif mode == "append":
            if self.entity.name in self._storage:
                # Union with existing data
                self._storage[self.entity.name] = self._storage[self.entity.name].union(df)
            else:
                self._storage[self.entity.name] = df
        else:
            raise ValueError(f"Mode {mode} not supported for in-memory provider")

    def exists(self) -> bool:
        return self.entity.name in self._storage

    def delete(self):
        if self.entity.name in self._storage:
            del self._storage[self.entity.name]

    @classmethod
    def clear_all(cls):
        """Clear all in-memory storage (useful for test teardown)."""
        cls._storage.clear()
```

### 5. Entity Declaration (No Changes)

Entity declarations remain unchanged - provider is set via configuration, not code:

```python
from kindling.data_entities import data_entity
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Uses config-specified provider
@data_entity(name="bronze.customers")
class Customers:
    schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("name", StringType()),
    ])

# Can override provider in decorator for specific entity
@data_entity(
    name="external.legacy_orders",
    tags={
        "provider": "parquet",  # Force Parquet for this entity
        "base_path": "/mnt/legacy"
    }
)
class LegacyOrders:
    schema = StructType([...])
```

## Interface-Based Framework Integration

### Philosophy: Interface Composition

The framework uses **Python's standard interface pattern** with `isinstance()` checks instead of custom capability systems:

- ✅ **Simple and Pythonic** - use language features, not custom machinery
- ✅ **Type-safe** - IDEs understand interfaces and provide autocomplete
- ✅ **Explicit** - provider class declaration shows all capabilities at a glance
- ✅ **Flexible** - providers can implement any subset of interfaces

### Interface-Aware Services

Framework services check interfaces and adapt behavior accordingly.

#### WatermarkManager with Interface Checks

```python
@inject
class WatermarkManager:
    """Watermark service that adapts to provider interfaces"""

    def read_incremental_changes(
        self,
        source_entity_name: str,
        reader_id: str
    ) -> DataFrame:
        """
        Read incremental changes since last watermark.
        Adapts strategy based on provider interfaces.
        """
        # Get the provider for this entity
        entity_entry = DataEntities.get_registered_entity(source_entity_name)
        provider_name = entity_entry.tags.get("provider", "delta")
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)

        # Get last watermark
        last_version = self._get_last_watermark(source_entity_name, reader_id)

        # Strategy 1: Use change stream (optimal - Delta CDF)
        if isinstance(provider, ChangeStreamProvider):
            self.logger.info(f"Using change stream for {source_entity_name}")
            return provider.read_changes(from_version=last_version)

        # Strategy 2: Use versioning (good - version-based filtering)
        elif isinstance(provider, VersionedProvider):
            self.logger.info(f"Using version-based incremental for {source_entity_name}")
            df = provider.read()

            if "version" in df.columns:
                return df.filter(f"version > {last_version}")
            elif "updated_at" in df.columns:
                last_timestamp = self._get_last_timestamp(source_entity_name, reader_id)
                return df.filter(f"updated_at > '{last_timestamp}'")
            else:
                self.logger.warning(f"No version/timestamp column, returning full table")
                return df

        # Strategy 3: Timestamp-based (acceptable - for basic providers)
        else:
            self.logger.warning(f"Using timestamp-based incremental for {source_entity_name}")
            df = provider.read()

            if "updated_at" in df.columns:
                last_timestamp = self._get_last_timestamp(source_entity_name, reader_id)
                return df.filter(f"updated_at > '{last_timestamp}'")
            else:
                raise ValueError(
                    f"Incremental read requires ChangeStreamProvider or VersionedProvider interface, "
                    f"or 'updated_at' column. Entity {source_entity_name} has none."
                )

    def save_watermark(
        self,
        source_entity_name: str,
        reader_id: str,
        execution_id: str
    ):
        """Save watermark after processing."""
        entity_entry = DataEntities.get_registered_entity(source_entity_name)
        provider_name = entity_entry.tags.get("provider", "delta")
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)

        # Get version if provider supports it
        if isinstance(provider, VersionedProvider):
            version = provider.get_version()
        else:
            version = None  # Use timestamp-based watermark

        self._save_watermark_record(
            source_entity_name,
            reader_id,
            version,
            execution_id
        )
```

#### Entity Manager with Interface Checks

```python
@inject
class DataEntityManager:
    """Entity manager with interface-aware operations"""

    def read_entity_version(
        self,
        entity_name: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None
    ) -> DataFrame:
        """
        Read specific version of entity (time travel).
        Only works if provider implements VersionedProvider.
        """
        entity_entry = DataEntities.get_registered_entity(entity_name)
        provider_name = entity_entry.tags.get("provider", "delta")
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)

        # Check if versioning supported
        if not isinstance(provider, VersionedProvider):
            raise TypeError(
                f"Time travel not supported for entity {entity_name}. "
                f"Provider {provider_name} does not implement VersionedProvider interface."
            )

        # Call appropriate time travel method
        if version is not None:
            return provider.read_version(version)
        elif timestamp is not None:
            return provider.read_timestamp(timestamp)
        else:
            return provider.read()

    def merge_entity(
        self,
        entity_name: str,
        df: DataFrame,
        merge_keys: List[str]
    ):
        """
        Merge data into entity.
        Only works if provider implements MergeableProvider.
        """
        entity_entry = DataEntities.get_registered_entity(entity_name)
        provider_name = entity_entry.tags.get("provider", "delta")
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)

        if not isinstance(provider, MergeableProvider):
            raise TypeError(
                f"Merge not supported for entity {entity_name}. "
                f"Provider {provider_name} does not implement MergeableProvider interface."
            )

        provider.merge(df, merge_keys)

    def optimize_entity(self, entity_name: str, **kwargs):
        """
        Optimize entity storage.
        No-op if provider doesn't implement OptimizableProvider.
        """
        entity_entry = DataEntities.get_registered_entity(entity_name)
        provider_name = entity_entry.tags.get("provider", "delta")
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)

        if isinstance(provider, OptimizableProvider):
            self.logger.info(f"Optimizing {entity_name}")
            provider.optimize(**kwargs)
        else:
            self.logger.debug(
                f"Optimize skipped for {entity_name} - "
                f"provider does not implement OptimizableProvider"
            )

    def vacuum_entity(self, entity_name: str, retention_hours: int = 168):
        """
        Vacuum old files from entity.
        No-op if provider doesn't implement OptimizableProvider.
        """
        entity_entry = DataEntities.get_registered_entity(entity_name)
        provider_name = entity_entry.tags.get("provider", "delta")
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)

        if isinstance(provider, OptimizableProvider):
            self.logger.info(f"Vacuuming {entity_name}")
            provider.vacuum(retention_hours)
        else:
            self.logger.debug(
                f"Vacuum skipped for {entity_name} - "
                f"provider does not implement OptimizableProvider"
            )
```

### Provider Adapter Pattern

Instead of framework services implementing fallback logic, use an **adapter** that wraps providers and provides smart interfaces:

#### EntityProviderAdapter (Wraps Any Provider)

```python
class EntityProviderAdapter(
    EntityProvider,
    VersionedProvider,
    ChangeStreamProvider,
    MergeableProvider,
    OptimizableProvider,
    SchemaEvolvableProvider,
    PartitionedProvider
):
    """
    Adapter that wraps any provider and implements all interfaces.

    If underlying provider supports an interface, delegates to it.
    If not, provides fallback implementation.

    This means framework services can treat ALL providers as if they
    support all features - adapter handles the adaptation.

    NOTE: Adapter does NOT inject naming/path services into providers.
    Each provider handles its own naming/path resolution internally:
    - Delta uses EntityNameMapper/EntityPathLocator (injected into Delta provider)
    - CSV uses file path conventions (internal to CSV provider)
    - Parquet uses directory path conventions (internal to Parquet provider)
    - InMemory uses simple keys (internal to InMemory provider)

    The adapter ONLY focuses on providing interface fallbacks.
    """

    def __init__(self, provider: EntityProvider):
        self.provider = provider
        self.entity = provider.entity
        self.tags = provider.tags

    # === Base EntityProvider - always delegate ===

    def read(self) -> DataFrame:
        return self.provider.read()

    def write(self, df: DataFrame, mode: str = "append"):
        return self.provider.write(df, mode)

    def exists(self) -> bool:
        return self.provider.exists()

    def delete(self):
        return self.provider.delete()

    # === MergeableProvider - delegate or fallback ===

    def merge(
        self,
        df: DataFrame,
        merge_keys: List[str],
        update_all: bool = True,
        insert_all: bool = True
    ):
        """Merge with automatic fallback if not natively supported"""
        # Delegate to native implementation if available
        if isinstance(self.provider, MergeableProvider):
            return self.provider.merge(df, merge_keys, update_all, insert_all)

        # Fallback: read-union-dedupe-overwrite
        logger.warning(f"Provider does not support native merge, using fallback strategy")

        if not self.provider.exists():
            self.provider.write(df, mode="overwrite")
            return

        # Read existing data
        existing = self.provider.read()

        # Union and deduplicate
        from pyspark.sql import Window
        import pyspark.sql.functions as F

        combined = existing.union(df)

        # Keep latest record per merge key (assumes updated_at column)
        if "updated_at" in combined.columns:
            window = Window.partitionBy(*merge_keys).orderBy(F.desc("updated_at"))
            deduped = combined.withColumn("rn", F.row_number().over(window)) \
                .filter("rn = 1") \
                .drop("rn")
        else:
            # Simple dedupe - keep any record
            deduped = combined.dropDuplicates(merge_keys)

        # Overwrite with merged result
        self.provider.write(deduped, mode="overwrite")

    # === VersionedProvider - delegate or return None ===

    def get_version(self) -> Optional[int]:
        """Get version if provider supports it, None otherwise"""
        if isinstance(self.provider, VersionedProvider):
            return self.provider.get_version()
        return None

    def get_history(self, limit: int = 20) -> Optional[DataFrame]:
        """Get history if provider supports it, None otherwise"""
        if isinstance(self.provider, VersionedProvider):
            return self.provider.get_history(limit)
        return None

    def read_version(self, version: int) -> DataFrame:
        """Read version if supported, else raise clear error"""
        if isinstance(self.provider, VersionedProvider):
            return self.provider.read_version(version)
        raise NotImplementedError(
            f"Time travel not supported by {self.provider.__class__.__name__}"
        )

    def read_timestamp(self, timestamp: str) -> DataFrame:
        """Read timestamp if supported, else raise clear error"""
        if isinstance(self.provider, VersionedProvider):
            return self.provider.read_timestamp(timestamp)
        raise NotImplementedError(
            f"Time travel not supported by {self.provider.__class__.__name__}"
        )

    # === ChangeStreamProvider - delegate or fallback to versioning ===

    def read_changes(
        self,
        from_version: int,
        to_version: Optional[int] = None
    ) -> DataFrame:
        """Read changes with fallback strategies"""
        # Strategy 1: Native change stream (best - Delta CDF)
        if isinstance(self.provider, ChangeStreamProvider):
            return self.provider.read_changes(from_version, to_version)

        # Strategy 2: Version-based filtering (acceptable)
        elif isinstance(self.provider, VersionedProvider):
            logger.warning("Using version-based filtering fallback for change stream")
            df = self.provider.read()
            if "version" in df.columns:
                return df.filter(f"version > {from_version}")
            else:
                return df  # Full table scan

        # Strategy 3: Timestamp-based filtering (minimal)
        else:
            logger.warning("Using timestamp-based fallback for change stream")
            df = self.provider.read()
            if "updated_at" in df.columns:
                # Would need to map version to timestamp
                return df  # Simplified - would need more logic
            else:
                raise NotImplementedError(
                    "Change stream requires ChangeStreamProvider, VersionedProvider, "
                    "or 'updated_at' column"
                )

    # === OptimizableProvider - delegate or no-op ===

    def optimize(self, where: Optional[str] = None, zorder_by: Optional[List[str]] = None):
        """Optimize if supported, otherwise no-op with debug log"""
        if isinstance(self.provider, OptimizableProvider):
            return self.provider.optimize(where, zorder_by)
        else:
            logger.debug(f"Optimize not supported by {self.provider.__class__.__name__}, skipping")

    def vacuum(self, retention_hours: int = 168):
        """Vacuum if supported, otherwise no-op with debug log"""
        if isinstance(self.provider, OptimizableProvider):
            return self.provider.vacuum(retention_hours)
        else:
            logger.debug(f"Vacuum not supported by {self.provider.__class__.__name__}, skipping")

    # === SchemaEvolvableProvider - delegate or use regular write ===

    def write_with_schema_evolution(self, df: DataFrame, mode: str = "append"):
        """Write with schema evolution if supported, else regular write"""
        if isinstance(self.provider, SchemaEvolvableProvider):
            return self.provider.write_with_schema_evolution(df, mode)
        else:
            logger.warning("Schema evolution not supported, using regular write")
            return self.provider.write(df, mode)

    # === PartitionedProvider - delegate or no-op ===

    def add_partition(self, partition_spec: Dict[str, Any]):
        if isinstance(self.provider, PartitionedProvider):
            return self.provider.add_partition(partition_spec)
        else:
            logger.debug("Partition management not supported, skipping")

    def drop_partition(self, partition_spec: Dict[str, Any]):
        if isinstance(self.provider, PartitionedProvider):
            return self.provider.drop_partition(partition_spec)
        else:
            logger.debug("Partition management not supported, skipping")

    def list_partitions(self) -> List[Dict[str, Any]]:
        if isinstance(self.provider, PartitionedProvider):
            return self.provider.list_partitions()
        else:
            return []
```

#### Simplified Framework Services

Now framework services don't need to check interfaces - just call methods:

```python
@inject
class DataEntityManager:
    """Entity manager - no interface checking needed!"""

    def read_entity(self, entity_name: str) -> DataFrame:
        provider = self._get_adapted_provider(entity_name)
        return provider.read()

    def write_entity(
        self,
        entity_name: str,
        df: DataFrame,
        mode: str = "append"
    ):
        provider = self._get_adapted_provider(entity_name)
        provider.write(df, mode)

    def upsert_entity(
        self,
        entity_name: str,
        df: DataFrame,
        merge_keys: List[str]
    ):
        """Just call merge - adapter handles fallback!"""
        provider = self._get_adapted_provider(entity_name)
        provider.merge(df, merge_keys)  # Adapter does the right thing

    def read_entity_version(
        self,
        entity_name: str,
        version: int
    ) -> DataFrame:
        """Just call read_version - adapter raises error if not supported"""
        provider = self._get_adapted_provider(entity_name)
        return provider.read_version(version)

    def optimize_entity(self, entity_name: str, **kwargs):
        """Just call optimize - adapter no-ops if not supported"""
        provider = self._get_adapted_provider(entity_name)
        provider.optimize(**kwargs)

    def _get_adapted_provider(self, entity_name: str) -> EntityProviderAdapter:
        """Get provider wrapped in adapter"""
        entity_entry = DataEntities.get_registered_entity(entity_name)
        provider_name = entity_entry.tags.get("provider", "delta")
        provider_class = EntityProviders.get_provider(provider_name)
        raw_provider = provider_class(entity_entry)

        # Wrap in adapter
        return EntityProviderAdapter(raw_provider)
```

#### Benefits of Adapter Pattern

**1. Single Responsibility:**
- **Concrete providers** (Delta, CSV, Parquet): Implement only what they actually support
- **Adapter**: Handles all fallback logic
- **Framework services**: Just call methods without checking

**2. Consistent Interface:**
```python
# All providers look the same to framework
provider.merge(df, merge_keys)  # Works for Delta (native) and CSV (fallback)
provider.optimize()  # Works for Delta (native) and CSV (no-op)
provider.read_changes(10)  # Works for Delta (CDF) and Parquet (fallback)
```

**3. Easy to Test:**
```python
# Test the adapter with a mock provider
mock_provider = Mock(spec=EntityProvider)
adapter = EntityProviderAdapter(mock_provider)

# Adapter provides all interfaces even though mock doesn't
adapter.merge(df, ["id"])  # Falls back to read-union-overwrite
adapter.optimize()  # No-ops gracefully
```

**4. No Provider Duplication:**
```python
# Without adapter, every provider would need:
class CsvEntityProvider(EntityProvider, MergeableProvider):
    def merge(self, df, merge_keys):
        # Copy-paste fallback logic

class ParquetEntityProvider(EntityProvider, MergeableProvider):
    def merge(self, df, merge_keys):
        # Same copy-paste fallback logic

# With adapter, providers stay focused:
class CsvEntityProvider(EntityProvider):
    # Only implements read/write/exists/delete

class ParquetEntityProvider(EntityProvider):
    # Only implements read/write/exists/delete

# Adapter provides merge for both
```

### Client Usage Patterns

#### Pattern 1: Framework Abstracts Everything (Preferred)

Most users never see providers or adapters:

```python
@data_pipe(name="silver.process_customers", dependencies=["bronze.customers"])
def process_customers():
    # Framework automatically:
    # - Reads from provider (any type)
    # - Writes to provider (any type)
    # - Uses merge if available, overwrite if not
    # - Uses incremental read if supported, full scan if not

    entity_mgr = DataEntityManager()

    # Read (works with any provider)
    customers = entity_mgr.read_entity("bronze.customers")

    # Transform
    processed = customers.withColumn("name_upper", F.upper("name"))

    # Write with automatic merge fallback
    entity_mgr.write_entity(
        "silver.processed_customers",
        processed,
        mode="merge",  # Framework adapts if provider doesn't support
        merge_keys=["customer_id"]
    )
```

**Framework's `write_entity()` with auto-fallback:**
```python
def write_entity(
    self,
    entity_name: str,
    df: DataFrame,
    mode: str = "append",
    merge_keys: Optional[List[str]] = None
):
    """Write with automatic fallback for unsupported modes"""
    provider = self._get_provider(entity_name)

    if mode == "merge":
        if isinstance(provider, MergeableProvider):
            # Use native merge (efficient)
            provider.merge(df, merge_keys)
        else:
            # Fall back to overwrite (less efficient but works)
            self.logger.warning(
                f"Merge not supported for {entity_name}, falling back to overwrite"
            )
            provider.write(df, mode="overwrite")
    else:
        provider.write(df, mode)
```

#### Pattern 2: Explicit Interface Checking (Advanced)

Advanced users can check interfaces for optimal behavior:

```python
@data_pipe(name="silver.upsert_orders")
def upsert_orders():
    entity_mgr = DataEntityManager()
    provider = entity_mgr._get_provider("silver.orders")

    orders = load_new_orders()

    # Explicit interface check for optimal strategy
    if isinstance(provider, MergeableProvider):
        # Use efficient native merge
        provider.merge(orders, merge_keys=["order_id"])
    else:
        # Read, union, dedupe, overwrite (fallback)
        existing = provider.read()
        combined = existing.union(orders)
        deduped = combined.dropDuplicates(["order_id"])
        provider.write(deduped, mode="overwrite")
```

#### Pattern 3: Configuration-Driven Behavior

Users specify behavior in config, framework adapts:

```yaml
my_package:
  data_entities:
    silver.processed_customers:
      tags:
        provider: delta
        write_mode: merge  # Framework uses merge if available
        merge_keys: [customer_id]
        fallback_mode: overwrite  # Fall back if merge not supported
```

### Why CSV Can't Support Merge

**Technical Reason:** CSV files are **immutable flat files** with no row-level operations:

```python
# What "merge" would require for CSV:
1. Read entire existing CSV into memory (expensive)
2. Read new data into memory
3. Union DataFrames
4. Group by merge keys and keep latest (complex logic)
5. Overwrite entire CSV file

# This is:
❌ Not atomic (file corruption risk)
❌ Not efficient (reads entire file every time)
❌ Not what users expect from "merge"
❌ Loses ordering/structure
```

**What Delta merge does (why it's powerful):**
```python
# Delta merge operation:
1. Reads only necessary files (partition pruning)
2. Uses transaction log for coordination
3. Atomic row-level upsert
4. Writes only new/changed data
5. Maintains ACID guarantees

# This is:
✅ Atomic (transactional)
✅ Efficient (incremental writes)
✅ Expected behavior (true upsert)
✅ Safe for concurrent operations
```

**CSV Provider Alternative:**
```python
class CsvEntityProvider(EntityProvider):
    """CSV can't support true merge, but can support overwrite"""

    def write(self, df: DataFrame, mode: str = "append"):
        if mode == "merge":
            raise NotImplementedError(
                "CSV provider does not support merge mode. "
                "Use 'append' or 'overwrite'. For upsert semantics, "
                "use a provider that implements MergeableProvider (e.g., Delta)."
            )

        # CSV supports append and overwrite
        path = self._get_path()
        df.write.format("csv").mode(mode).save(path)
```

### Prefer-Merge-Fallback Pattern

Framework implements "prefer best, accept acceptable" strategy:

```python
class DataEntityManager:
    """Entity manager with smart fallbacks"""

    def upsert_entity(
        self,
        entity_name: str,
        df: DataFrame,
        merge_keys: List[str],
        allow_fallback: bool = True
    ):
        """
        Intelligent upsert with fallback strategies.

        Strategy preference:
        1. Native merge (Delta) - best performance
        2. Read-union-dedupe-overwrite - acceptable
        3. Error if !allow_fallback
        """
        provider = self._get_provider(entity_name)

        # Strategy 1: Use native merge if available
        if isinstance(provider, MergeableProvider):
            self.logger.info(f"Using native merge for {entity_name}")
            provider.merge(df, merge_keys)
            return

        # Strategy 2: Fallback to manual merge
        if allow_fallback:
            self.logger.warning(
                f"Native merge not available for {entity_name}, "
                f"using read-union-overwrite fallback"
            )
            self._manual_merge(provider, df, merge_keys)
            return

        # Strategy 3: Error if fallback not allowed
        raise TypeError(
            f"Entity {entity_name} provider does not support merge "
            f"and allow_fallback=False"
        )

    def _manual_merge(
        self,
        provider: EntityProvider,
        new_df: DataFrame,
        merge_keys: List[str]
    ):
        """Manual merge fallback for non-mergeable providers"""
        if provider.exists():
            # Read existing data
            existing_df = provider.read()

            # Union and deduplicate
            from pyspark.sql import Window
            import pyspark.sql.functions as F

            combined = existing_df.union(new_df)

            # Keep latest record per merge key
            window = Window.partitionBy(*merge_keys).orderBy(F.desc("updated_at"))
            deduped = combined.withColumn("rn", F.row_number().over(window)) \
                .filter("rn = 1") \
                .drop("rn")

            # Overwrite with merged result
            provider.write(deduped, mode="overwrite")
        else:
            # No existing data, just write
            provider.write(new_df, mode="overwrite")
```

### Real-World Usage Example

```python
# User's data pipe - doesn't care about provider details
@data_pipe(name="silver.customer_updates", dependencies=["bronze.customers"])
def update_customers():
    entity_mgr = DataEntityManager()

    # Read new customer data
    new_customers = entity_mgr.read_entity("bronze.customers")

    # Transform
    processed = new_customers.withColumn("processed_at", F.current_timestamp())

    # Upsert - framework handles everything
    entity_mgr.upsert_entity(
        entity_name="silver.customers",
        df=processed,
        merge_keys=["customer_id"],
        allow_fallback=True  # Use best available strategy
    )
```

**What happens in different environments:**

**Production (Delta provider):**
```
INFO: Using native merge for silver.customers
→ Efficient Delta merge operation
→ Row-level upsert
→ Transaction log updated
→ Fast, atomic, ACID-compliant
```

**Dev (CSV provider):**
```
WARNING: Native merge not available for silver.customers, using read-union-overwrite fallback
→ Reads entire CSV
→ Unions with new data
→ Deduplicates by customer_id
→ Overwrites CSV file
→ Works, but slower and not atomic
```

**Test (In-memory provider):**
```
WARNING: Native merge not available for silver.customers, using in-memory union
→ Unions DataFrames in memory
→ Deduplicates
→ Stores back to in-memory dict
→ Fast for tests, but not persistent
```

### Interface Comparison Matrix

| Feature | Interface | Delta | Parquet | CSV | In-Memory |
|---------|-----------|-------|---------|-----|-----------|
| **Basic CRUD** | `EntityProvider` | ✅ | ✅ | ✅ | ✅ |
| **Time Travel** | `VersionedProvider` | ✅ | ❌ | ❌ | ❌ |
| **Change Stream** | `ChangeStreamProvider` | ✅ | ❌ | ❌ | ❌ |
| **Merge/Upsert** | `MergeableProvider` | ✅ | ❌ | ❌ | ⚠️ Limited |
| **Optimize** | `OptimizableProvider` | ✅ | ❌ | ❌ | ❌ |
| **Schema Evolution** | `SchemaEvolvableProvider` | ✅ | ✅ | ⚠️ Limited | ✅ |
| **Partitions** | `PartitionedProvider` | ✅ | ✅ | ❌ | ❌ |

**Watermarking Strategy by Interface:**
- `ChangeStreamProvider`: Optimal (change data feed)
- `VersionedProvider`: Good (version-based filtering)
- Base `EntityProvider`: Acceptable (timestamp-based filtering)

### Benefits of Interface-Based Design

**1. Standard Python Patterns**
```python
# Everyone understands isinstance()
if isinstance(provider, OptimizableProvider):
    provider.optimize()
```

**2. IDE Support**
- Autocomplete works on interface methods
- Type checkers understand interface contracts
- Refactoring tools work correctly

**3. Clear Declaration**
```python
# At a glance, you see what Delta supports
class DeltaEntityProvider(
    EntityProvider,          # Base: read/write/exists/delete
    VersionedProvider,       # Time travel
    ChangeStreamProvider,    # Change data feed
    MergeableProvider,       # Upserts
    OptimizableProvider,     # Compaction/vacuum
    SchemaEvolvableProvider, # Schema merge
    PartitionedProvider      # Partition management
):
```

**4. Graceful Degradation**
- Critical features (merge, time travel): Error if interface missing
- Optional features (optimize, vacuum): Skip if interface missing
- Degradable features (watermarking): Use fallback strategy

## Configuration Examples

### Example 1: Production Setup (Layered Providers)

```yaml
my_package:
  data_entities:
    # Bronze: Delta for raw ingestion
    bronze.*:
      tags:
        provider: delta
        base_path: abfss://bronze@prod.dfs.core.windows.net/data

    # Silver: Delta for transformations
    silver.*:
      tags:
        provider: delta
        base_path: abfss://silver@prod.dfs.core.windows.net/data

    # Gold: Delta for analytics
    gold.*:
      tags:
        provider: delta
        base_path: abfss://gold@prod.dfs.core.windows.net/data

    # Test fixtures: CSV files
    test.*:
      tags:
        provider: csv
        base_path: /dbfs/test_fixtures

    # External systems
    external.snowflake_customers:
      tags:
        provider: snowflake
        account: mycompany.us-east-1
        database: SALES
        schema: CUSTOMERS
        table: DIM_CUSTOMER
```

### Example 2: Development Environment

```yaml
# env_dev.yaml
my_package:
  data_entities:
    # Override everything to use local CSV files
    "*":
      tags:
        provider: csv
        base_path: ./dev_data

    # Except external entities still hit real systems
    external.*:
      tags:
        provider: snowflake  # Still use Snowflake in dev
```

### Example 3: Test Environment

```yaml
# env_test.yaml
my_package:
  data_entities:
    # All entities use in-memory for fast unit tests
    "*":
      tags:
        provider: in_memory

    # Except integration test entities use CSV fixtures
    test.integration.*:
      tags:
        provider: csv
        base_path: /tmp/integration_fixtures
```

### Example 4: Staging (Cost-Optimized)

```yaml
# env_staging.yaml
my_package:
  data_entities:
    # Use Parquet in staging to save costs (no Delta overhead)
    bronze.*:
      tags:
        provider: parquet
        base_path: abfss://staging@storage.dfs.core.windows.net/bronze
        partition_by: [date]

    silver.*:
      tags:
        provider: parquet
        base_path: abfss://staging@storage.dfs.core.windows.net/silver
        partition_by: [date]
```

### Example 5: Provider-Specific Configuration

```yaml
my_package:
  data_entities:
    # Delta with merge configuration
    silver.customers:
      tags:
        provider: delta
        base_path: /mnt/silver
        merge_keys: [customer_id]  # For merge mode writes

    # Parquet with partitioning
    bronze.events:
      tags:
        provider: parquet
        base_path: /mnt/bronze
        partition_by: [year, month, day]

    # Snowflake with connection config
    external.orders:
      tags:
        provider: snowflake
        account: mycompany.us-east-1
        database: SALES
        schema: ORDERS
        table: FACT_ORDERS
        warehouse: ANALYTICS_WH
        role: DATA_READER
```

## Use Cases

### 1. Fast Unit Testing

**Before (with hardcoded Delta):**
```python
def test_pipeline():
    # Requires real Delta table infrastructure
    setup_delta_table("bronze.customers")
    write_test_data_to_delta("bronze.customers", test_df)

    # Run pipeline (slow, requires cloud)
    run_pipeline()

    # Read results
    result = read_delta_table("silver.processed")
    assert result.count() == 10
```

**After (with in-memory provider):**
```python
def test_pipeline():
    # In-memory config already set in env_test.yaml
    entity_mgr = DataEntityManager()

    # Write test data (instant, no I/O)
    entity_mgr.write_entity("bronze.customers", test_df)

    # Run pipeline (fast, local)
    run_pipeline()

    # Read results (instant)
    result = entity_mgr.read_entity("silver.processed")
    assert result.count() == 10
```

### 2. Local Development Without Cloud Access

**Before:**
- Developer needs Azure/Databricks credentials
- Must provision lakehouse/workspace resources
- Can't work offline
- Slow feedback loop

**After:**
```yaml
# env_dev.yaml - developer's local machine
my_package:
  data_entities:
    "*":
      tags:
        provider: csv
        base_path: ./local_data  # Just local files!
```

Developer workflow:
1. Clone repo
2. Put CSV test files in `./local_data/`
3. Run code locally with Spark
4. No cloud credentials needed

### 3. External System Integration

```yaml
my_package:
  data_entities:
    # Read from Snowflake
    external.customer_master:
      tags:
        provider: snowflake
        database: PROD
        schema: DIM

    # Read from REST API
    external.weather_data:
      tags:
        provider: rest_api
        endpoint: https://api.weather.com/v1/data
        auth_token_secret: weather_api_token

    # Write to internal Delta
    silver.enriched_orders:
      tags:
        provider: delta
        base_path: /mnt/silver
```

### 4. Migration Path (CSV → Parquet → Delta)

**Phase 1: Prototype with CSV**
```yaml
bronze.*:
  tags:
    provider: csv
    base_path: /dbfs/prototype
```

**Phase 2: Staging with Parquet**
```yaml
bronze.*:
  tags:
    provider: parquet
    base_path: /mnt/staging
```

**Phase 3: Production with Delta**
```yaml
bronze.*:
  tags:
    provider: delta
    base_path: /mnt/prod
```

Code never changes - just configuration evolves.

### 5. Testing Strategy

```yaml
# Different providers for different test types
my_package:
  data_entities:
    # Unit tests: in-memory (fast, isolated)
    test.unit.*:
      tags:
        provider: in_memory

    # Integration tests: CSV fixtures (controlled data)
    test.integration.*:
      tags:
        provider: csv
        base_path: /tmp/test_fixtures

    # System tests: real Delta (production-like)
    test.system.*:
      tags:
        provider: delta
        base_path: /mnt/test_delta
```

## Benefits

### 1. Testing Benefits
- ✅ **10-100x faster unit tests** - in-memory provider eliminates I/O
- ✅ **No flaky cloud tests** - local providers remove network dependencies
- ✅ **Perfect isolation** - each test gets clean in-memory storage
- ✅ **Simple fixtures** - CSV files in version control

### 2. Developer Experience
- ✅ **Work offline** - CSV/in-memory providers need no cloud
- ✅ **Fast feedback** - no waiting for cloud operations
- ✅ **Lower barrier to entry** - new devs don't need cloud access
- ✅ **Laptop development** - run full pipelines locally

### 3. Flexibility
- ✅ **Multi-cloud** - provider per cloud vendor (Azure, AWS, GCP)
- ✅ **External systems** - read from Snowflake, BigQuery, APIs
- ✅ **Staged rollout** - different providers per environment
- ✅ **Cost optimization** - Parquet in staging, Delta in prod

### 4. Maintainability
- ✅ **Config-driven** - change providers without code changes
- ✅ **Wildcard patterns** - bulk configuration for layers
- ✅ **Clear boundaries** - provider abstraction enforces contracts
- ✅ **Extensible** - add new providers without framework changes

## Implementation Phases

### Phase 1: Core Interfaces (1 week)
- [ ] Define `EntityProvider` base interface (read, write, exists, delete)
  - Base constructor: `__init__(entity: DataEntity, config: ConfigService)`
  - NO shared naming/path services - providers handle internally
- [ ] Define optional interfaces:
  - `VersionedProvider` (time travel)
  - `ChangeStreamProvider` (change data feed)
  - `MergeableProvider` (upserts)
  - `OptimizableProvider` (compaction/vacuum)
  - `SchemaEvolvableProvider` (schema merge)
  - `PartitionedProvider` (partition management)
- [ ] Create `EntityProviderRegistry` class
- [ ] Write unit tests for registry and interfaces

### Phase 2: Provider Adapter (1 week)
- [ ] Implement `EntityProviderAdapter` that wraps any provider
- [ ] Adapter does NOT inject naming services (providers handle internally)
- [ ] Implement all interface methods with delegation + fallback:
  - `merge()` with read-union-dedupe-overwrite fallback
  - `read_changes()` with version/timestamp fallback
  - `optimize()` with no-op fallback
  - Time travel methods with error fallback
- [ ] Write comprehensive adapter tests with mock providers
- [ ] Test fallback strategies work correctly

### Phase 3: Built-In Providers (2-3 weeks)

**Phase 3a: DeltaEntityProvider (1 week)**
- [ ] Update `DeltaEntityProvider` to use new base interface
- [ ] Keep existing `EntityNameMapper`/`EntityPathLocator` dependencies (Delta-specific)
- [ ] Implement all Delta interfaces:
  - Core CRUD operations
  - Extended methods: `get_version()`, `read_changes()`, `merge()`
  - Optimization methods: `optimize()`, `vacuum()`
- [ ] Update unit/integration tests
- [ ] **No breaking changes** - existing code continues to work

**Phase 3b: CSV/Parquet Providers (1 week)**
- [ ] Implement `CsvEntityProvider` (base interface only)
  - Internal file path resolution (no EntityNameMapper/EntityPathLocator)
  - Layer-based path conventions
  - CSV-specific options from tags
- [ ] Implement `ParquetEntityProvider` (base + schema evolution + partitioning)
  - Internal directory path resolution
  - Partition support from tags
- [ ] Write unit tests for each provider
- [ ] Integration tests with actual storage

**Phase 3c: InMemoryProvider (1 week)**
- [ ] Implement `InMemoryEntityProvider` (test-only, base interface)
  - No paths - pure in-memory storage
  - Class-level storage dict
  - Simple entity.name as key
- [ ] Write unit tests
- [ ] Update system tests to use in-memory provider
- [ ] Provider feature matrix documentation

### Phase 4: Entity Manager Integration (1-2 weeks)
- [ ] Update `DataEntityManager` to use adapter-wrapped providers
- [ ] Simplify methods (no more interface checking):
  - `read_entity()` → just calls `provider.read()`
  - `upsert_entity()` → just calls `provider.merge()`
  - `optimize_entity()` → just calls `provider.optimize()`
  - `read_entity_version()` → just calls `provider.read_version()`
- [ ] Add `_get_adapted_provider()` helper method
- [ ] Update existing code to use new API
- [ ] Migration guide for users

### Phase 5: Watermark Manager Adaptation (1 week)
- [ ] Update `WatermarkManager` to use adapter
- [ ] Simplify incremental read logic:
  - Just call `provider.read_changes(from_version)`
  - Adapter handles CDF vs versioning vs timestamp fallback
- [ ] Update watermark save to use `provider.get_version()`
- [ ] Clear error messages when features not available
- [ ] Update watermark documentation

### Phase 6: Configuration Integration (1 week)
- [ ] Add provider resolution from tags
- [ ] Support wildcard patterns for provider config
- [ ] Environment-specific provider overrides
- [ ] Provider-specific configuration options:
  - Delta: `table_name`, `table_path`, `merge_schema`
  - CSV: `file_path`, `csv_options`
  - Parquet: `directory_path`, `partition_by`
  - InMemory: (no configuration needed)
- [ ] Validation and error handling
- [ ] Configuration examples showing adapter behavior
- [ ] Document fallback strategies per interface

### Phase 7: Testing & Documentation (1 week)
- [ ] Comprehensive test suite for all providers
- [ ] Adapter testing (verify all fallback paths)
- [ ] Update all system tests to use in-memory provider
- [ ] Migration guide for existing codebases
- [ ] User guide with examples
- [ ] Provider development guide (including when to use internal naming vs services)
- [ ] Interface implementation guide
- [ ] Feature comparison matrix documentation
- [ ] Document provider-specific naming patterns:
  - Delta: EntityNameMapper/EntityPathLocator (table-centric)
  - CSV: File path conventions (file-centric)
  - Parquet: Directory path conventions (directory-centric)
  - InMemory: Simple keys (no paths)

**Total Estimated Time:** 8-10 weeks

**Key Architectural Decision:**
- **EntityNameMapper and EntityPathLocator remain Delta-specific services**
- **Other providers use internal naming conventions appropriate to their storage model**
- **No leaky abstractions** - CSV doesn't pretend to have "table names"
- **Adapter focuses on interface fallbacks, NOT naming services**

## Related Patterns & Cross-Cutting Concerns

### Secret Management for External Providers

External entity providers (Snowflake, REST APIs, databases) require credentials. See **[Secret Provider Service Proposal](secret_provider_service.md)** for platform-specific secret management:

```python
class SnowflakeEntityProvider(EntityProvider):
    @inject
    def __init__(
        self,
        entity: DataEntity,
        config: ConfigService,
        secret_provider: SecretProvider,  # ← Platform-specific secrets
        logger_provider: PythonLoggerProvider
    ):
        super().__init__(entity, config)

        # Get credentials from platform-specific secret store
        # - Fabric/Synapse: Azure Key Vault via mssparkutils.credentials
        # - Databricks: Databricks Secrets via dbutils.secrets
        # - Standalone: Environment variables
        self.username = secret_provider.get_secret("snowflake-username")
        self.password = secret_provider.get_secret("snowflake-password")
```

**Benefits:**
- ✅ **Platform abstraction** - Same code works on Fabric, Synapse, Databricks
- ✅ **Secure by default** - Secrets never in logs or config files
- ✅ **Configuration-driven** - Secret vault/scope specified in settings
- ✅ **Enables external providers** - Snowflake, APIs, databases can use secrets

### Storage Operations

Entity providers may need platform-specific storage operations (copy, exists, delete). Use `PlatformServiceProvider` for file system operations:

```python
class CsvEntityProvider(EntityProvider):
    def exists(self) -> bool:
        from kindling.platform_provider import SparkPlatformServiceProvider
        platform = SparkPlatformServiceProvider().get_service()
        return platform.path_exists(self._get_file_path())
```

### Authentication Tokens

External entity providers accessing REST APIs may need authentication tokens. Use `platform.get_token(audience)` from PlatformService:

```python
class RestApiEntityProvider(EntityProvider):
    def _get_auth_token(self):
        from kindling.platform_provider import SparkPlatformServiceProvider
        platform = SparkPlatformServiceProvider().get_service()
        return platform.get_token("https://api.example.com")
```

---

## Migration Path

### Support Services Impact Analysis

The current `DeltaEntityProvider` depends on support services that provide naming and path conventions:

```python
# Current implementation
@inject
def __init__(
    self,
    config: ConfigService,
    entity_name_mapper: EntityNameMapper,      # Maps entity.name → table name
    path_locator: EntityPathLocator,           # Gets storage path for entity
    logger_provider: PythonLoggerProvider,
    signal_provider: SignalProvider
):
```

**Question:** Where do these services fit in the new architecture?

#### Option A: Inject into Each Provider (Current Pattern)

```python
class DeltaEntityProvider(EntityProvider, MergeableProvider, ...):
    @inject
    def __init__(
        self,
        entity_entry: DataEntity,
        entity_name_mapper: EntityNameMapper,
        path_locator: EntityPathLocator,
        config: ConfigService
    ):
        self.entity = entity_entry
        self.name_mapper = entity_name_mapper
        self.path_locator = path_locator
        self.config = config

    def _get_path(self) -> str:
        return self.path_locator.get_table_path(self.entity)

    def _get_table_name(self) -> str:
        return self.name_mapper.get_table_name(self.entity)
```

**Pros:**
- ✅ Providers can use naming/path conventions
- ✅ Consistent with current pattern
- ✅ Services are reusable across providers

**Cons:**
- ❌ Every provider needs to know about framework services
- ❌ CSV/Parquet providers need path locators too
- ❌ Tight coupling between providers and framework

#### Option B: Move to Provider Factory

```python
@inject
class EntityProviderFactory:
    """Factory creates providers with support services"""

    def __init__(
        self,
        entity_name_mapper: EntityNameMapper,
        path_locator: EntityPathLocator,
        config: ConfigService,
        logger_provider: LoggerProvider
    ):
        self.name_mapper = entity_name_mapper
        self.path_locator = path_locator
        self.config = config
        self.logger = logger_provider.get_logger("provider_factory")

    def create_provider(self, entity_entry: DataEntity) -> EntityProvider:
        """Create provider with all support services injected"""
        provider_name = entity_entry.tags.get("provider", "delta")

        if provider_name == "delta":
            provider = DeltaEntityProvider(
                entity_entry,
                self.name_mapper,
                self.path_locator,
                self.config
            )
        elif provider_name == "csv":
            provider = CsvEntityProvider(
                entity_entry,
                self.path_locator  # CSV just needs path
            )
        # ... etc

        # Wrap in adapter
        return EntityProviderAdapter(provider)
```

**Pros:**
- ✅ Centralized provider creation
- ✅ Support services injected in one place
- ✅ Clean separation of concerns

**Cons:**
- ❌ Factory grows with each provider type
- ❌ Providers still need support service dependencies

#### Option C: Support Services in Tags

```python
class DeltaEntityProvider(EntityProvider, MergeableProvider, ...):
    """Provider gets everything from entity tags"""

    def __init__(self, entity_entry: DataEntity):
        self.entity = entity_entry
        self.tags = entity_entry.tags

    def _get_path(self) -> str:
        # Path comes from tags (set by framework during registration)
        base_path = self.tags.get("base_path")
        return f"{base_path}/{self.entity.name.replace('.', '/')}"

    def _get_table_name(self) -> str:
        # Table name comes from tags (set by framework)
        return self.tags.get("table_name", self.entity.name)

    def read(self) -> DataFrame:
        path = self._get_path()
        return spark.read.format("delta").load(path)
```

**How it works:**
```python
# Framework enriches entity before provider sees it
@inject
class DataEntityManager:
    def __init__(
        self,
        entity_name_mapper: EntityNameMapper,
        path_locator: EntityPathLocator
    ):
        self.name_mapper = entity_name_mapper
        self.path_locator = path_locator

    def _get_adapted_provider(self, entity_name: str) -> EntityProviderAdapter:
        entity_entry = DataEntities.get_registered_entity(entity_name)

        # Enrich tags with support service results
        entity_entry.tags["table_name"] = self.name_mapper.get_table_name(entity_entry)
        entity_entry.tags["table_path"] = self.path_locator.get_table_path(entity_entry)
        entity_entry.tags["base_path"] = self.path_locator.get_base_path(entity_entry)

        # Create provider (tags contain everything it needs)
        provider_name = entity_entry.tags.get("provider", "delta")
        provider_class = EntityProviders.get_provider(provider_name)
        raw_provider = provider_class(entity_entry)

        # Wrap in adapter
        return EntityProviderAdapter(raw_provider)
```

**Pros:**
- ✅ Providers are pure - no framework dependencies
- ✅ Tags are already the configuration mechanism
- ✅ Easy to test providers (just pass tags dict)

**Cons:**
- ❌ **Loses pattern-based naming** - path computed once, can't leverage patterns
- ❌ **Loses naming flexibility** - can't change naming strategy per operation
- ❌ Support services run on every provider creation (inefficient)

#### Option D: Inject Support Services via Adapter

Providers stay pure, but adapter provides naming/path services:

```python
class EntityProviderAdapter(
    EntityProvider,
    MergeableProvider,
    VersionedProvider,
    # ... all interfaces
):
    """
    Adapter wraps provider AND provides support services.
    Provider can request naming/path resolution via adapter.
    """

    @inject
    def __init__(
        self,
        provider: EntityProvider,
        entity_name_mapper: EntityNameMapper,
        path_locator: EntityPathLocator,
        config: ConfigService,
        logger_provider: LoggerProvider
    ):
        self.provider = provider
        self.entity = provider.entity
        self.tags = provider.tags

        # Support services available to adapter
        self.name_mapper = entity_name_mapper
        self.path_locator = path_locator
        self.config = config
        self.logger = logger_provider.get_logger("provider_adapter")

        # Inject support services into provider if it has the methods
        if hasattr(provider, 'set_naming_context'):
            provider.set_naming_context(self)

    def get_table_name(self, entity: DataEntity) -> str:
        """Naming service available to providers"""
        return self.name_mapper.get_table_name(entity)

    def get_table_path(self, entity: DataEntity) -> str:
        """Path resolution service available to providers"""
        return self.path_locator.get_table_path(entity)

    def get_base_path(self, layer: str = None) -> str:
        """Base path resolution with layer support"""
        return self.path_locator.get_base_path(layer)

    # Delegate base methods to provider
    def read(self) -> DataFrame:
        return self.provider.read()

    # ... other interface methods
```

**Provider uses adapter for naming:**

```python
class DeltaEntityProvider(EntityProvider, MergeableProvider, ...):
    """Provider is pure but can request naming services"""

    def __init__(self, entity_entry: DataEntity):
        self.entity = entity_entry
        self.tags = entity_entry.tags
        self.naming_context = None  # Set by adapter

    def set_naming_context(self, adapter):
        """Adapter injects itself for naming services"""
        self.naming_context = adapter

    def _get_path(self) -> str:
        """Use pattern-based path resolution"""
        if self.naming_context:
            # Use naming pattern from support services
            return self.naming_context.get_table_path(self.entity)
        else:
            # Fallback to tags (for testing without adapter)
            base_path = self.tags.get("base_path", "/mnt/data")
            return f"{base_path}/{self.entity.name.replace('.', '/')}"

    def _get_table_name(self) -> str:
        """Use pattern-based naming"""
        if self.naming_context:
            return self.naming_context.get_table_name(self.entity)
        else:
            return self.tags.get("table_name", self.entity.name)

    def read(self) -> DataFrame:
        path = self._get_path()  # Uses naming pattern!
        return spark.read.format("delta").load(path)
```

**Pros:**
- ✅ **Preserves naming patterns** - path resolution uses conventions
- ✅ **Providers stay pure** - optional dependency, works without adapter
- ✅ **Support services centralized** - injected once into adapter
- ✅ **Pattern flexibility** - naming/path strategies can change
- ✅ **Easy testing** - provider works with or without adapter

**Cons:**
- ⚠️ Generic EntityNameMapper/PathLocator - not provider-specific
- ⚠️ Adapter needs DI for multiple services

#### Option E: Provider-Specific Naming Conventions (Recommended)

Register naming/path conventions per provider, similar to file ingestion patterns:

```python
class ProviderNamingConvention(ABC):
    """Base class for provider-specific naming conventions"""

    @abstractmethod
    def get_table_name(self, entity: DataEntity, config: ConfigService) -> str:
        """Get table name using provider conventions"""
        pass

    @abstractmethod
    def get_table_path(self, entity: DataEntity, config: ConfigService) -> str:
        """Get storage path using provider conventions"""
        pass

class DeltaNamingConvention(ProviderNamingConvention):
    """Delta Lake naming conventions"""

    def get_table_name(self, entity: DataEntity, config: ConfigService) -> str:
        """Pattern: {layer}_{entity_name}"""
        layer, name = entity.name.split('.', 1)
        return f"{layer}_{name.replace('.', '_')}"

    def get_table_path(self, entity: DataEntity, config: ConfigService) -> str:
        """Pattern: {base_path}/{layer}/{entity_name}"""
        layer, name = entity.name.split('.', 1)
        base_path = config.get(f"{layer.upper()}_BASE_PATH", "/mnt/data")
        return f"{base_path}/{layer}/{name}"

class CsvNamingConvention(ProviderNamingConvention):
    """CSV file naming conventions"""

    def get_table_name(self, entity: DataEntity, config: ConfigService) -> str:
        """CSV doesn't use table names"""
        return None

    def get_table_path(self, entity: DataEntity, config: ConfigService) -> str:
        """Pattern: {base_path}/{entity_name}.csv"""
        base_path = config.get("CSV_BASE_PATH", "./data")
        return f"{base_path}/{entity.name.replace('.', '/')}.csv"

# Register conventions per provider
ProviderConventions.register("delta", DeltaNamingConvention())
ProviderConventions.register("csv", CsvNamingConvention())
ProviderConventions.register("parquet", ParquetNamingConvention())
```

**Configuration with custom conventions:**

```yaml
my_package:
  # Define provider conventions (like file ingestion patterns)
  provider_conventions:
    delta:
      table_name_pattern: "{layer}_{name}"
      table_path_pattern: "{base_path}/{layer}/{name}"

    csv:
      table_path_pattern: "{base_path}/{layer}/{name}.csv"

    parquet:
      table_path_pattern: "{base_path}/{layer}/{name}.parquet"
      partition_pattern: "{layer}/{year}/{month}/{day}/{name}"

  # Layer-specific base paths
  storage:
    bronze_base: /mnt/bronze
    silver_base: /mnt/silver
    gold_base: /mnt/gold
    test_base: /tmp/test

  # Override convention for specific entity
  data_entities:
    bronze.special_customers:
      tags:
        provider: delta
        table_path: /mnt/archive/special/customers  # Override pattern
```

**Provider uses registered conventions:**

```python
class DeltaEntityProvider(EntityProvider, MergeableProvider, ...):
    """Provider uses its registered naming convention"""

    def __init__(self, entity_entry: DataEntity):
        self.entity = entity_entry
        self.tags = entity_entry.tags
        self.convention = None  # Set by adapter
        self.config = None

    def set_naming_convention(self, convention: ProviderNamingConvention, config: ConfigService):
        """Adapter injects the convention"""
        self.convention = convention
        self.config = config

    def _get_path(self) -> str:
        """Use convention or fallback to tags"""
        # Priority 1: Tag override
        if "table_path" in self.tags:
            return self.tags["table_path"]

        # Priority 2: Naming convention
        if self.convention:
            return self.convention.get_table_path(self.entity, self.config)

        # Priority 3: Simple fallback
        base_path = self.tags.get("base_path", "/mnt/data")
        return f"{base_path}/{self.entity.name.replace('.', '/')}"

    def _get_table_name(self) -> str:
        """Use convention or fallback"""
        if "table_name" in self.tags:
            return self.tags["table_name"]

        if self.convention:
            return self.convention.get_table_name(self.entity, self.config)

        return self.entity.name
```

**Adapter injects conventions:**

```python
class EntityProviderAdapter:
    """Adapter injects provider-specific conventions"""

    @inject
    def __init__(
        self,
        provider: EntityProvider,
        config: ConfigService,
        logger_provider: LoggerProvider
    ):
        self.provider = provider
        self.config = config

        # Get provider-specific naming convention
        provider_name = provider.tags.get("provider", "delta")
        convention = ProviderConventions.get(provider_name)

        # Inject into provider
        if hasattr(provider, 'set_naming_convention'):
            provider.set_naming_convention(convention, config)
```

**Pros:**
- ✅ **Provider-specific conventions** - Delta has different patterns than CSV
- ✅ **Pattern-based like file ingestion** - familiar pattern
- ✅ **Configurable** - users can override patterns in YAML
- ✅ **Clean provider interface** - just calls convention methods
- ✅ **Single responsibility** - convention handles all naming/path logic
- ✅ **Adapter stays simple** - just injects config and convention

**Cons:**
- None significant - this is the cleanest approach

#### ⚠️ Critical Flaw: EntityNameMapper/EntityPathLocator Are Delta-Specific

**All options above (A-E) share a fundamental architectural problem:**

`EntityNameMapper` and `EntityPathLocator` are **Delta table abstractions** that don't translate to other providers:

| Provider | get_table_name() | get_table_path() |
|----------|------------------|------------------|
| **Delta** | ✅ Catalog table name (`bronze.customers`) | ✅ ABFSS path (`abfss://...`) |
| **CSV** | ❌ No catalog concept | ✅ File path (`/data/customers.csv`) |
| **Parquet** | ❌ No catalog concept | ✅ Directory path (`/data/customers/`) |
| **InMemory** | ❌ No persistence | ❌ No paths (just keys) |

**The Problem:**
```python
# Current - assumes all providers use table names + paths
@inject
def __init__(
    self,
    entity_name_mapper: EntityNameMapper,  # ❌ Delta-centric
    path_locator: EntityPathLocator        # ❌ Delta-centric
):
    pass
```

**Why This Breaks:**
1. **CSV doesn't have "table names"** - It's just files, no catalog registration
2. **InMemory doesn't have "paths"** - Pure in-memory DataFrame storage
3. **Parquet paths are different** - Directory-based, not table-based
4. **One naming service for all** - Can't express provider-specific conventions

#### Recommendation: Provider-Internal Naming Conventions (Option F)

**Each provider handles its own naming/path resolution internally:**

```python
# ===== Base Provider Interface - No Naming Services =====

class EntityProvider(ABC):
    """
    Base provider interface - NO naming/path services.
    Each provider implements its own resolution internally.
    """

    def __init__(self, entity: DataEntity, config: ConfigService):
        """
        All providers receive:
        - entity: The entity definition (name, schema, tags)
        - config: Global config for base paths, environment, etc.
        """
        self.entity = entity
        self.config = config
        self.tags = entity.tags

    @abstractmethod
    def read(self) -> DataFrame:
        pass

    @abstractmethod
    def write(self, df: DataFrame, mode: str = "append"):
        pass

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def delete(self):
        pass


# ===== Delta Provider - Uses EntityNameMapper/EntityPathLocator =====

class DeltaEntityProvider(EntityProvider, MergeableProvider, VersionedProvider, ...):
    """
    Delta provider uses table-centric naming services.
    These services are DELTA-SPECIFIC, not shared with other providers.
    """

    @inject
    def __init__(
        self,
        entity: DataEntity,
        config: ConfigService,
        entity_name_mapper: EntityNameMapper,  # Delta-specific service
        path_locator: EntityPathLocator,       # Delta-specific service
        logger: LoggerProvider
    ):
        super().__init__(entity, config)
        self.name_mapper = entity_name_mapper
        self.path_locator = path_locator
        self.logger = logger.get_logger("DeltaEntityProvider")

    def _get_table_name(self) -> str:
        """Use Delta naming convention"""
        # Priority 1: Tag override
        if "table_name" in self.tags:
            return self.tags["table_name"]

        # Priority 2: Naming service (pattern-based)
        return self.name_mapper.get_table_name(self.entity)

    def _get_table_path(self) -> str:
        """Use Delta path resolution"""
        # Priority 1: Tag override
        if "table_path" in self.tags:
            return self.tags["table_path"]

        # Priority 2: Path locator (pattern-based)
        return self.path_locator.get_table_path(self.entity)

    def read(self, version: int = None) -> DataFrame:
        path = self._get_table_path()
        reader = spark.read.format("delta")

        if version is not None:
            reader = reader.option("versionAsOf", version)

        return reader.load(path)

    def write(self, df: DataFrame, mode: str = "append"):
        path = self._get_table_path()
        df.write.format("delta").mode(mode).save(path)

    def exists(self) -> bool:
        try:
            DeltaTable.forPath(spark, self._get_table_path())
            return True
        except:
            return False


# ===== CSV Provider - File-Centric Resolution =====

class CsvEntityProvider(EntityProvider):
    """
    CSV provider uses file paths, NO naming services.
    Resolution is internal to the provider.
    """

    def __init__(
        self,
        entity: DataEntity,
        config: ConfigService,
        logger: LoggerProvider
    ):
        super().__init__(entity, config)
        self.logger = logger.get_logger("CsvEntityProvider")

    def _get_file_path(self) -> str:
        """CSV-specific path resolution - NO external service needed"""

        # Priority 1: Tag override
        if "file_path" in self.tags:
            return self.tags["file_path"]

        # Priority 2: Provider convention (layer-based paths)
        layer = self.entity.name.split('.')[0]
        name = self.entity.name.split('.', 1)[1]

        # Get base path from config
        base_path = self.config.get(f"{layer.upper()}_BASE_PATH", "./data")

        # CSV convention: {base_path}/{layer}/{name}.csv
        return f"{base_path}/{layer}/{name.replace('.', '/')}.csv"

    def read(self) -> DataFrame:
        path = self._get_file_path()

        # CSV reading with options from tags
        csv_options = self.tags.get("csv_options", {})
        reader = spark.read.format("csv").options(**csv_options)

        if self.entity.schema:
            reader = reader.schema(self.entity.schema)
        else:
            reader = reader.option("inferSchema", "true")

        return reader.load(path)

    def write(self, df: DataFrame, mode: str = "append"):
        path = self._get_file_path()

        if mode == "append":
            # CSV append requires read-union-write pattern
            if self.exists():
                existing = self.read()
                df = existing.union(df)
            mode = "overwrite"

        csv_options = self.tags.get("csv_options", {})
        df.write.format("csv").mode(mode).options(**csv_options).save(path)

    def exists(self) -> bool:
        from kindling.platform_provider import SparkPlatformServiceProvider
        platform = SparkPlatformServiceProvider().get_service()
        return platform.path_exists(self._get_file_path())


# ===== Parquet Provider - Directory-Centric Resolution =====

class ParquetEntityProvider(EntityProvider):
    """
    Parquet provider uses directory paths, NO naming services.
    """

    def __init__(
        self,
        entity: DataEntity,
        config: ConfigService,
        logger: LoggerProvider
    ):
        super().__init__(entity, config)
        self.logger = logger.get_logger("ParquetEntityProvider")

    def _get_directory_path(self) -> str:
        """Parquet-specific path resolution"""

        # Priority 1: Tag override
        if "directory_path" in self.tags:
            return self.tags["directory_path"]

        # Priority 2: Provider convention
        layer = self.entity.name.split('.')[0]
        name = self.entity.name.split('.', 1)[1]

        base_path = self.config.get(f"{layer.upper()}_BASE_PATH", "./data")

        # Parquet convention: {base_path}/{layer}/{name}/
        return f"{base_path}/{layer}/{name.replace('.', '/')}"

    def read(self) -> DataFrame:
        path = self._get_directory_path()
        return spark.read.format("parquet").load(path)

    def write(self, df: DataFrame, mode: str = "append"):
        path = self._get_directory_path()

        # Support partitioning from tags
        partition_cols = self.tags.get("partition_by", [])
        writer = df.write.format("parquet").mode(mode)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(path)

    def exists(self) -> bool:
        from kindling.platform_provider import SparkPlatformServiceProvider
        platform = SparkPlatformServiceProvider().get_service()
        return platform.path_exists(self._get_directory_path())


# ===== InMemory Provider - No Paths at All =====

class InMemoryEntityProvider(EntityProvider):
    """
    In-memory provider uses entity name as key.
    NO paths, NO naming services.
    """

    # Class-level storage
    _storage: Dict[str, DataFrame] = {}

    def __init__(
        self,
        entity: DataEntity,
        config: ConfigService,
        logger: LoggerProvider
    ):
        super().__init__(entity, config)
        self.logger = logger.get_logger("InMemoryEntityProvider")

    def _get_storage_key(self) -> str:
        """Simple key - just use entity name"""
        return self.entity.name

    def read(self) -> DataFrame:
        key = self._get_storage_key()

        if key not in self._storage:
            if self.entity.schema:
                return spark.createDataFrame([], self.entity.schema)
            else:
                raise ValueError(f"Entity {key} not found in memory")

        return self._storage[key]

    def write(self, df: DataFrame, mode: str = "append"):
        key = self._get_storage_key()

        if mode == "overwrite":
            self._storage[key] = df
        elif mode == "append":
            if key in self._storage:
                self._storage[key] = self._storage[key].union(df)
            else:
                self._storage[key] = df

    def exists(self) -> bool:
        return self._get_storage_key() in self._storage

    def delete(self):
        key = self._get_storage_key()
        if key in self._storage:
            del self._storage[key]
```

**Configuration Example:**

```yaml
kindling:
  # Layer-specific base paths (shared across providers)
  storage:
    bronze_base_path: /mnt/bronze
    silver_base_path: /mnt/silver
    gold_base_path: /mnt/gold
    test_base_path: /tmp/test

  # Default provider
  entity_provider: delta

  # Entity-specific overrides
  data_entities:
    bronze.customers:
      tags:
        provider: delta
        # Delta-specific tags
        table_name: bronze.raw_customers
        table_path: abfss://data@lake.dfs.core.windows.net/bronze/customers

    test.sample_data:
      tags:
        provider: csv
        # CSV-specific tags
        file_path: ./test_data/sample.csv
        csv_options:
          header: true
          delimiter: ","

    bronze.legacy_orders:
      tags:
        provider: parquet
        # Parquet-specific tags
        directory_path: /mnt/legacy/orders
        partition_by: ["year", "month"]

    test.temp_results:
      tags:
        provider: in_memory
        # In-memory has no path tags
```

**Pros:**
- ✅ **No leaky abstractions** - CSV doesn't pretend to have "table names"
- ✅ **Provider-specific logic** - Each provider uses appropriate concepts
- ✅ **Delta keeps current services** - EntityNameMapper/EntityPathLocator work as-is
- ✅ **Simple configuration** - Tags express provider-specific needs
- ✅ **Clean interfaces** - No shared naming services that don't fit
- ✅ **Easy testing** - Each provider standalone with just config
- ✅ **Flexible conventions** - Delta can use patterns, CSV uses simple paths

**Cons:**
- ⚠️ **No shared naming logic** - Each provider implements its own (but that's correct!)
- ⚠️ **Multiple DI paths** - Delta has more dependencies than CSV (but that reflects reality)

**Why This Is Correct:**
1. **Different storage models need different concepts:**
   - Delta: Catalog tables + ABFSS paths
   - CSV: File paths
   - Parquet: Directory paths (often partitioned)
   - InMemory: Just keys
2. **EntityNameMapper/EntityPathLocator are Delta-specific** - Trying to generalize them forces CSV to pretend it has catalog tables
3. **Provider-internal conventions match storage model** - Each provider expresses naming in terms that make sense for that storage type
4. **Configuration is provider-specific** - Tags naturally express different needs per provider

**Migration from Current Code:**
- `DeltaEntityProvider` keeps current `EntityNameMapper`/`EntityPathLocator` dependencies
- New providers (CSV, Parquet, InMemory) don't use these services
- No breaking changes to existing Delta code
- Adapter still wraps providers for interface fallbacks, but doesn't inject naming services

#### Updated Recommendation: Option F (Provider-Internal Naming)

**This is the architecturally correct approach because:**

#### Updated Recommendation: Option F (Provider-Internal Naming)

**This is the architecturally correct approach because:**

1. **Respects Storage Model Differences:**
   - Delta tables have catalog names + storage paths
   - CSV files have file paths only
   - Parquet uses directories (often partitioned)
   - InMemory uses simple keys

2. **No Leaky Abstractions:**
   - Don't force CSV to implement `get_table_name()` when it has no tables
   - Don't force InMemory to implement `get_table_path()` when it has no persistence
   - Each provider expresses concepts natural to its storage model

3. **Delta-Specific Services Stay Delta-Specific:**
   - `EntityNameMapper` and `EntityPathLocator` remain as Delta services
   - Other providers don't pretend to use these abstractions
   - Clean separation of concerns

4. **Configuration Matches Reality:**
   ```yaml
   bronze.customers:
     tags:
       provider: delta
       table_name: bronze.raw_customers   # Delta concept
       table_path: abfss://...            # Delta concept

   test.sample:
     tags:
       provider: csv
       file_path: ./data/sample.csv       # CSV concept (no "table_name")
   ```

5. **Adapter Focuses on Interface Fallbacks:**
   - Adapter provides missing interface methods (merge fallback, etc.)
   - Adapter does NOT inject naming services
   - Providers handle their own naming internally

---

### Support Services - Summary

**INCORRECT Approaches (Options A-E):**
- All assume EntityNameMapper/EntityPathLocator work for all providers
- Force non-table storage (CSV, Parquet, InMemory) into table abstractions
- Create leaky abstractions that don't match storage models

**CORRECT Approach (Option F):**
- Each provider handles naming/path resolution internally
- Delta keeps EntityNameMapper/EntityPathLocator as Delta-specific services
- CSV/Parquet/InMemory use concepts appropriate to their storage model
- Configuration via tags expresses provider-specific needs
- Adapter focuses on interface fallbacks, not naming services

---

## 6. Entity Provider Adapter
        return entity.name.replace('.', '_')
```

### Pattern-Based Resolution Examples

**Layer-Specific Base Paths:**
```python
# Configuration
my_package:
  storage:
    bronze_base: /mnt/bronze
    silver_base: /mnt/silver
    gold_base: /mnt/gold

# EntityPathLocator uses pattern
def get_table_path(self, entity):
    layer = entity.name.split('.')[0]
    base = self.config.get(f"{layer}_base")
    return f"{base}/{entity.name.split('.')[1]}"

# Results:
# bronze.customers → /mnt/bronze/customers
# silver.customers → /mnt/silver/customers
# gold.customers → /mnt/gold/customers
```

**Environment-Specific Naming:**
```python
# EntityPathLocator adapts to environment
def get_table_path(self, entity):
    env = self.config.get("ENVIRONMENT")
    layer = entity.name.split('.')[0]

    if env == "dev":
        return f"/tmp/dev/{layer}/{entity.name.split('.')[1]}"
    elif env == "test":
        return f"/tmp/test/{layer}/{entity.name.split('.')[1]}"
    else:  # prod
        return f"/mnt/prod/{layer}/{entity.name.split('.')[1]}"
```

**Platform-Specific Naming:**
```python
# EntityNameMapper adapts to platform
def get_table_name(self, entity):
    platform = self.config.get("PLATFORM")

    if platform == "fabric":
        # Fabric uses lakehouse.schema.table
        return f"lakehouse.{entity.name.replace('.', '_')}"
    elif platform == "synapse":
        # Synapse uses schema_table
        return entity.name.replace('.', '_')
    elif platform == "databricks":
        # Databricks uses catalog.schema.table
        return f"catalog.{entity.name.replace('.', '_')}"
```

**Tag Override Support:**
```yaml
# Users can still override in config
my_package:
  data_entities:
    bronze.special_customers:
      tags:
        # Override the pattern for this specific entity
        table_path: /mnt/archive/special/customers
        table_name: legacy_customers_table
```

### Support Service Responsibilities

**EntityNameMapper** - Pattern-based table naming
- Injected into adapter
- Provides `get_table_name(entity)` with naming conventions
- Can adapt to platform, environment, layer
- Examples: Fabric style, Synapse style, custom patterns

**EntityPathLocator** - Pattern-based storage paths
- Injected into adapter
- Provides `get_table_path(entity)` with path conventions
- Can use layer-specific bases, environment-specific paths
- Examples: `/mnt/{layer}/{name}`, `/tmp/test/{name}`

**ConfigService** - Global configuration
- Injected into adapter
- Provides base paths, platform settings, environment info
- Used by naming/path services for pattern resolution

**Adapter Role** - Bridge between providers and services
- Wraps provider with support services
- Implements all interfaces with fallbacks
- Provides naming context to providers
- Centralizes DI for support services

**Result:** Providers stay pure, naming patterns preserved, support services valuable and centralized.

## Migration Path

### For Framework Developers

**Step 1: Implement provider interface (backward compatible)**
- Keep existing Delta-based code working
- Add provider abstraction in parallel
- No breaking changes initially

**Step 2: Register built-in providers**
```python
# In bootstrap.py or entity framework init
EntityProviders.register("delta", DeltaEntityProvider)
EntityProviders.register("csv", CsvEntityProvider)
EntityProviders.register("parquet", ParquetEntityProvider)
EntityProviders.register("in_memory", InMemoryEntityProvider)
```

**Step 3: Update EntityManager with fallback**
```python
def read_entity(self, entity_name: str) -> DataFrame:
    entity_entry = DataEntities.get_registered_entity(entity_name)

    # Check if provider configured (new path)
    if "provider" in entity_entry.tags:
        provider_name = entity_entry.tags["provider"]
        provider_class = EntityProviders.get_provider(provider_name)
        provider = provider_class(entity_entry)
        return provider.read()

    # Fall back to old Delta path (backward compatible)
    return self._legacy_read_delta(entity_name)
```

**Step 4: Deprecate legacy path**
- Add warnings when legacy path is used
- Update docs to show provider-based approach
- Provide migration examples

**Step 5: Remove legacy code**
- After deprecation period, remove fallback
- Provider becomes required (defaults to "delta")

### For Framework Users

**No immediate changes required:**
- Existing code continues to work
- Default provider is "delta" (current behavior)

**Opt-in to new features:**
```yaml
# Start using providers gradually
my_package:
  data_entities:
    # Just test entities initially
    test.*:
      tags:
        provider: in_memory
```

**Full migration:**
```yaml
# Explicitly configure all entity providers
my_package:
  data_entities:
    "*":
      tags:
        provider: delta  # Make current behavior explicit
```

## Open Questions

### 1. Provider Auto-Detection?

**Question:** Should framework auto-detect provider based on path/format?

**Options:**
- A. No auto-detection - always explicit configuration
- B. Auto-detect with explicit override
- C. Auto-detect with provider hints

**Recommendation:** **Option A** - explicit configuration is clearer and prevents surprises. Users should know what provider they're using.

### 2. Provider Composition?

**Question:** Should providers support composition (e.g., caching layer over Delta)?

**Options:**
- A. No composition - single provider per entity
- B. Provider chain (cache → delta → storage)
- C. Decorator pattern for provider enhancements

**Recommendation:** **Option A initially, B in future** - start simple with single provider per entity. If caching is needed, add `CachedDeltaProvider` as a separate provider. Later, consider provider composition if there's strong demand.

### 3. Schema Evolution Strategy?

**Question:** How should providers handle schema evolution differences?

**Options:**
- A. Provider-specific (Delta has merge schema, CSV doesn't)
- B. Framework-level schema evolution service
- C. Entity-level schema version tags

**Recommendation:** **Option A** - leverage provider-specific features. Delta and Parquet have `mergeSchema` option, CSV doesn't support evolution. Framework just passes schema validation flags. Providers expose their capabilities via `ProviderCapability.SCHEMA_EVOLUTION`.

### 4. Transaction Support?

**Question:** Should providers support transactions across multiple entities?

**Options:**
- A. No transactions - entity-level operations only
- B. Provider-specific transactions (Delta supports, CSV doesn't)
- C. Framework-level transaction coordinator

**Recommendation:** **Option B** - expose provider capabilities without forcing all providers to implement transactions. DeltaProvider can support transactions via `ProviderCapability.TRANSACTIONS`, others can skip or raise NotImplementedError. No framework-level coordinator initially.

### 5. Capability-Based Feature Gating?

**Question:** Should framework automatically disable features when provider doesn't support them?

**Options:**
- A. Auto-disable features silently (graceful degradation)
- B. Always error if feature not supported (explicit)
- C. Warn but continue (best effort)

**Recommendation:** **Option C (context-dependent):**
- **Critical features** (merge writes, incremental reads requested explicitly): Error with clear message
- **Optional features** (optimize, vacuum): Log warning and skip
- **Degradable features** (watermarking): Warn and fall back to alternative strategy

Example:
```python
# Critical - error if not supported
def read_entity_version(entity, version):
    if not provider.supports_capability(TIME_TRAVEL):
        raise ValueError("Time travel not supported")

# Optional - skip if not supported
def optimize_entity(entity):
    if provider.supports_capability(OPTIMIZE):
        provider.optimize()
    else:
        logger.debug("Optimize skipped")

# Degradable - use fallback
def read_incremental(entity):
    if provider.supports_capability(CHANGE_DATA_FEED):
        return provider.read_changes()
    elif provider.supports_capability(VERSIONING):
        return provider.read().filter("version > ...")
    else:
        logger.warning("Using timestamp-based incremental")
        return provider.read().filter("timestamp > ...")
```

### 6. Provider-Specific Configuration Validation?

**Question:** Should framework validate provider-specific config tags?

**Options:**
- A. No validation - providers handle their own config
- B. Providers declare config schema, framework validates
- C. Runtime validation only (fail on first use)

**Recommendation:** **Option A with documentation** - providers are responsible for their own configuration validation. Framework documents common patterns, but doesn't enforce. This keeps provider interface simple and allows flexibility. Providers throw clear errors on misconfiguration.

### 7. Extended Methods Discovery?

**Question:** How should framework discover provider-specific extended methods?

**Options:**
- A. Capability enum + hasattr() checks
- B. Provider declares method registry
- C. Explicit interfaces for each capability group

**Recommendation:** **Option A** - simple and flexible:
```python
if provider.supports_capability(TIME_TRAVEL):
    if hasattr(provider, 'read'):
        df = provider.read(version=10)  # Delta-specific signature
```

This allows providers to implement extended methods without strict interface contracts. Framework checks capabilities first, then uses hasattr() for method signature flexibility.

## Related Work

### Integration with Other Proposals

**Package Configuration Architecture:**
- Entity providers use tag-based configuration
- Wildcard patterns configure providers by layer
- Environment overrides enable dev/test/prod providers
- No changes to package config system needed

**DAG Execution Framework:**
- Executor reads entity via `EntityManager` (provider-agnostic)
- Streaming execution can check provider capabilities
- Entity lineage tracks provider metadata
- DAG optimizer could leverage provider features (e.g., Delta time travel)

## References

- `docs/proposals/package_config_architecture.md` - Tag-based configuration system
- `docs/proposals/dag_execution_implementation_plan.md` - Execution framework integration
- `docs/entity_providers.md` - Existing entity provider documentation
- `docs/data_entities.md` - Current entity architecture

---

**Next Steps:**
1. Review and approve this proposal
2. Prototype provider interface with one built-in provider
3. Validate approach with real use cases
4. Full implementation per phases above
