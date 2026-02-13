# Blue/Green Table Migrations for Kindling

**Status**: Draft
**Author**: System Analysis
**Created**: 2026-02-13
**Updated**: 2026-02-13

## Executive Summary

This proposal introduces automatic/dynamic table migration capabilities to the Kindling framework using a blue/green deployment pattern. The solution leverages Kindling's existing entity registry and provider architecture to enable zero-downtime schema changes, data transformations, and table refactoring with atomic cutover and rollback capabilities.

## Problem Statement

### Current Limitations

Modern data platforms require frequent schema evolution and table restructuring to adapt to changing business requirements. Common scenarios include:

1. **Breaking Schema Changes**: Column renames, type changes, or deletions that aren't supported by additive schema evolution
2. **Data Quality Fixes**: Correcting historical data or applying transformations to existing records
3. **Performance Optimization**: Repartitioning or reindexing tables based on usage patterns
4. **Compliance Requirements**: Applying data masking or encryption retroactively
5. **Architecture Refactoring**: Splitting or merging tables as domain models evolve

### Current State in Kindling

Kindling currently supports:
- ✅ **Additive schema evolution** via Delta Lake's `mergeSchema` option
- ✅ **Tag-based path configuration** for flexible table location management
- ✅ **Watermark tracking** for incremental processing
- ✅ **Provider abstraction** enabling multiple storage backends

However, Kindling lacks:
- ❌ **Orchestrated migration workflows** for breaking changes
- ❌ **Zero-downtime table replacement** capabilities
- ❌ **Automated dual-write coordination** during migrations
- ❌ **Migration state tracking** and observability
- ❌ **Rollback mechanisms** for failed migrations

### Impact

Without built-in migration support, teams must:
- Write custom migration scripts for each table change
- Coordinate manual cutover processes across teams
- Risk data loss or downtime during migrations
- Maintain separate tooling outside the Kindling framework
- Struggle with watermark and checkpoint continuity

## Proposed Solution

### Blue/Green Migration Pattern

Implement a **registry-based blue/green deployment pattern** that:

1. Maintains two table slots per entity (blue and green)
2. Uses entity tags to track the active slot
3. Provides atomic cutover via registry pointer updates
4. Supports automated backfill and dual-write orchestration
5. Enables instant rollback to previous slot

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│  (Data Pipes, Streaming Queries, Batch Jobs)               │
└────────────────┬────────────────────────────────────────────┘
                 │ read_entity() / write_to_entity()
                 ↓
┌─────────────────────────────────────────────────────────────┐
│              EntityProviderRegistry                         │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  MigrationAwareEntityProvider (wrapper)              │  │
│  │  - Resolves active slot from tags                   │  │
│  │  - Routes reads to active slot                       │  │
│  │  - Coordinates dual writes during migration         │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ↓
┌─────────────────────────────────────────────────────────────┐
│         DeltaEntityProvider (or other provider)             │
│  Reads/writes to resolved table path                        │
└────────────────┬────────────────────────────────────────────┘
                 │
        ┌────────┴────────┐
        ↓                 ↓
┌──────────────┐  ┌──────────────┐
│  Blue Table  │  │ Green Table  │
│  (active)    │  │ (inactive)   │
└──────────────┘  └──────────────┘
        ↑                 │
        └─────────────────┘
           Cutover = Tag Update
```

## Detailed Design

### 1. Entity Tag Schema

Extend entity tags to support migration configuration:

```python
@DataEntities.entity(
    entityid="sales.transactions",
    name="transactions",
    partition_columns=["date"],
    merge_columns=["transaction_id"],
    tags={
        # Provider configuration
        "provider_type": "delta",
        "provider.path": "Tables/sales/transactions",  # Logical path

        # Migration configuration
        "migration.enabled": "true",
        "migration.blue_path": "Tables/sales/transactions_blue",
        "migration.green_path": "Tables/sales/transactions_green",
        "migration.active_slot": "blue",  # or "green"
        "migration.schema_version": "2",
        "migration.state": "active",  # active | migrating | rolling_back
        "migration.last_cutover": "2026-02-13T10:30:00Z",

        # Optional: Migration policies
        "migration.auto_cleanup": "true",
        "migration.retention_days": "7",
        "migration.dual_write_window_hours": "24",
    },
    schema=transactions_schema
)
```

### 2. Core Components

#### A. MigrationAwareEntityProvider

Transparent wrapper that routes operations based on migration state:

```python
from kindling.entity_provider import (
    BaseEntityProvider,
    WritableEntityProvider,
    StreamableEntityProvider,
    StreamWritableEntityProvider
)
from kindling.data_entities import EntityMetadata
from pyspark.sql import DataFrame
from typing import Optional

class MigrationAwareEntityProvider(
    BaseEntityProvider,
    WritableEntityProvider,
    StreamableEntityProvider,
    StreamWritableEntityProvider
):
    """
    Entity provider wrapper that supports blue/green migrations.

    Responsibilities:
    - Resolve active slot from entity tags
    - Route reads to active slot
    - Coordinate dual writes during migration
    - Delegate to underlying provider for actual I/O

    Signal emissions:
    - migration.read_routed: When read is routed to specific slot
    - migration.dual_write_started: When dual write begins
    - migration.dual_write_completed: When dual write completes
    - migration.slot_resolved: When active slot is determined
    """

    def __init__(
        self,
        underlying_provider: BaseEntityProvider,
        migration_manager: 'TableMigrationManager'
    ):
        self.provider = underlying_provider
        self.migration_manager = migration_manager

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """Read from active slot."""
        if self._is_migration_enabled(entity_metadata):
            active_slot = self._get_active_slot(entity_metadata)
            entity_metadata = self._override_path_for_slot(
                entity_metadata,
                active_slot
            )
            # Emit signal
            self.emit(
                "migration.read_routed",
                entity_id=entity_metadata.entityid,
                slot=active_slot
            )

        return self.provider.read_entity(entity_metadata)

    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata):
        """Write to target slot(s) based on migration state."""
        if not self._is_migration_enabled(entity_metadata):
            return self.provider.write_to_entity(df, entity_metadata)

        migration_state = entity_metadata.tags.get("migration.state", "active")

        if migration_state == "migrating":
            # Dual write mode: write to both slots
            return self._dual_write(df, entity_metadata)
        else:
            # Normal mode: write to active slot only
            active_slot = self._get_active_slot(entity_metadata)
            entity_metadata = self._override_path_for_slot(
                entity_metadata,
                active_slot
            )
            return self.provider.write_to_entity(df, entity_metadata)

    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata):
        """Append to target slot(s) based on migration state."""
        if not self._is_migration_enabled(entity_metadata):
            return self.provider.append_to_entity(df, entity_metadata)

        migration_state = entity_metadata.tags.get("migration.state", "active")

        if migration_state == "migrating":
            # Dual append mode
            return self._dual_append(df, entity_metadata)
        else:
            active_slot = self._get_active_slot(entity_metadata)
            entity_metadata = self._override_path_for_slot(
                entity_metadata,
                active_slot
            )
            return self.provider.append_to_entity(df, entity_metadata)

    def _dual_write(self, df: DataFrame, entity_metadata: EntityMetadata):
        """Write to both blue and green slots."""
        self.emit("migration.dual_write_started", entity_id=entity_metadata.entityid)

        try:
            # Write to blue
            blue_entity = self._override_path_for_slot(entity_metadata, "blue")
            self.provider.write_to_entity(df, blue_entity)

            # Write to green
            green_entity = self._override_path_for_slot(entity_metadata, "green")
            self.provider.write_to_entity(df, green_entity)

            self.emit("migration.dual_write_completed", entity_id=entity_metadata.entityid)
        except Exception as e:
            self.emit(
                "migration.dual_write_failed",
                entity_id=entity_metadata.entityid,
                error=str(e)
            )
            raise

    def _dual_append(self, df: DataFrame, entity_metadata: EntityMetadata):
        """Append to both blue and green slots."""
        # Similar to _dual_write but using append
        pass

    def _is_migration_enabled(self, entity_metadata: EntityMetadata) -> bool:
        """Check if migration is enabled for this entity."""
        return entity_metadata.tags.get("migration.enabled", "").lower() == "true"

    def _get_active_slot(self, entity_metadata: EntityMetadata) -> str:
        """Get active slot (blue or green) from tags."""
        return entity_metadata.tags.get("migration.active_slot", "blue")

    def _override_path_for_slot(
        self,
        entity_metadata: EntityMetadata,
        slot: str
    ) -> EntityMetadata:
        """Create new EntityMetadata with path overridden for specific slot."""
        from dataclasses import replace

        slot_path = entity_metadata.tags.get(f"migration.{slot}_path")
        if not slot_path:
            raise ValueError(
                f"No migration.{slot}_path configured for entity "
                f"{entity_metadata.entityid}"
            )

        # Create new tags dict with updated path
        new_tags = {**entity_metadata.tags, "provider.path": slot_path}

        return replace(entity_metadata, tags=new_tags)

    # Delegate streaming methods if underlying provider supports them
    def read_entity_as_stream(
        self,
        entity_metadata: EntityMetadata,
        format: Optional[str] = None,
        options: Optional[dict] = None
    ) -> DataFrame:
        """Read stream from active slot."""
        if not isinstance(self.provider, StreamableEntityProvider):
            raise TypeError("Underlying provider does not support streaming reads")

        if self._is_migration_enabled(entity_metadata):
            active_slot = self._get_active_slot(entity_metadata)
            entity_metadata = self._override_path_for_slot(
                entity_metadata,
                active_slot
            )

        return self.provider.read_entity_as_stream(
            entity_metadata,
            format,
            options
        )

    def append_as_stream(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        checkpoint_location: str,
        format: Optional[str] = None,
        options: Optional[dict] = None
    ):
        """Stream append to active slot (dual streaming not supported)."""
        if not isinstance(self.provider, StreamWritableEntityProvider):
            raise TypeError("Underlying provider does not support streaming writes")

        # For streaming, dual write is complex - require manual migration
        # or use checkpoint migration strategy
        if self._is_migration_enabled(entity_metadata):
            active_slot = self._get_active_slot(entity_metadata)
            entity_metadata = self._override_path_for_slot(
                entity_metadata,
                active_slot
            )

        return self.provider.append_as_stream(
            df,
            entity_metadata,
            checkpoint_location,
            format,
            options
        )
```

#### B. TableMigrationManager

Core service for orchestrating migrations:

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Callable
from enum import Enum

class MigrationState(Enum):
    """Migration lifecycle states."""
    ACTIVE = "active"              # Normal operation
    PREPARING = "preparing"        # Creating target slot
    BACKFILLING = "backfilling"    # Populating target slot
    MIGRATING = "migrating"        # Dual write mode
    CUTOVER = "cutover"            # Switching active slot
    ROLLING_BACK = "rolling_back"  # Reverting to previous slot
    CLEANUP = "cleanup"            # Removing old slot

@dataclass
class MigrationContext:
    """Context for an active migration."""
    entity_id: str
    source_slot: str
    target_slot: str
    state: MigrationState
    started_at: datetime
    schema_version: Optional[str] = None
    backfill_progress: float = 0.0
    dual_write_started_at: Optional[datetime] = None
    validation_passed: bool = False

    def get_source_path(self, entity_metadata) -> str:
        """Get path for source slot."""
        return entity_metadata.tags.get(f"migration.{self.source_slot}_path")

    def get_target_path(self, entity_metadata) -> str:
        """Get path for target slot."""
        return entity_metadata.tags.get(f"migration.{self.target_slot}_path")


class TableMigrationManager(ABC):
    """
    Abstract base for table migration orchestration.

    Implementations MUST emit these signals:
        - migration.initiated: When migration starts
        - migration.prepared: Target slot ready
        - migration.backfill_started: Backfill begins
        - migration.backfill_progress: Progress updates
        - migration.backfill_completed: Backfill done
        - migration.dual_write_enabled: Dual write activated
        - migration.before_cutover: Before active slot switch
        - migration.after_cutover: After active slot switch
        - migration.cutover_failed: Cutover error
        - migration.rollback_started: Rollback initiated
        - migration.rollback_completed: Rollback done
        - migration.cleanup_started: Cleanup begins
        - migration.cleanup_completed: Cleanup done
    """

    EMITS = [
        "migration.initiated",
        "migration.prepared",
        "migration.backfill_started",
        "migration.backfill_progress",
        "migration.backfill_completed",
        "migration.dual_write_enabled",
        "migration.before_cutover",
        "migration.after_cutover",
        "migration.cutover_failed",
        "migration.rollback_started",
        "migration.rollback_completed",
        "migration.cleanup_started",
        "migration.cleanup_completed",
    ]

    @abstractmethod
    def initiate_migration(
        self,
        entity_id: str,
        new_schema: Optional[Any] = None,
        transformation: Optional[Callable] = None
    ) -> MigrationContext:
        """
        Initiate migration to inactive slot.

        Args:
            entity_id: Entity to migrate
            new_schema: Optional new schema for target slot
            transformation: Optional transformation function for backfill

        Returns:
            MigrationContext for tracking progress
        """
        pass

    @abstractmethod
    def backfill(
        self,
        context: MigrationContext,
        transformation: Optional[Callable] = None
    ) -> bool:
        """
        Backfill target slot with historical data.

        Args:
            context: Migration context
            transformation: Optional transformation to apply during backfill

        Returns:
            True if backfill succeeded
        """
        pass

    @abstractmethod
    def enable_dual_write(self, context: MigrationContext) -> bool:
        """
        Enable dual-write mode for incremental sync.

        Updates entity tags to set migration.state = "migrating"

        Returns:
            True if dual write enabled
        """
        pass

    @abstractmethod
    def validate_target_slot(self, context: MigrationContext) -> bool:
        """
        Validate target slot before cutover.

        Checks:
        - Row counts match
        - Schema is valid
        - Data quality checks pass

        Returns:
            True if validation passed
        """
        pass

    @abstractmethod
    def cutover(self, context: MigrationContext) -> bool:
        """
        Atomically switch active slot.

        Updates:
        - entity.tags["migration.active_slot"]
        - entity.tags["migration.state"] = "active"
        - entity.tags["migration.last_cutover"] = timestamp
        - Watermark associations
        - Configuration cache invalidation

        Returns:
            True if cutover succeeded
        """
        pass

    @abstractmethod
    def rollback(self, context: MigrationContext) -> bool:
        """
        Rollback to previous active slot.

        Reverts:
        - entity.tags["migration.active_slot"]
        - entity.tags["migration.state"] = "active"

        Returns:
            True if rollback succeeded
        """
        pass

    @abstractmethod
    def cleanup(
        self,
        context: MigrationContext,
        retention_days: int = 7
    ) -> bool:
        """
        Clean up old slot after successful migration.

        Only executes after retention period expires.

        Args:
            context: Migration context
            retention_days: Days to retain old slot

        Returns:
            True if cleanup succeeded
        """
        pass

    @abstractmethod
    def get_migration_status(self, entity_id: str) -> Optional[MigrationContext]:
        """Get current migration status for entity."""
        pass
```

### 3. Watermark Migration Strategy

Watermarks must be migrated to maintain incremental processing continuity:

```python
class WatermarkMigrationSupport:
    """Helper for migrating watermarks during table migrations."""

    def migrate_watermarks(
        self,
        source_entity_id: str,
        target_entity_id: str,
        reader_id: Optional[str] = None
    ):
        """
        Copy watermarks from source to target entity.

        If reader_id is provided, migrates only that reader's watermark.
        Otherwise, migrates all watermarks for the entity.
        """
        watermark_entity = self.watermark_entity_finder.get_watermark_entity_for_entity(
            source_entity_id
        )

        # Read existing watermarks
        watermarks_df = self.entity_provider.read_entity(watermark_entity).filter(
            col("source_entity_id") == source_entity_id
        )

        if reader_id:
            watermarks_df = watermarks_df.filter(col("reader_id") == reader_id)

        # Update entity ID to target
        migrated_df = watermarks_df.withColumn(
            "source_entity_id",
            lit(target_entity_id)
        ).withColumn(
            "watermark_id",
            concat(lit(target_entity_id), lit("_"), col("reader_id"))
        )

        # Write migrated watermarks
        self.entity_provider.merge_to_entity(migrated_df, watermark_entity)

    def create_temporary_watermark(
        self,
        entity_id: str,
        reader_id: str,
        version: int
    ):
        """
        Create temporary watermark for target slot during migration.

        Allows incremental processing to continue on new slot
        while old slot is still active.
        """
        pass
```

### 4. Streaming Query Checkpoint Migration

Special handling for streaming queries:

```python
class StreamingCheckpointMigration:
    """Handles checkpoint migration for streaming queries."""

    def migrate_checkpoint(
        self,
        source_checkpoint: str,
        target_checkpoint: str,
        new_table_path: str
    ):
        """
        Migrate streaming checkpoint to new location.

        Strategy:
        1. Stop streaming query
        2. Copy checkpoint directory
        3. Update offsets file with new table path
        4. Restart query with new checkpoint
        """
        pass

    def create_compatible_checkpoint(
        self,
        existing_checkpoint: str,
        new_checkpoint: str,
        target_slot_path: str
    ):
        """
        Create new checkpoint compatible with target slot.

        Used when target slot already has data from backfill.
        Sets appropriate starting offsets.
        """
        pass
```

### 5. Registry Integration

Update EntityProviderRegistry to wrap providers with migration support:

```python
@GlobalInjector.singleton_autobind()
class EntityProviderRegistry:
    # ... existing code ...

    def get_provider_for_entity(
        self,
        entity_metadata: EntityMetadata
    ) -> BaseEntityProvider:
        """Get provider with migration support if enabled."""
        provider_type = entity_metadata.tags.get("provider_type", "delta")
        base_provider = self.get_provider(provider_type)

        # Wrap with migration support if enabled
        if entity_metadata.tags.get("migration.enabled", "").lower() == "true":
            if not hasattr(self, '_migration_manager'):
                self._migration_manager = GlobalInjector.get(TableMigrationManager)

            return MigrationAwareEntityProvider(
                base_provider,
                self._migration_manager
            )

        return base_provider
```

## Implementation Phases

### Phase 1: Foundation (2-3 weeks)

**Goal**: Enable manual blue/green migrations via configuration

**Deliverables**:
- Tag schema definition and validation
- `MigrationAwareEntityProvider` implementation
- Basic path resolution for blue/green slots
- Configuration-based slot switching
- Documentation and examples

**Success Criteria**:
- Entities can be configured with blue/green paths
- Reads route to active slot correctly
- Manual cutover via config update works
- Zero code changes needed in consuming pipes

### Phase 2: Orchestration (3-4 weeks)

**Goal**: Automated migration workflows

**Deliverables**:
- `TableMigrationManager` service
- `MigrationContext` state tracking
- Backfill orchestration
- Dual-write coordination
- Cutover and rollback automation
- Signal emissions for observability

**Success Criteria**:
- Migrations can be initiated via API
- Backfill runs automatically
- Dual writes work correctly
- Cutover is atomic
- Rollback works instantly
- All operations emit signals

### Phase 3: Advanced Features (2-3 weeks)

**Goal**: Production-ready migration capabilities

**Deliverables**:
- Watermark migration
- Streaming checkpoint handling
- Validation gates before cutover
- Automated cleanup after retention period
- Migration monitoring dashboard
- Recovery from partial failures

**Success Criteria**:
- Watermarks carry over correctly
- Streaming queries migrate successfully
- Failed migrations rollback cleanly
- Old slots cleaned up automatically
- Migration metrics available

### Phase 4: Automation (2-3 weeks)

**Goal**: Intelligent migration triggers

**Deliverables**:
- Schema change detection
- Auto-triggered migrations for safe changes
- Policy-based migration decisions
- Cost/benefit analysis
- Integration with CI/CD

**Success Criteria**:
- Schema updates trigger migrations automatically
- Policies prevent unsafe auto-migrations
- Developers notified of migration status
- Migrations tracked in version control

## Migration Workflow Examples

### Example 1: Breaking Schema Change

```python
from kindling.migration import TableMigrationManager
from kindling.data_entities import DataEntityManager
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# 1. Define entity with migration support
@DataEntities.entity(
    entityid="sales.transactions",
    name="transactions",
    partition_columns=["date"],
    merge_columns=["transaction_id"],
    tags={
        "provider_type": "delta",
        "migration.enabled": "true",
        "migration.blue_path": "Tables/sales/transactions_blue",
        "migration.green_path": "Tables/sales/transactions_green",
        "migration.active_slot": "blue"
    },
    schema=old_schema
)

# 2. Initiate migration with new schema
migration_mgr = get_kindling_service(TableMigrationManager)

new_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("transaction_date", TimestampType(), False),  # Renamed from 'date'
    StructField("amount_cents", IntegerType(), False),        # Changed from dollars
])

def transform_to_new_schema(df):
    """Transform function for backfill."""
    return df \
        .withColumnRenamed("date", "transaction_date") \
        .withColumn("amount_cents", col("amount_dollars") * 100) \
        .drop("amount_dollars")

context = migration_mgr.initiate_migration(
    "sales.transactions",
    new_schema=new_schema,
    transformation=transform_to_new_schema
)

# 3. Backfill target slot
success = migration_mgr.backfill(context, transformation=transform_to_new_schema)
assert success, "Backfill failed"

# 4. Enable dual-write for incremental sync
migration_mgr.enable_dual_write(context)

# Wait for incremental data to sync (or verify manually)
time.sleep(300)  # 5 minutes

# 5. Validate target slot
validation_passed = migration_mgr.validate_target_slot(context)
if not validation_passed:
    migration_mgr.rollback(context)
    raise Exception("Validation failed, rolled back")

# 6. Cutover (atomic)
success = migration_mgr.cutover(context)
assert success, "Cutover failed"

# Now all reads go to green slot with new schema
# Blue slot retained for rollback

# 7. Cleanup old slot after 7 days
migration_mgr.cleanup(context, retention_days=7)
```

### Example 2: Data Quality Fix

```python
# Scenario: Fix corrupted records in historical data

def fix_data_quality(df):
    """Apply data quality fixes during backfill."""
    return df \
        .withColumn(
            "email",
            when(col("email").rlike("^[^@]+@[^@]+$"), col("email"))
            .otherwise(lit(None))
        ) \
        .withColumn(
            "phone",
            regexp_replace(col("phone"), r"[^\d]", "")
        )

# Initiate migration with same schema but data fixes
context = migration_mgr.initiate_migration(
    "customers.profiles",
    new_schema=None,  # Same schema
    transformation=fix_data_quality
)

migration_mgr.backfill(context, transformation=fix_data_quality)
migration_mgr.enable_dual_write(context)
migration_mgr.validate_target_slot(context)
migration_mgr.cutover(context)
```

### Example 3: Simple Manual Migration

For teams not ready for full automation:

```python
# 1. Current config
entity_tags = {
    "provider.path": "Tables/sales/transactions_blue"
}

# 2. Manually create and populate green table
# (custom script or notebook)

spark.read.format("delta").load("Tables/sales/transactions_blue") \
    .transform(apply_changes) \
    .write.format("delta").save("Tables/sales/transactions_green")

# 3. Update entity configuration to cutover
entity_tags = {
    "provider.path": "Tables/sales/transactions_green"
}

# All readers instantly use green via config refresh
# No code changes needed in consuming applications
```

## Trade-offs and Alternatives

### Blue/Green vs. In-Place Evolution

| Aspect | Blue/Green | In-Place Evolution |
|--------|-----------|-------------------|
| **Downtime** | Zero | Zero |
| **Rollback** | Instant | Complex |
| **Storage** | 2x during migration | 1x |
| **Schema Changes** | All types | Additive only |
| **Complexity** | Higher orchestration | Simpler |
| **Data Quality Fixes** | Yes | No |
| **Testing** | Can validate before cutover | Limited |

### Alternative Approaches Considered

#### 1. Shadow Tables

**Description**: Write to shadow table alongside main table, then swap

**Pros**:
- Similar to blue/green
- Doesn't require provider changes

**Cons**:
- Manual coordination required
- No framework integration
- Watermark issues

#### 2. View-Based Indirection

**Description**: Use Delta Lake views to abstract table location

**Pros**:
- Native Delta feature
- Simple cutover

**Cons**:
- Limited to catalog-enabled platforms
- Doesn't work with path-based access (Fabric)
- No dual-write support

#### 3. Time Travel + Copy-on-Write

**Description**: Use Delta Lake time travel for rollback

**Pros**:
- Native Delta feature
- No duplicate storage

**Cons**:
- Can't change schema structure
- Performance impact
- Retention limits

**Decision**: Blue/green with registry provides best balance of flexibility, safety, and framework integration.

## Success Criteria

### Functional Requirements

- [ ] Entities can be configured with blue/green paths via tags
- [ ] Reads automatically route to active slot
- [ ] Writes respect migration state (single or dual)
- [ ] Cutover is atomic and requires no code changes
- [ ] Rollback works instantly
- [ ] Watermarks migrate correctly
- [ ] Streaming queries can migrate
- [ ] Failed migrations rollback cleanly

### Non-Functional Requirements

- [ ] Migration orchestration has <5% overhead vs manual
- [ ] Cutover completes in <1 second
- [ ] Rollback completes in <1 second
- [ ] Dual writes have <10% latency impact
- [ ] All operations emit observability signals
- [ ] Documentation covers common scenarios
- [ ] Migration status is queryable

### Production Readiness

- [ ] Unit tests for all components (>80% coverage)
- [ ] Integration tests for migration workflows
- [ ] System tests on all platforms (Fabric, Synapse, Databricks)
- [ ] Failure injection tests (rollback scenarios)
- [ ] Performance benchmarks
- [ ] Migration monitoring dashboard
- [ ] Runbooks for common issues

## Open Questions

1. **Streaming Checkpoint Strategy**: Should we support live checkpoint migration or require query restart?
2. **Concurrent Migrations**: Should we allow multiple entities to migrate simultaneously or serialize?
3. **Cross-Entity Dependencies**: How do we handle migrations when entities have foreign key relationships?
4. **Partial Cutover**: Should we support gradual cutover (percentage of traffic)?
5. **Schema Validation**: What level of schema compatibility checking should we enforce?
6. **Storage Costs**: Should we auto-cleanup immediately after successful migration or always retain for X days?
7. **Audit Trail**: Should migration history be tracked in a dedicated table or via signals only?

## References

- [Entity Provider Registry Documentation](../entity_providers.md)
- [Data Entities Documentation](../data_entities.md)
- [Watermarking Documentation](../watermarking.md)
- [Config-Based Entity Providers Proposal](config_based_entity_providers.md)
- [Delta Lake Schema Evolution](https://docs.delta.io/latest/delta-update.html#automatic-schema-evolution)
- [Blue/Green Deployment Pattern](https://martinfowler.com/bliki/BlueGreenDeployment.html)

## Appendix

### A. Migration State Machine

```
                    ┌─────────┐
                    │ ACTIVE  │
                    └────┬────┘
                         │ initiate_migration()
                         ↓
                   ┌──────────┐
                   │PREPARING │
                   └────┬─────┘
                        │ prepare_target_slot()
                        ↓
                  ┌────────────┐
                  │BACKFILLING │
                  └─────┬──────┘
                        │ backfill()
                        ↓
                  ┌───────────┐
                  │ MIGRATING │ ← dual_write mode
                  └─────┬─────┘
                        │ validate()
                        ↓
                   ┌─────────┐
              ┌────│ CUTOVER │────┐
              │    └─────────┘    │
              │ cutover()          │ error
              ↓                    ↓
         ┌────────┐         ┌──────────────┐
         │ ACTIVE │         │ROLLING_BACK  │
         └───┬────┘         └──────┬───────┘
             │                     │ rollback()
             │ cleanup()           ↓
             ↓                ┌─────────┐
        ┌─────────┐           │ ACTIVE  │
        │ CLEANUP │           └─────────┘
        └─────────┘
             │ cleanup_old_slot()
             ↓
        ┌─────────┐
        │ ACTIVE  │
        └─────────┘
```

### B. Signal Emissions Reference

All migration operations emit signals for observability:

```python
# Migration lifecycle
"migration.initiated" → {entity_id, source_slot, target_slot, schema_version}
"migration.prepared" → {entity_id, target_slot, table_created}
"migration.backfill_started" → {entity_id, source_slot, target_slot, row_count}
"migration.backfill_progress" → {entity_id, progress_pct, rows_processed}
"migration.backfill_completed" → {entity_id, duration_seconds, rows_migrated}
"migration.dual_write_enabled" → {entity_id, timestamp}
"migration.before_cutover" → {entity_id, source_slot, target_slot}
"migration.after_cutover" → {entity_id, new_active_slot, duration_ms}
"migration.cutover_failed" → {entity_id, error, error_type}
"migration.rollback_started" → {entity_id, reason}
"migration.rollback_completed" → {entity_id, restored_slot}
"migration.cleanup_started" → {entity_id, removing_slot}
"migration.cleanup_completed" → {entity_id, bytes_freed}

# Per-operation routing
"migration.read_routed" → {entity_id, slot, table_path}
"migration.dual_write_started" → {entity_id, target_slots}
"migration.dual_write_completed" → {entity_id, duration_ms}
"migration.dual_write_failed" → {entity_id, failed_slot, error}
```

### C. Configuration Examples

**Minimal Configuration** (manual migration):
```yaml
entities:
  sales.transactions:
    tags:
      provider.path: Tables/sales/transactions_blue  # Switch to _green to cutover
```

**Full Blue/Green Configuration**:
```yaml
entities:
  sales.transactions:
    tags:
      # Migration control
      migration.enabled: true
      migration.blue_path: Tables/sales/transactions_blue
      migration.green_path: Tables/sales/transactions_green
      migration.active_slot: blue
      migration.state: active

      # Policies
      migration.auto_cleanup: true
      migration.retention_days: 7
      migration.dual_write_window_hours: 24

      # Validation rules
      migration.validation.row_count_threshold: 0.99  # 99% match required
      migration.validation.schema_strict: true
```

**Streaming Entity Configuration**:
```yaml
entities:
  events.clickstream:
    tags:
      migration.enabled: true
      migration.blue_path: Tables/events/clickstream_blue
      migration.green_path: Tables/events/clickstream_green
      migration.active_slot: blue

      # Streaming-specific
      migration.checkpoint_strategy: copy_and_update  # or restart_from_latest
      migration.stream_cutover_mode: stop_and_start   # or live_switchover
```
