# Watermarking System

The Watermarking System provides change tracking and incremental processing capabilities for Kindling Framework data pipelines. It allows data pipes to efficiently process only new or changed data since their last execution.

## Core Concepts

### Watermarks

Watermarks track the processing state between data sources and their consumers. Each watermark records:

- Source entity ID
- Reader/consumer ID
- Last processed version
- Last execution timestamp
- Execution ID

### Incremental Processing

Watermarking enables efficient incremental data processing by:

1. Tracking the last processed version for each source-consumer pair
2. Reading only changes since the last processed version
3. Updating watermarks after successful processing

## Key Components

### WatermarkEntityFinder

Interface that determines which entity stores watermarks for a given entity or layer.

```python
class WatermarkEntityFinder(ABC):
    @abstractmethod
    def get_watermark_entity_for_entity(self, context: str) -> Any:
        """Returns the entity that stores watermarks for the specified entity"""
        pass

    @abstractmethod
    def get_watermark_entity_for_layer(self, layer: str) -> Any:
        """Returns the entity that stores watermarks for the specified layer"""
        pass
```

The default autobind implementation is `SimpleWatermarkEntityFinder`, which stores all watermarks in a single `system.watermarks` Delta table.

### WatermarkService

Abstract base class defining the core watermarking operations. Implementations must also extend `SignalEmitter` and emit the signals listed in `WatermarkService.EMITS`.

```python
class WatermarkService(ABC):
    EMITS = [
        "watermark.before_get",
        "watermark.watermark_found",
        "watermark.watermark_missing",
        "watermark.before_save",
        "watermark.after_save",
        "watermark.save_failed",
        "watermark.before_read_changes",
        "watermark.after_read_changes",
        "watermark.no_new_data",
    ]

    @abstractmethod
    def get_watermark(self, source_entity_id: str, reader_id: str) -> Optional[int]:
        """Get the watermark for a specific source-reader pair"""
        pass

    @abstractmethod
    def save_watermark(self, source_entity_id: str, reader_id: str,
                      last_version_processed: int, last_execution_id: str) -> DataFrame:
        """Save a new watermark after processing"""
        pass

    @abstractmethod
    def read_current_entity_changes(self, entity, pipe):
        """Read changes for the entity since the last watermark for the pipe"""
        pass
```

### WatermarkManager

Default implementation of `WatermarkService`. Also extends `SignalEmitter` and is registered as a singleton via `@GlobalInjector.singleton_autobind()`.

```python
@GlobalInjector.singleton_autobind()
class WatermarkManager(WatermarkService, SignalEmitter):
    @inject
    def __init__(
        self,
        ep: EntityProvider,
        wef: WatermarkEntityFinder,
        lp: PythonLoggerProvider,
        signal_provider: Optional[SignalProvider] = None,
    ):
        ...
```

`read_current_entity_changes` accepts entity and pipe objects (not string IDs). It returns `None` when there is no new data, and a `DataFrame` of changes otherwise.

## Usage Examples

### Reading Incremental Changes

```python
# Get the watermark service
watermark_service = GlobalInjector.get(WatermarkService)

# Read only changes since the last time this pipe processed the entity
changes_df = watermark_service.read_current_entity_changes(entity, pipe)
```

### Saving Watermarks After Processing

```python
# Get current version of the source entity (pass the entity object, not a string)
current_version = entity_provider.get_entity_version(entity)

# After processing, save the watermark
watermark_service.save_watermark(
    source_entity_id="bronze.source_data",
    reader_id="silver.transform_pipe",
    last_version_processed=current_version,
    last_execution_id="execution-2025-06-20-001"
)
```

## Custom Watermark Entity Storage

You can customize where watermarks are stored by implementing your own `WatermarkEntityFinder`:

```python
@GlobalInjector.singleton_autobind()
class CustomWatermarkEntityFinder(WatermarkEntityFinder):
    def get_watermark_entity_for_entity(self, context: str):
        return "system.watermarks"

    def get_watermark_entity_for_layer(self, layer: str):
        return "system.watermarks"
```

## Best Practices

1. **Use watermarking for incremental processing**: Avoid full-table scans when only changes need to be processed.

2. **Ensure idempotent transformations**: Data pipes should be idempotent to allow for safe reprocessing.

3. **Save watermarks after successful processing**: Only update watermarks when processing has completed successfully.

4. **Handle schema evolution**: Consider how schema changes affect incremental processing.

5. **Monitor watermark drift**: Large gaps between current versions and watermarks may indicate processing backlogs.
