# Logging and Tracing

The Kindling Framework provides comprehensive logging and distributed tracing capabilities to enable full observability of data pipelines. These features help with debugging, performance monitoring, and operational insights. Tracing semantics have been intentionally similar to the OpenTelemetry framework. Logging semantics have been designed to be semantically similar to log4j.

## Logging System

The logging system in Kindling is built on top of Spark's native logging capabilities, with extensions for structured logging and context tracking.

### Core Components

#### PythonLoggerProvider

Abstract interface for obtaining loggers.

```python
class PythonLoggerProvider(ABC):
    @abstractmethod
    def get_logger(self, name: str):
        """Get a logger instance with the specified name"""
        pass
```

#### SparkLoggerProvider

Default implementation of PythonLoggerProvider that creates SparkLogger instances.

```python
@GlobalInjector.singleton_autobind()
class SparkLoggerProvider(PythonLoggerProvider):
    def get_logger(self, name: str):
        return SparkLogger(name)
```

#### SparkLogger

A wrapper around Spark's Log4j logger with additional functionality:

- Log level filtering
- Structured log formatting
- Trace context inclusion
- Pattern-based logging

```python
class SparkLogger:
    def __init__(self, name: str, baselogger = None):
        # Initialize logger with name
        pass
        
    def debug(self, msg: str): 
        # Log at debug level
        pass
        
    def info(self, msg: str):
        # Log at info level
        pass
        
    def warn(self, msg: str):
        # Log at warning level
        pass
        
    def error(self, msg: str):
        # Log at error level
        pass
        
    def with_pattern(self, pattern: str):
        # Set custom logging pattern
        pass
```

### Usage Examples

#### Basic Logging

```python
# Get a logger through dependency injection
@inject
def __init__(self, lp: PythonLoggerProvider):
    self.logger = lp.get_logger("my_component")
    
# Log at different levels
self.logger.debug("Detailed diagnostic information")
self.logger.info("Normal operational information")
self.logger.warn("Warning condition")
self.logger.error("Error condition")
```

#### Custom Log Patterns

```python
# Create a logger with custom pattern
logger = lp.get_logger("custom_logger").with_pattern(
    "%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m"
)
```

## Distributed Tracing

The tracing system provides distributed tracing capabilities to track operations across multiple components, including capturing timing, errors, and context information.

### Core Components

#### CustomEventEmitter

Interface for emitting custom events to be collected by monitoring systems.

```python
class CustomEventEmitter(ABC):
    @abstractmethod
    def emit_custom_event(self,
        component: str,
        operation: str,
        details: dict,
        eventId: str,
        traceId: uuid ) -> None:
            pass
```

#### AzureEventEmitter

Default implementation of CustomEventEmitter that emits events to Azure's monitoring infrastructure.

```python
@GlobalInjector.singleton_autobind()
class AzureEventEmitter(CustomEventEmitter):
    # Implementation of event emission
    pass
```

#### SparkTraceProvider

Provider for creating and managing trace spans.

```python
class SparkTraceProvider(ABC):
    @abstractmethod
    def span(self, component: str, operation: str, details: Optional[Dict] = None, reraise: bool = False):
        """Create a new trace span"""
        pass
        
    @abstractmethod
    def get_current_trace_id(self) -> Optional[str]:
        """Get the current trace ID"""
        pass
```

#### SparkSpan

Represents a single operation within a trace.

```python
@dataclass
class SparkSpan:
    component: str
    operation: str
    details: Dict = field(default_factory=dict)
    parent_id: Optional[str] = None
    span_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    error: Optional[Exception] = None
```

### Usage Examples

#### Basic Tracing

```python
# Get the trace provider
trace_provider = GlobalInjector.get(SparkTraceProvider)

# Create a span around an operation
with trace_provider.span(
    component="DataTransformation",
    operation="ValidateCustomerData",
    details={"source": "CRM", "records": 1000}
) as span:
    # Your operation code here
    # Span automatically captures timing and errors
    pass
```

#### Nested Spans

```python
with trace_provider.span(component="Pipeline", operation="ProcessData") as outer_span:
    # Do some initial work
    
    with trace_provider.span(component="Validation", operation="ValidateSchema") as inner_span:
        # Validation logic
        pass
        
    with trace_provider.span(component="Transformation", operation="CleanData") as inner_span:
        # Transformation logic
        pass
```

#### MDC Context

The framework provides an MDC (Mapped Diagnostic Context) feature for enriching logs with trace context:

```python
with mdc_context(
    component="DataLoader",
    operation="LoadCustomers",
    trace_id=str(uuid.uuid4())
):
    # All logs within this context will include these attributes
    logger.info("Loading customer data")
```

## Integration with Entity Operations

The logging and tracing systems are fully integrated with entity operations. For example, when using the DeltaEntityProvider:

```python
# Automatically captures entity operations in traces
@trace_operation(component="EntityProvider", operation="MergeData")
def merge_to_entity(self, df, entity):
    # Operation is traced with timing and metadata
    pass
```

## Configuration

### Log Levels

Configure log levels through Spark configuration:

```python
spark.sparkContext.setLogLevel("INFO")  # Set Spark's log level

# Or through the framework's configuration system
config.set("LOG_LEVEL", "DEBUG")
```

### Event Destinations

The tracing system can be configured to emit events to various destinations:

- Azure Application Insights
- Custom event collectors
- Log files

Configure the destination in your environment setup:

```python
# Example configuration for Azure App Insights
config.set("TRACE_DESTINATION", "azure_app_insights")
config.set("APP_INSIGHTS_CONNECTION_STRING", "InstrumentationKey=...")
```

## Best Practices

1. **Consistent Component Names**: Use consistent naming conventions for components and operations.

2. **Appropriate Log Levels**: Use the appropriate log level for each message:
   - DEBUG: Detailed diagnostic information
   - INFO: Normal operational messages
   - WARN: Warning conditions
   - ERROR: Error conditions

3. **Structured Details**: Include structured details in traces for easier analysis:
   ```python
   details = {
       "entity_id": entity.entityid,
       "record_count": df.count(),
       "execution_id": execution_id
   }
   ```

4. **Trace Correlation**: Use trace IDs to correlate operations across components.

5. **Error Handling**: Ensure errors are properly captured in traces:
   ```python
   with trace_provider.span(component="Processing", operation="TransformData", reraise=True):
       # With reraise=True, exceptions will be captured in the trace and then re-thrown
   ```
