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
    def get_logger(self, name: str, session=None):
        """Get a logger instance with the specified name"""
        pass
```

#### SparkLoggerProvider

Default implementation of PythonLoggerProvider that creates SparkLogger instances.

```python
@GlobalInjector.singleton_autobind()
class SparkLoggerProvider(PythonLoggerProvider):
    def get_logger(self, name: str, session=None):
        return SparkLogger(name, config=self.config, session=session)
```

The optional `session` parameter accepts a SparkSession instance. When omitted, the provider uses the active Spark session from `get_or_create_spark_session()`.

#### SparkLogger

A wrapper around Spark's Log4j logger with additional functionality:

- Log level filtering
- Structured log formatting
- Trace context inclusion
- Pattern-based logging

```python
class SparkLogger:
    def __init__(self, name: str, baselogger=None, session=None, config=None):
        # Initialize logger with name
        pass

    def debug(self, msg: str, include_traceback: bool = False):
        # Log at debug level
        pass

    def info(self, msg: str, include_traceback: bool = False):
        # Log at info level
        pass

    def warn(self, msg: str, include_traceback: bool = False):
        # Log at warning level
        pass

    def error(self, msg: str, include_traceback: bool = False):
        # Log at error level
        pass

    def exception(self, msg: str):
        # Log at error level, always including the current traceback
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

#### Logging with an explicit SparkSession

```python
# Pass a specific session when the default session is not yet active
logger = lp.get_logger("my_component", session=spark)
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

The tracing system is built upon the existing Spark event infrastructure using a custom event emitter. Events are emitted based on the creation of Microsoft's custom event class that is part of the Fabric and Synapse spark runtime and passing the custom event to the underlying Spark emitter.

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

Default implementation of CustomEventEmitter based on Microsoft Synapse runtime components that emits to Spark's event ingestion.

```python
@GlobalInjector.singleton_autobind()
class AzureEventEmitter(CustomEventEmitter):
    # Implementation of event emission
    pass
```

#### SparkTraceProvider

Abstract provider for creating and managing trace spans.

```python
class SparkTraceProvider(ABC):
    @abstractmethod
    def span(
        self,
        operation: str = None,
        component: str = None,
        details: dict = None,
        reraise: bool = False,
    ):
        """Create a new trace span as a context manager"""
        pass

    @abstractmethod
    def start_span(
        self,
        operation: str,
        component: str,
        details: dict = None,
    ) -> "SparkSpan":
        """Start a span manually. Returns the span for later add_event/end_span calls."""
        pass

    @abstractmethod
    def add_event(
        self,
        span: "SparkSpan",
        name: str,
        attributes: dict = None,
    ) -> None:
        """Add a timestamped event marker to an active span."""
        pass

    @abstractmethod
    def end_span(
        self,
        span: "SparkSpan",
        error: Optional[str] = None,
    ) -> None:
        """End a manually started span. Optionally record an error."""
        pass
```

#### EventBasedSparkTrace

Default concrete implementation of `SparkTraceProvider`. Registered as a singleton via `GlobalInjector` and wired to `AzureEventEmitter`. Each call to `span()` emits `_START`, `_END`, and (on exception) `_ERROR` events through the underlying `CustomEventEmitter`. An internal activity counter is used as the span ID, and a UUID is generated as the trace ID for root-level spans and inherited by nested spans.

```python
@GlobalInjector.singleton_autobind()
class EventBasedSparkTrace(SparkTraceProvider):
    @inject
    def __init__(self, emitter: CustomEventEmitter):
        self.emitter = emitter
        self.current_span = None
        self.activity_counter = 1
```

Obtain an instance through the injector rather than constructing it directly:

```python
trace_provider = GlobalInjector.get(SparkTraceProvider)
```

#### SparkSpan

Represents a single operation within a trace.

```python
@dataclass
class SparkSpan:
    id: str
    component: str
    operation: str
    attributes: Dict[str, str]
    traceId: uuid
    reraise: bool
    start_time: datetime = None
    end_time: datetime = None
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
):
    # Your operation code here
    # Span automatically captures timing and errors
    pass
```

#### Nested Spans

```python
with trace_provider.span(component="Pipeline", operation="ProcessData"):
    # Do some initial work

    with trace_provider.span(component="Validation", operation="ValidateSchema"):
        # Validation logic
        pass

    with trace_provider.span(component="Transformation", operation="CleanData"):
        # Transformation logic
        pass
```

#### Manual Span Management

Use `start_span` / `add_event` / `end_span` when you need explicit control over span boundaries (e.g. spanning across callbacks or async boundaries).

```python
span = trace_provider.start_span(
    component="DataLoader",
    operation="LoadCustomers",
    details={"source": "CRM"},
)

# ... do work ...

trace_provider.add_event(span, "RecordsRead", attributes={"count": 1000})

# ... do more work ...

trace_provider.end_span(span)
# or, on failure:
# trace_provider.end_span(span, error=traceback.format_exc())
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

## Configuration

### Log Levels

Configure log levels through Spark configuration:

```python
spark.sparkContext.setLogLevel("INFO")  # Set Spark's log level

# Or through the framework's configuration system
config.set("LOG_LEVEL", "DEBUG")
```


## JVM Bridge Boundary

`spark._jvm` and `spark._jsc` (and `spark.sparkContext` on some runtimes) raise
`PySparkAttributeError` (`[JVM_ATTRIBUTE_NOT_SUPPORTED]`, an `AttributeError`
subclass) on Databricks UC shared/standard access mode clusters and Spark
Connect. Framework code must not assume a py4j bridge exists:

- Route filesystem and platform operations through `PlatformService` instead of
  the Hadoop JVM API.
- Telemetry code that optionally uses the JVM must guard access with
  `except (AttributeError, TypeError)` and latch the result so the probe runs
  once (see `mdc_context` in `kindling/spark_trace.py`).
- Runtime capability detection is a probe, not platform sniffing: bootstrap
  reads the `spark.jvm_bridge` runtime feature (set by
  `kindling.features.discover_runtime_features`) and rebinds the telemetry
  providers to the JVM-free implementations in `kindling/plain_telemetry.py`
  when the bridge is absent. Override with the static
  `kindling.features.spark.jvm_bridge` config key.

The rule is enforced by `tests/unit/test_architecture_jvm_boundary.py`: any
new `_jvm`/`_jsc` reference outside its allowlist fails CI. Add a file to the
allowlist only with a guard or a runtime guarantee, and say which in the
inline comment.

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
