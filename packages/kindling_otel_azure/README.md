# Kindling Azure Monitor OpenTelemetry Integration

Azure Monitor OpenTelemetry integration for the Kindling Spark framework.

## Overview

This package provides Azure Monitor-backed implementations of Kindling's telemetry providers:
- `AzureMonitorLoggerProvider` - Sends logs to Azure Monitor/Application Insights
- `AzureMonitorTraceProvider` - Sends distributed traces to Azure Monitor

## Installation

```bash
pip install kindling-otel-azure
```

## Usage

### Configuration

Add the extension to your `settings.yaml` or `BOOTSTRAP_CONFIG`:

```yaml
kindling:
  # Extensions are automatically loaded (installed + imported)
  extensions:
    - kindling-otel-azure>=0.1.0

  telemetry:
    azure_monitor:
      connection_string: "InstrumentationKey=your-key;IngestionEndpoint=https://..."
      # Optional settings
      enable_logging: true
      enable_tracing: true
      sampling_rate: 1.0  # 1.0 = 100% sampling
```

Or in bootstrap config:
```python
BOOTSTRAP_CONFIG = {
    'extensions': ['kindling-otel-azure>=0.1.0'],
    'kindling': {
        'telemetry': {
            'azure_monitor': {
                'connection_string': 'InstrumentationKey=...',
                'enable_logging': True,
                'enable_tracing': True,
            }
        }
    }
}
```

### Using with Dependency Injection

The providers automatically register with Kindling's DI container:

```python
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_trace import SparkTraceProvider
from kindling.injection import GlobalInjector

# Import to register Azure Monitor providers
import kindling_otel_azure

# Get logger (automatically uses Azure Monitor if configured)
logger_provider = GlobalInjector.get(PythonLoggerProvider)
logger = logger_provider.get_logger("my_app")

# Get tracer (automatically uses Azure Monitor if configured)
trace_provider = GlobalInjector.get(SparkTraceProvider)

# Use tracing
with trace_provider.span(component="data_pipeline", operation="process"):
    logger.info("Processing data...")
    # Your code here
```

### Manual Setup

You can also manually configure the providers:

```python
from kindling_otel_azure import AzureMonitorLoggerProvider, AzureMonitorTraceProvider
from kindling.spark_config import ConfigService

config = ConfigService()
config.load_from_file("settings.yaml")

# Create providers
logger_provider = AzureMonitorLoggerProvider(config)
trace_provider = AzureMonitorTraceProvider(config)

# Use them
logger = logger_provider.get_logger("my_app")
logger.info("Hello from Azure Monitor!")
```

## Features

- **Automatic instrumentation** - Logs and traces flow to Azure Monitor
- **Distributed tracing** - Correlate operations across services
- **Custom metrics** - Track pipeline execution times, row counts, etc.
- **Application Insights integration** - Query with KQL, set up alerts
- **Fallback behavior** - Gracefully falls back if Azure Monitor unavailable

## Requirements

- Python 3.10+
- Kindling framework
- Azure subscription with Application Insights resource
- Application Insights connection string

## License

MIT
