# Kindling Databricks DLT Extension

Databricks Delta Live Tables extension for the Kindling framework.

This extension provides advanced DLT functionality for Databricks platform that doesn't belong in the core platform implementation.

## Installation

```bash
pip install kindling-databricks-dlt
```

## Usage

```python
from kindling_databricks_dlt import DLTPipelineManager, StreamingKDARunner

# Use DLT pipeline manager
dlt_manager = DLTPipelineManager()

# Use streaming KDA runner  
streaming_runner = StreamingKDARunner()
```