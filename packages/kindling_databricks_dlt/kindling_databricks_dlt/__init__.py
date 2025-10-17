"""
Delta Live Tables extension for Kindling Databricks

This extension provides advanced DLT functionality for Databricks platform
that doesn't belong in the core platform implementation.
"""

from .dlt_pipeline import DLTPipelineManager
from .streaming_kda import StreamingKDARunner

__version__ = "0.1.0"
__all__ = ["DLTPipelineManager", "StreamingKDARunner"]
