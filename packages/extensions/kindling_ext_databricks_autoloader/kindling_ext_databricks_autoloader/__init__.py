"""Databricks Auto Loader (cloudFiles) discovery extension for Kindling file ingestion."""

from .autoloader_file_ingestion import (
    DatabricksAutoLoaderFileIngestionRunner,
    register_runner,
)

__all__ = [
    "DatabricksAutoLoaderFileIngestionRunner",
    "register_runner",
]

__version__ = "0.14.0"


register_runner()
