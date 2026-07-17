"""Azure Data Explorer entity provider extension for Kindling."""

from .entity_provider_adx import (
    ADX_SPARK_CONNECTOR_MAVEN_COORDINATE,
    AdxEntityProvider,
    register_provider,
)

__all__ = [
    "ADX_SPARK_CONNECTOR_MAVEN_COORDINATE",
    "AdxEntityProvider",
    "register_provider",
]

__version__ = "0.1.0"


register_provider()
