"""Azure Cosmos DB entity provider extension for Kindling."""

from .entity_provider_cosmos import (
    COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE,
    CosmosEntityProvider,
    register_provider,
)

__all__ = [
    "COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE",
    "CosmosEntityProvider",
    "register_provider",
]

__version__ = "0.1.0"


register_provider()
