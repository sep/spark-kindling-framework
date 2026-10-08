"""Azure Cosmos DB entity provider extension for Kindling."""

from .entity_provider_cosmos import (
    COSMOS_CHANGEFEED_FORMAT,
    COSMOS_FORMAT,
    COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE,
    COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES,
    COSMOS_SPARK_CONNECTOR_VERSION,
    CosmosEntityProvider,
    register_provider,
    resolve_cosmos_spark_connector_coordinate,
    spark_family,
)

__all__ = [
    "COSMOS_CHANGEFEED_FORMAT",
    "COSMOS_FORMAT",
    "COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE",
    "COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES",
    "COSMOS_SPARK_CONNECTOR_VERSION",
    "CosmosEntityProvider",
    "register_provider",
    "resolve_cosmos_spark_connector_coordinate",
    "spark_family",
]

__version__ = "0.2.0"


register_provider()
