"""Azure Cosmos DB (NoSQL API) entity provider for Kindling.

Writes are upserts: the Cosmos Spark connector's default ``ItemOverwrite``
strategy overwrites by ``(id, partition key)``, which makes writes naturally
idempotent — a retried persist converges instead of duplicating. Map the
entity's logical key onto the document ``id`` to get merge-like semantics.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from injector import inject
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from kindling.data_entities import EntityMetadata
from kindling.entity_provider import (
    BaseEntityProvider,
    StreamWritableEntityProvider,
    WritableEntityProvider,
)
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import GlobalInjector
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_session import get_or_create_spark_session

COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE = (
    "com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:4.37.2"
)
COSMOS_FORMAT = "cosmos.oltp"


class CosmosEntityProvider(
    BaseEntityProvider, WritableEntityProvider, StreamWritableEntityProvider
):
    """Read and write DataFrames against Cosmos DB through the Cosmos Spark connector."""

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("CosmosEntityProvider")

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """Read a Cosmos container (or `provider.query` SQL result) as a batch DataFrame.

        Schema inference is enabled by default (`provider.infer_schema`);
        heterogeneous containers usually want a `provider.query` that projects
        the relevant fields.
        """
        config = self._get_provider_config(entity_metadata)
        options = self._build_read_options(entity_metadata, config)

        self.logger.info(
            "Reading entity '%s' from Cosmos container '%s'%s",
            entity_metadata.entityid,
            options.get("spark.cosmos.container"),
            " via custom query" if "spark.cosmos.read.customQuery" in options else "",
        )

        spark = get_or_create_spark_session()
        reader = spark.read.format(COSMOS_FORMAT)
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load()

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """Return configured existence assumption for write-path compatibility.

        Cosmos writes are upserts to a pre-provisioned container, so append
        and write behave identically; assuming existence keeps the persist
        path append-oriented (same posture as the ADX provider).
        """
        config = self._get_provider_config(entity_metadata)
        return bool(config.get("assume_exists", True))

    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Write (upsert) DataFrame documents into the Cosmos container."""
        self._save(df, entity_metadata)

    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Append DataFrame documents into the Cosmos container.

        Uses the configured write strategy (default ``ItemOverwrite`` = upsert,
        idempotent under retry). Set ``provider.write_strategy: ItemAppend``
        for insert-only semantics.
        """
        self._save(df, entity_metadata)

    def append_as_stream(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        checkpoint_location: str,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> StreamingQuery:
        """Append a streaming DataFrame to Cosmos through the connector sink."""
        config = self._get_provider_config(entity_metadata)
        if options:
            config = {**config, **options}

        connector_options = self._build_write_options(entity_metadata, config)
        output_mode = str(config.get("output_mode", "append"))
        query_name = config.get("query_name")

        self.logger.info(
            "Starting streaming write for entity '%s' to Cosmos container '%s'",
            entity_metadata.entityid,
            connector_options.get("spark.cosmos.container"),
        )

        writer = df.writeStream.format(format or COSMOS_FORMAT).outputMode(output_mode)
        if query_name:
            writer = writer.queryName(str(query_name))
        writer = writer.option("checkpointLocation", checkpoint_location)
        for key, value in connector_options.items():
            writer = writer.option(key, value)
        return writer.start()

    def _save(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        config = self._get_provider_config(entity_metadata)
        options = self._build_write_options(entity_metadata, config)
        # The Cosmos Spark sink requires mode Append; write semantics are
        # controlled by spark.cosmos.write.strategy, not the save mode.
        writer = df.write.format(COSMOS_FORMAT)
        for key, value in options.items():
            writer = writer.option(key, value)

        self.logger.info(
            "Writing entity '%s' to Cosmos container '%s' (strategy=%s)",
            entity_metadata.entityid,
            options.get("spark.cosmos.container"),
            options.get("spark.cosmos.write.strategy"),
        )
        writer.mode("Append").save()

    def _build_read_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, str]:
        options = self._connection_options(entity_metadata, config)

        infer_schema = config.get("infer_schema", True)
        options["spark.cosmos.read.inferSchema.enabled"] = infer_schema

        query = config.get("query") or config.get("custom_query")
        if query:
            options["spark.cosmos.read.customQuery"] = query

        return self._stringify_options(options)

    def _build_write_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, str]:
        options = self._connection_options(entity_metadata, config)
        options["spark.cosmos.write.strategy"] = config.get("write_strategy", "ItemOverwrite")
        return self._stringify_options(options)

    def _connection_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Endpoint, database/container, auth, and passthrough options."""
        endpoint = config.get("account_endpoint") or config.get("endpoint")
        database = config.get("database")
        container = config.get("container")
        for name, value in (
            ("provider.account_endpoint", endpoint),
            ("provider.database", database),
            ("provider.container", container),
        ):
            if not value:
                raise ValueError(f"Cosmos entity '{entity_metadata.entityid}' requires {name}")

        options: Dict[str, Any] = {
            "spark.cosmos.accountEndpoint": endpoint,
            "spark.cosmos.database": database,
            "spark.cosmos.container": container,
        }
        options.update(self._auth_options(entity_metadata, config))
        options.update(self._extra_connector_options(config))
        return options

    def _auth_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        auth_mode = str(config.get("auth", config.get("auth_mode", "service_principal"))).lower()

        if auth_mode in ("service_principal", "spn"):
            required = {
                "spark.cosmos.auth.aad.clientId": config.get("client_id") or config.get("app_id"),
                "spark.cosmos.auth.aad.clientSecret": (
                    config.get("client_secret") or config.get("app_secret")
                ),
                "spark.cosmos.account.tenantId": (
                    config.get("tenant_id") or config.get("authority_id")
                ),
            }
            missing = [key for key, value in required.items() if not value]
            if missing:
                raise ValueError(
                    f"Cosmos entity '{entity_metadata.entityid}' service principal auth "
                    f"is missing options: {', '.join(missing)}"
                )
            required["spark.cosmos.auth.type"] = "ServicePrincipal"
            subscription_id = config.get("subscription_id")
            if subscription_id:
                required["spark.cosmos.account.subscriptionId"] = subscription_id
            resource_group = config.get("resource_group")
            if resource_group:
                required["spark.cosmos.account.resourceGroupName"] = resource_group
            return required

        if auth_mode in ("master_key", "key", "account_key"):
            account_key = config.get("account_key") or config.get("key")
            if not account_key:
                raise ValueError(
                    f"Cosmos entity '{entity_metadata.entityid}' uses master_key auth "
                    "but provider.account_key is not set"
                )
            return {"spark.cosmos.accountKey": account_key}

        raise ValueError(
            f"Unsupported Cosmos auth mode '{auth_mode}'. Supported modes: "
            "service_principal, master_key."
        )

    def _extra_connector_options(self, config: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "option."
        return {
            key[len(prefix) :]: value
            for key, value in config.items()
            if key.startswith(prefix) and value is not None
        }

    def _stringify_options(self, options: Dict[str, Any]) -> Dict[str, str]:
        return {
            key: self._stringify_option(value)
            for key, value in options.items()
            if value is not None
        }

    def _stringify_option(self, value: Any) -> str:
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)


def register_provider(provider_type: str = "cosmos") -> None:
    """Register the Cosmos provider with Kindling's entity provider registry."""
    registry = GlobalInjector.get(EntityProviderRegistry)
    registry.register_provider(provider_type, CosmosEntityProvider)
