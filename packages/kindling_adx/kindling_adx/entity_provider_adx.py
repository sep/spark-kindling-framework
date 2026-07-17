"""Azure Data Explorer entity provider for Kindling.

The provider is intentionally append-oriented. ADX is an ingestion target, not a
Delta table, so merge/upsert semantics should be modeled explicitly by the app
or by a later provider feature instead of being implied.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from injector import inject
from kindling.data_entities import EntityMetadata
from kindling.entity_provider import (
    BaseEntityProvider,
    StreamWritableEntityProvider,
    WritableEntityProvider,
)
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import GlobalInjector
from kindling.spark_config import get_or_create_spark_session
from kindling.spark_log_provider import PythonLoggerProvider
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

ADX_SPARK_CONNECTOR_MAVEN_COORDINATE = "com.microsoft.azure.kusto:kusto-spark_3.0_2.12:7.0.6"
GENERIC_FORMAT = "com.microsoft.kusto.spark.datasource"
SYNAPSE_FORMAT = "com.microsoft.kusto.spark.synapse.datasource"


class AdxEntityProvider(BaseEntityProvider, WritableEntityProvider, StreamWritableEntityProvider):
    """Read and write DataFrames against Azure Data Explorer through the Kusto Spark connector."""

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("AdxEntityProvider")

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """Read an ADX table (or `provider.query` KQL result) as a batch DataFrame.

        Requires database Viewer permissions for the configured identity —
        write-only entities keep working without them because the persist path
        never calls read.
        """
        config = self._get_provider_config(entity_metadata)
        options = self._build_read_options(entity_metadata, config)
        source_format = self._source_format(config)

        self.logger.info(
            "Reading entity '%s' from ADX %s using %s",
            entity_metadata.entityid,
            (
                f"query on database '{options.get('kustoDatabase')}'"
                if "kustoQuery" in options
                else f"table '{options.get('kustoTable')}'"
            ),
            source_format,
        )

        spark = get_or_create_spark_session()
        reader = spark.read.format(source_format)
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load()

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """Return configured existence assumption for write-path compatibility.

        Kindling's current persist path asks whether the destination exists before
        choosing append vs write. For ADX both operations route through the same
        Spark sink, so defaulting to True keeps runtime ingestion append-oriented
        and avoids requiring read/query permissions for a write-only target.
        """
        config = self._get_provider_config(entity_metadata)
        return bool(config.get("assume_exists", True))

    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Write DataFrame to ADX using the configured save mode."""
        config = self._get_provider_config(entity_metadata)
        mode = str(config.get("save_mode", config.get("mode", "Append")))
        self._save(df, entity_metadata, mode)

    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Append DataFrame rows to ADX."""
        self._save(df, entity_metadata, "Append")

    def append_as_stream(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        checkpoint_location: str,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> StreamingQuery:
        """Append a streaming DataFrame to ADX through the Kusto Spark connector."""
        config = self._get_provider_config(entity_metadata)
        if options:
            config = {**config, **options}

        connector_options = self._build_options(entity_metadata, config)
        source_format = format or self._source_format(config)
        output_mode = str(config.get("output_mode", "append"))
        query_name = config.get("query_name")

        self.logger.info(
            "Starting streaming write for entity '%s' to ADX table '%s' using %s",
            entity_metadata.entityid,
            connector_options.get("kustoTable"),
            source_format,
        )

        writer = df.writeStream.format(source_format).outputMode(output_mode)
        if query_name:
            writer = writer.queryName(str(query_name))
        writer = writer.option("checkpointLocation", checkpoint_location)
        for key, value in connector_options.items():
            writer = writer.option(key, value)
        return writer.start()

    def _save(self, df: DataFrame, entity_metadata: EntityMetadata, mode: str) -> None:
        config = self._get_provider_config(entity_metadata)
        options = self._build_options(entity_metadata, config)
        source_format = self._source_format(config)

        self.logger.info(
            "Writing entity '%s' to ADX table '%s' using %s",
            entity_metadata.entityid,
            options.get("kustoTable"),
            source_format,
        )

        writer = df.write.format(source_format)
        for key, value in options.items():
            writer = writer.option(key, value)
        writer.mode(mode).save()

    def _source_format(self, config: Dict[str, Any]) -> str:
        if "format" in config:
            return str(config["format"])
        if self._auth_mode(config) == "synapse_linked_service":
            return SYNAPSE_FORMAT
        return GENERIC_FORMAT

    def _build_read_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, str]:
        database = config.get("database")
        if not database:
            raise ValueError(f"ADX entity '{entity_metadata.entityid}' requires provider.database")

        options: Dict[str, Any] = {"kustoDatabase": database}

        query = config.get("query") or config.get("kusto_query")
        table = config.get("table") or config.get("table_name")
        if query:
            options["kustoQuery"] = query
        elif table:
            options["kustoTable"] = table
        else:
            raise ValueError(
                f"ADX entity '{entity_metadata.entityid}' requires provider.table "
                "or provider.query for reads"
            )

        options.update(self._connection_options(entity_metadata, config))
        return self._stringify_options(options)

    def _build_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, str]:
        database = config.get("database")
        table = config.get("table") or config.get("table_name")
        if not database:
            raise ValueError(f"ADX entity '{entity_metadata.entityid}' requires provider.database")
        if not table:
            raise ValueError(f"ADX entity '{entity_metadata.entityid}' requires provider.table")

        options: Dict[str, Any] = {
            "kustoDatabase": database,
            "kustoTable": table,
            "writeMode": config.get("write_mode", "Queued"),
            "tableCreateOptions": config.get("table_create_options", "FailIfNotExist"),
        }

        options.update(self._connection_options(entity_metadata, config))
        return self._stringify_options(options)

    def _connection_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Cluster/linked-service, auth, and passthrough options shared by reads and writes."""
        options: Dict[str, Any] = {}

        auth_mode = self._auth_mode(config)
        if auth_mode == "synapse_linked_service":
            linked_service = config.get("linked_service")
            if not linked_service:
                raise ValueError(
                    f"ADX entity '{entity_metadata.entityid}' uses synapse_linked_service auth "
                    "but provider.linked_service is not set"
                )
            options["spark.synapse.linkedService"] = linked_service
        else:
            cluster = config.get("cluster")
            if not cluster:
                raise ValueError(
                    f"ADX entity '{entity_metadata.entityid}' requires provider.cluster "
                    f"for auth mode '{auth_mode}'"
                )
            options["kustoCluster"] = cluster
            options.update(self._auth_options(entity_metadata, config, auth_mode))

        options.update(self._extra_connector_options(config))
        return options

    def _stringify_options(self, options: Dict[str, Any]) -> Dict[str, str]:
        return {
            key: self._stringify_option(value)
            for key, value in options.items()
            if value is not None
        }

    def _auth_mode(self, config: Dict[str, Any]) -> str:
        return str(config.get("auth", config.get("auth_mode", "managed_identity"))).lower()

    def _auth_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any], auth_mode: str
    ) -> Dict[str, Any]:
        if auth_mode in ("managed_identity", "system_identity", "system_assigned_identity"):
            options: Dict[str, Any] = {"managedIdentityAuth": "true"}
            client_id = config.get("managed_identity_client_id") or config.get("client_id")
            if client_id:
                options["managedIdentityClientId"] = client_id
            return options

        if auth_mode == "access_token":
            token = config.get("access_token")
            if not token:
                raise ValueError(
                    f"ADX entity '{entity_metadata.entityid}' uses access_token auth "
                    "but provider.access_token is not set"
                )
            return {"accessToken": token}

        if auth_mode in ("service_principal", "spn"):
            required = {
                "kustoAadAppId": config.get("app_id") or config.get("client_id"),
                "kustoAadAppSecret": config.get("app_secret") or config.get("client_secret"),
                "kustoAadAuthorityID": config.get("tenant_id") or config.get("authority_id"),
            }
            missing = [key for key, value in required.items() if not value]
            if missing:
                raise ValueError(
                    f"ADX entity '{entity_metadata.entityid}' service principal auth "
                    f"is missing options: {', '.join(missing)}"
                )
            return required

        raise ValueError(
            f"Unsupported ADX auth mode '{auth_mode}'. Supported modes: "
            "managed_identity, synapse_linked_service, access_token, service_principal."
        )

    def _extra_connector_options(self, config: Dict[str, Any]) -> Dict[str, Any]:
        prefix = "option."
        return {
            key[len(prefix) :]: value
            for key, value in config.items()
            if key.startswith(prefix) and value is not None
        }

    def _stringify_option(self, value: Any) -> str:
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)


def register_provider(provider_type: str = "adx") -> None:
    """Register the ADX provider with Kindling's entity provider registry."""
    registry = GlobalInjector.get(EntityProviderRegistry)
    registry.register_provider(provider_type, AdxEntityProvider)
