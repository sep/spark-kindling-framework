"""Azure Data Explorer entity provider (API-based) for Kindling.

Talks to ADX through the azure-kusto-data / azure-kusto-ingest Python SDKs
instead of the JVM Kusto Spark connector (``kindling_ext_adx``). Use it where
the connector cannot run — UC shared/standard access mode clusters,
serverless, standalone Python — or where installing cluster libraries is not
an option.

Data moves through the driver: query results and ingestion batches are
materialized as pandas DataFrames. That makes this provider a fit for the
*boundaries* of a solution — reference data, config tables, modest extracts —
not bulk pipeline storage; the Spark-connector extension remains the choice
for large volumes and streaming.

Like the connector provider, it is append-oriented: ADX is an ingestion
target, queued ingestion is at-least-once, and there is no merge. Keep
execution retry off for ADX-targeted pipes.

Configuration (entity tags):
    provider_type: "adx-api"
    provider.cluster: query endpoint URI, e.g. https://<name>.<region>.kusto.windows.net (required)
    provider.database: target database (required)
    provider.table: target table (required for writes; reads may use query)
    provider.query: KQL for reads (defaults to reading provider.table)
    provider.ingest_uri: ingestion endpoint (default: cluster URI with "ingest-" host prefix)
    provider.auth: managed_identity (default) | service_principal | azure_cli | access_token
    provider.client_id / provider.app_id: SP app id (service_principal; falls back to AZURE_CLIENT_ID)
    provider.client_secret / provider.app_secret: SP secret (falls back to AZURE_CLIENT_SECRET)
    provider.tenant_id / provider.authority_id: SP tenant (falls back to AZURE_TENANT_ID)
    provider.managed_identity_client_id: user-assigned MI client id (optional)
    provider.access_token: bearer token (access_token auth)
    provider.flush_immediately: skip the ingestion batching window (default false)
    provider.assume_exists: existence fallback when the metadata query fails (default true)

Requires the [adx] extra: pip install 'spark-kindling[adx]'
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from injector import inject
from pyspark.sql import DataFrame

from .data_entities import EntityMetadata
from .entity_provider import (
    BaseEntityProvider,
    DestinationEnsuringProvider,
    WritableEntityProvider,
)
from .spark_log_provider import PythonLoggerProvider
from .spark_session import get_or_create_spark_session

_INSTALL_HINT = (
    "The adx-api provider requires azure-kusto-data and azure-kusto-ingest. "
    "Install with: pip install 'spark-kindling[adx]'"
)

# Spark simpleString -> Kusto column type. Complex types land as dynamic.
_KUSTO_TYPES = {
    "string": "string",
    "tinyint": "int",
    "smallint": "int",
    "int": "int",
    "bigint": "long",
    "float": "real",
    "double": "real",
    "boolean": "bool",
    "date": "datetime",
    "timestamp": "datetime",
    "timestamp_ntz": "datetime",
    "binary": "string",
}


def _require_kusto_data():
    try:
        from azure.kusto import data as kusto_data

        return kusto_data
    except ImportError as exc:
        raise ImportError(_INSTALL_HINT) from exc


def _require_kusto_ingest():
    try:
        from azure.kusto import ingest as kusto_ingest

        return kusto_ingest
    except ImportError as exc:
        raise ImportError(_INSTALL_HINT) from exc


class AdxApiEntityProvider(BaseEntityProvider, WritableEntityProvider, DestinationEnsuringProvider):
    """Read and write DataFrames against ADX through the Kusto Python SDKs."""

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider):
        self.logger = logger_provider.get_logger("AdxApiEntityProvider")
        self._query_clients: Dict[str, Any] = {}
        self._ingest_clients: Dict[str, Any] = {}

    # ---- BaseEntityProvider ----

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame:
        """Run the entity's KQL (or read its table) and return a batch DataFrame.

        Results are materialized on the driver via pandas. An empty result
        needs the entity's declared schema to build a typed empty DataFrame.
        """
        from azure.kusto.data.helpers import (  # noqa: PLC0415
            dataframe_from_result_table,
        )

        config = self._get_provider_config(entity_metadata)
        database = self._require(entity_metadata, config, "database")
        query = config.get("query") or config.get("kusto_query")
        if not query:
            # A bare table name is a valid KQL query.
            query = self._require(entity_metadata, config, "table", alternatives=("table_name",))

        self.logger.info(
            f"Reading entity '{entity_metadata.entityid}' from ADX database "
            f"'{database}' via API"
        )
        result = self._query_client(entity_metadata, config).execute(database, str(query))
        pdf = dataframe_from_result_table(result.primary_results[0])

        spark = get_or_create_spark_session()
        if pdf.empty:
            if entity_metadata.schema is not None:
                return spark.createDataFrame([], schema=self._as_struct(entity_metadata))
            raise ValueError(
                f"ADX query for entity '{entity_metadata.entityid}' returned no rows and "
                "the entity declares no schema to type an empty DataFrame."
            )
        return spark.createDataFrame(pdf)

    def check_entity_exists(self, entity_metadata: EntityMetadata) -> bool:
        """Check table existence via metadata query; fall back to assume_exists."""
        config = self._get_provider_config(entity_metadata)
        database = self._require(entity_metadata, config, "database")
        table = self._require(entity_metadata, config, "table", alternatives=("table_name",))
        try:
            result = self._query_client(entity_metadata, config).execute_mgmt(
                database, f".show tables | where TableName == '{table}'"
            )
            return len(result.primary_results[0].rows) > 0
        except ImportError:
            raise
        except Exception as e:
            fallback = bool(config.get("assume_exists", True))
            self.logger.warning(
                f"Could not check existence of ADX table '{table}' for entity "
                f"'{entity_metadata.entityid}' ({e}); assuming exists={fallback}"
            )
            return fallback

    # ---- WritableEntityProvider ----

    def write_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Queued ingestion. ADX is append-only; write and append are the same."""
        self._ingest(df, entity_metadata)

    def append_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Append DataFrame rows to ADX via queued ingestion."""
        self._ingest(df, entity_metadata)

    # ---- DestinationEnsuringProvider ----

    def ensure_destination(self, entity_metadata: EntityMetadata) -> None:
        """Create the target table from the entity's declared schema.

        Uses ``.create-merge table`` (idempotent; adds missing columns, never
        drops). This closes the connector provider's gap where queued
        ingestion fails permanently for tables the ingestion service has
        never seen.
        """
        config = self._get_provider_config(entity_metadata)
        database = self._require(entity_metadata, config, "database")
        table = self._require(entity_metadata, config, "table", alternatives=("table_name",))

        if entity_metadata.schema is None:
            self.logger.debug(
                f"Entity '{entity_metadata.entityid}' declares no schema; "
                "nothing to ensure in ADX"
            )
            return

        columns = ", ".join(
            f"['{field.name}']: {self._kusto_type(field.dataType)}"
            for field in self._as_struct(entity_metadata).fields
        )
        command = f".create-merge table ['{table}'] ({columns})"
        self.logger.info(f"Ensuring ADX table '{table}' for entity '{entity_metadata.entityid}'")
        self._query_client(entity_metadata, config).execute_mgmt(database, command)

    # ---- Ingestion ----

    def _ingest(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        kusto_ingest = _require_kusto_ingest()

        config = self._get_provider_config(entity_metadata)
        database = self._require(entity_metadata, config, "database")
        table = self._require(entity_metadata, config, "table", alternatives=("table_name",))

        pdf = df.toPandas()
        if pdf.empty:
            self.logger.info(
                f"Entity '{entity_metadata.entityid}': empty DataFrame, skipping ADX ingestion"
            )
            return

        properties = kusto_ingest.IngestionProperties(
            database=database,
            table=table,
            flush_immediately=self._as_bool(config.get("flush_immediately", False)),
        )
        self.logger.info(
            f"Ingesting {len(pdf)} row(s) into ADX table '{table}' for entity "
            f"'{entity_metadata.entityid}'"
        )
        self._ingest_client(entity_metadata, config).ingest_from_dataframe(
            pdf, ingestion_properties=properties
        )

    # ---- Clients and auth ----

    def _query_client(self, entity_metadata: EntityMetadata, config: Dict[str, Any]):
        kusto_data = _require_kusto_data()
        cluster = self._require(entity_metadata, config, "cluster")
        cached = self._query_clients.get(entity_metadata.entityid)
        if cached is None:
            kcsb = self._connection_builder(entity_metadata, config, cluster)
            cached = kusto_data.KustoClient(kcsb)
            self._query_clients[entity_metadata.entityid] = cached
        return cached

    def _ingest_client(self, entity_metadata: EntityMetadata, config: Dict[str, Any]):
        kusto_ingest = _require_kusto_ingest()
        cluster = self._require(entity_metadata, config, "cluster")
        ingest_uri = config.get("ingest_uri") or self._default_ingest_uri(cluster)
        cached = self._ingest_clients.get(entity_metadata.entityid)
        if cached is None:
            kcsb = self._connection_builder(entity_metadata, config, ingest_uri)
            cached = kusto_ingest.QueuedIngestClient(kcsb)
            self._ingest_clients[entity_metadata.entityid] = cached
        return cached

    @staticmethod
    def _default_ingest_uri(cluster: str) -> str:
        if "://" in cluster:
            scheme, rest = cluster.split("://", 1)
            return f"{scheme}://ingest-{rest}"
        return f"https://ingest-{cluster}"

    def _connection_builder(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any], uri: str
    ):
        kusto_data = _require_kusto_data()
        kcsb = kusto_data.KustoConnectionStringBuilder
        auth_mode = str(config.get("auth", config.get("auth_mode", "managed_identity"))).lower()

        if auth_mode in ("managed_identity", "system_identity", "system_assigned_identity"):
            client_id = config.get("managed_identity_client_id") or config.get("client_id")
            if client_id:
                return kcsb.with_aad_managed_service_identity_authentication(
                    uri, client_id=str(client_id)
                )
            return kcsb.with_aad_managed_service_identity_authentication(uri)

        if auth_mode in ("service_principal", "spn"):
            app_id = config.get("app_id") or config.get("client_id") or os.getenv("AZURE_CLIENT_ID")
            app_secret = (
                config.get("app_secret")
                or config.get("client_secret")
                or os.getenv("AZURE_CLIENT_SECRET")
            )
            tenant_id = (
                config.get("tenant_id")
                or config.get("authority_id")
                or os.getenv("AZURE_TENANT_ID")
            )
            missing = [
                name
                for name, value in (
                    ("app_id", app_id),
                    ("app_secret", app_secret),
                    ("tenant_id", tenant_id),
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    f"ADX entity '{entity_metadata.entityid}' service principal auth is "
                    f"missing: {', '.join(missing)} (tags or AZURE_* environment)"
                )
            return kcsb.with_aad_application_key_authentication(
                uri, str(app_id), str(app_secret), str(tenant_id)
            )

        if auth_mode == "azure_cli":
            return kcsb.with_az_cli_authentication(uri)

        if auth_mode == "access_token":
            token = config.get("access_token")
            if not token:
                raise ValueError(
                    f"ADX entity '{entity_metadata.entityid}' uses access_token auth "
                    "but provider.access_token is not set"
                )
            return kcsb.with_aad_application_token_authentication(uri, str(token))

        raise ValueError(
            f"Unsupported ADX auth mode '{auth_mode}'. Supported modes: "
            "managed_identity, service_principal, azure_cli, access_token."
        )

    # ---- Helpers ----

    def _require(
        self,
        entity_metadata: EntityMetadata,
        config: Dict[str, Any],
        key: str,
        alternatives: tuple = (),
    ) -> str:
        for candidate in (key, *alternatives):
            value = config.get(candidate)
            if value:
                return str(value)
        raise ValueError(f"ADX entity '{entity_metadata.entityid}' requires provider.{key}")

    @staticmethod
    def _as_struct(entity_metadata: EntityMetadata):
        from pyspark.sql.types import StructType  # noqa: PLC0415

        schema = entity_metadata.schema
        return schema if isinstance(schema, StructType) else StructType(schema)

    @staticmethod
    def _kusto_type(data_type) -> str:
        simple = data_type.simpleString().lower()
        if simple.startswith("decimal"):
            return "decimal"
        if simple.startswith(("array", "map", "struct")):
            return "dynamic"
        return _KUSTO_TYPES.get(simple, "string")

    @staticmethod
    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("true", "1", "yes", "on")
