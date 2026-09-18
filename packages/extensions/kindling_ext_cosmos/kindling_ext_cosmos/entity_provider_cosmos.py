"""Azure Cosmos DB (NoSQL API) entity provider for Kindling.

Writes are upserts: the Cosmos Spark connector's default ``ItemOverwrite``
strategy overwrites by ``(id, partition key)``, which makes writes naturally
idempotent — a retried persist converges instead of duplicating. Map the
entity's logical key onto the document ``id`` to get merge-like semantics.

Streaming reads come from the container's change feed
(``cosmos.oltp.changeFeed``), so a Cosmos entity can be the driving input of
a streaming pipe alongside Delta, Parquet and Event Hubs.

Run-level throughput posture is configured under ``kindling.cosmos.*`` and
applied to every connector read/write before per-entity ``provider.option.*``
overrides — "config dictates, tags override".

The Cosmos Spark connector is a JVM artifact built per Spark line and Scala
binary. Spark 3.x runtimes (Fabric Runtime 1.3, Synapse 3.4/3.5) use the
``_3-<minor>_2-12`` builds; Spark 4.x runtimes (Databricks Runtime 17+) use
the ``_4-<minor>_2-13`` builds. :func:`resolve_cosmos_spark_connector_coordinate`
picks the Maven coordinate for the running (or a named) Spark version; the
Python wheel itself is Spark-version neutral and its ``spark_3_x`` / ``spark_4_x``
extras only pin ``pyspark`` for local and CI environments.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Tuple

from injector import inject
from kindling.data_entities import EntityMetadata
from kindling.entity_provider import (
    DECLARATIVE_SOURCE_OPTION,
    BaseEntityProvider,
    DeclarableStreamingSource,
    SourceValidationIssue,
    StreamableEntityProvider,
    StreamingSourceSpec,
    StreamWritableEntityProvider,
    WritableEntityProvider,
)
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.features import _coerce_bool
from kindling.injection import GlobalInjector
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

# The Cosmos Spark connector is a JVM package published per Spark line
# (and Scala binary): one connector release, several artifacts. The Python
# wheel cannot install it; put the coordinate for the pool's Spark version on
# the cluster (``spark.jars.packages`` or the platform's library UI). This
# is keyed by Spark line, not platform (the ``spark_3_x`` / ``spark_4_x``
# extras follow the same split): a Databricks runtime on Spark 4.0 and a
# standalone Spark 4.0 use the same artifact. The Python side is
# version-neutral -- it only names the data source format and options.
COSMOS_SPARK_CONNECTOR_VERSION = "4.49.2"
COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES: Mapping[str, str] = {
    # Spark 3.4 / Scala 2.12: Synapse Spark 3.4, Databricks 13.x-14.x
    "3.4": f"com.azure.cosmos.spark:azure-cosmos-spark_3-4_2-12:{COSMOS_SPARK_CONNECTOR_VERSION}",
    # Spark 3.5 / Scala 2.12: Fabric Runtime 1.3, Synapse Spark 3.5, standalone
    "3.5": f"com.azure.cosmos.spark:azure-cosmos-spark_3-5_2-12:{COSMOS_SPARK_CONNECTOR_VERSION}",
    # Spark 4.0 / Scala 2.13: Databricks runtimes on Spark 4.0 (e.g. 17.x)
    "4.0": f"com.azure.cosmos.spark:azure-cosmos-spark_4-0_2-13:{COSMOS_SPARK_CONNECTOR_VERSION}",
    # Spark 4.1 / Scala 2.13: standalone-4x, Databricks runtimes on Spark 4.1
    "4.1": f"com.azure.cosmos.spark:azure-cosmos-spark_4-1_2-13:{COSMOS_SPARK_CONNECTOR_VERSION}",
}
# Backward-compatible name: the Spark 3.5 artifact, which is what every
# pre-Spark-4 consumer was told to install.
COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE = COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES["3.5"]

COSMOS_FORMAT = "cosmos.oltp"
# The change feed is a distinct (read-only, streaming) data source from the
# OLTP one used for batch reads and all writes.
COSMOS_CHANGEFEED_FORMAT = "cosmos.oltp.changeFeed"

# provider.changefeed.mode -> connector spark.cosmos.changeFeed.mode. The
# connector accepts both the legacy (Incremental/FullFidelity) and current
# (LatestVersion/AllVersionsAndDeletes) spellings; we emit the current ones.
CHANGEFEED_MODES: Mapping[str, str] = {
    "latest_version": "LatestVersion",
    "full_fidelity": "AllVersionsAndDeletes",
}
CHANGEFEED_START_FROM_KEYWORDS = ("Beginning", "Now")

# Connector-side defaults, pinned explicitly so a bulk read's RU posture is a
# visible, overridable setting rather than an implicit one.
READ_PARTITIONING_STRATEGIES = ("Default", "Custom", "Restrictive", "Aggressive")
DEFAULT_READ_PARTITIONING_STRATEGY = "Default"
DEFAULT_READ_MAX_ITEM_COUNT = 1000

CONFIG_PREFIX = "kindling.cosmos"

DECLARABLE_SUPPORTED_TAGS: Tuple[str, ...] = (
    "provider.account_endpoint",
    "provider.database",
    "provider.container",
    "provider.auth",
    "provider.client_id",
    "provider.client_secret",
    "provider.tenant_id",
    "provider.subscription_id",
    "provider.resource_group",
    "provider.account_key",
    "provider.changefeed.mode",
    "provider.changefeed.start_from",
    "provider.changefeed.items_per_trigger",
    "provider.infer_schema",
    "provider.option.*",
)

# One table drives both the runtime auth options and the declaration-time
# completeness check, so an alias or a new required credential is added in
# exactly one place: (connector option, provider tag, accepted alias tags).
SERVICE_PRINCIPAL_CREDENTIALS: Tuple[Tuple[str, str, Tuple[str, ...]], ...] = (
    ("spark.cosmos.auth.aad.clientId", "client_id", ("app_id",)),
    ("spark.cosmos.auth.aad.clientSecret", "client_secret", ("app_secret",)),
    ("spark.cosmos.account.tenantId", "tenant_id", ("authority_id",)),
    # The connector resolves account metadata through ARM for ServicePrincipal
    # auth, so subscription and resource group are hard requirements.
    ("spark.cosmos.account.subscriptionId", "subscription_id", ()),
    ("spark.cosmos.account.resourceGroupName", "resource_group", ()),
)
MASTER_KEY_CREDENTIAL: Tuple[str, str, Tuple[str, ...]] = (
    "spark.cosmos.accountKey",
    "account_key",
    ("key",),
)
SERVICE_PRINCIPAL_AUTH_MODES = ("service_principal", "spn")
MASTER_KEY_AUTH_MODES = ("master_key", "key", "account_key")

# The connector parses a non-keyword startFrom with DateTimeFormatter.ISO_INSTANT:
# a UTC instant such as 2026-01-31T00:00:00Z (optional fractional seconds).
# Date-only, zone-naive and non-UTC-offset values are rejected by the
# connector at stream start, so they are rejected here at validation time.
CHANGEFEED_START_FROM_INSTANT = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?Z$")

# Write strategies that upsert (merge semantics) vs. insert-only.
MERGE_WRITE_STRATEGIES = ("ItemOverwrite", "ItemOverwriteIfNotModified", "ItemPatch")
INSERT_WRITE_STRATEGY = "ItemAppend"


class CosmosEntityProvider(
    BaseEntityProvider,
    WritableEntityProvider,
    StreamWritableEntityProvider,
    StreamableEntityProvider,
    DeclarableStreamingSource,
):
    """Read and write DataFrames against Cosmos DB through the Cosmos Spark connector.

    Provider configuration (entity tags with the ``provider.`` prefix):

    - ``provider.account_endpoint``, ``provider.database``, ``provider.container``
    - ``provider.auth``: ``service_principal`` (default) or ``master_key``, plus
      the matching credential tags (see the extension README)
    - ``provider.query``: Cosmos SQL for batch reads
    - ``provider.write_strategy``: ``ItemOverwrite`` (default), ``ItemAppend``,
      ``ItemDelete``
    - ``provider.changefeed.mode``: ``latest_version`` (default) or
      ``full_fidelity`` (all versions and deletes; the container must be
      provisioned for it)
    - ``provider.changefeed.start_from``: ``Beginning`` (default), ``Now`` or
      a UTC instant such as ``2026-01-31T00:00:00Z``
    - ``provider.changefeed.items_per_trigger``: approximate items per micro-batch
    - ``provider.option.<connector option>``: verbatim passthrough, highest
      precedence

    Run-level configuration (``kindling.cosmos.*``), applied to every
    connector read and write below ``provider.option.*``:

    - ``kindling.cosmos.read.partitioning_strategy`` (default ``Default``)
    - ``kindling.cosmos.read.max_item_count`` (default ``1000``)
    - ``kindling.cosmos.throughput_control.enabled`` (default ``false``); when
      enabled, ``group_name`` and exactly one of ``target_threshold`` /
      ``target_throughput`` are required, and ``global_control.database`` /
      ``global_control.container`` are optional (both or neither)
    """

    @inject
    def __init__(
        self,
        logger_provider: PythonLoggerProvider,
        config_service: Optional[ConfigService] = None,
    ):
        self.logger = logger_provider.get_logger("CosmosEntityProvider")
        self.config_service = config_service

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

    # ---- StreamableEntityProvider ----

    def read_entity_as_stream(
        self,
        entity_metadata: EntityMetadata,
        format: Optional[str] = None,
        options: Optional[dict] = None,
    ) -> DataFrame:
        """Read the container's change feed as a streaming DataFrame.

        Continuation tokens are Spark Structured Streaming's responsibility and
        live in the sink's checkpoint, so nothing is passed on the read side.
        ``options`` are merged over the entity's ``provider.*`` config (same
        keys, without the prefix). The declarative engine's
        ``declarativeSource`` marker is accepted and ignored.
        """
        if format and format != COSMOS_CHANGEFEED_FORMAT:
            raise ValueError(
                f"Cosmos streaming reads use format {COSMOS_CHANGEFEED_FORMAT!r}, "
                f"got {format!r}"
            )

        config = self._get_provider_config(entity_metadata)
        if options:
            config = {**config, **options}
        config.pop(DECLARATIVE_SOURCE_OPTION, None)

        connector_options = self._build_changefeed_options(entity_metadata, config)

        self.logger.info(
            "Starting change-feed streaming read for entity '%s' from Cosmos container "
            "'%s' (mode=%s, startFrom=%s)",
            entity_metadata.entityid,
            connector_options.get("spark.cosmos.container"),
            connector_options.get("spark.cosmos.changeFeed.mode"),
            connector_options.get("spark.cosmos.changeFeed.startFrom"),
        )

        spark = get_or_create_spark_session()
        reader = spark.readStream.format(COSMOS_CHANGEFEED_FORMAT)
        for key, value in connector_options.items():
            reader = reader.option(key, value)
        return reader.load()

    # ---- DeclarableStreamingSource ----

    def streaming_source_spec(self, entity_metadata: EntityMetadata) -> StreamingSourceSpec:
        """Return an inert, secret-safe declaration of the change-feed source.

        Reports option *names* and structural identity only — never the
        account key, client secret or any other connector option value. No
        Spark, JVM, network or secret access happens here.
        """
        tags = entity_metadata.tags or {}
        config = self._get_provider_config(entity_metadata)
        issues = []

        for tag_key, config_key in (
            ("provider.account_endpoint", ("account_endpoint", "endpoint")),
            ("provider.database", ("database",)),
            ("provider.container", ("container",)),
        ):
            if not any(config.get(key) for key in config_key):
                issues.append(
                    SourceValidationIssue(
                        tag=tag_key,
                        constraint="is required",
                        remediation=f"set {tag_key} on the entity",
                    )
                )

        issues.extend(self._changefeed_issues(config))
        issues.extend(self._auth_issues(config))

        database = str(config.get("database") or "").strip()
        container = str(config.get("container") or "").strip()
        identity = f"{database}/{container}" if database and container else container or database

        return StreamingSourceSpec(
            provider_type=str(tags.get("provider_type", "cosmos")),
            source_format=COSMOS_CHANGEFEED_FORMAT,
            source_identity=identity,
            supported_option_names=DECLARABLE_SUPPORTED_TAGS,
            applied_option_names=tuple(sorted(key for key in tags if key.startswith("provider."))),
            validation_issues=tuple(issues),
        )

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

    def merge_to_entity(self, df: DataFrame, entity_metadata: EntityMetadata) -> None:
        """Merge DataFrame documents into the Cosmos container.

        Cosmos has no server-side MERGE; the connector's ``ItemOverwrite``
        strategy already upserts by ``(id, partition key)``, so merge *is* the
        default write. Declaring it formally lets the persist path treat a
        Cosmos entity with ``merge_columns`` as merge-capable instead of
        falling back to append. The entity's ``merge_columns`` are not applied
        as a match condition — the document ``id`` is the key — so the merge
        key must be mapped onto ``id`` (exact, lower-case column name).

        ``write.mode`` on the entity selects the strategy the same way it does
        for Delta and memory entities: ``insert`` writes with ``ItemAppend``
        (insert-if-absent, existing documents untouched); ``merge`` or unset
        writes with ``ItemOverwrite`` (full-document upsert). An explicit
        ``provider.write_strategy`` must agree with that mode; ``ItemDelete``
        is never a merge.
        """
        config = self._get_provider_config(entity_metadata)
        strategy = self._merge_write_strategy(entity_metadata, config)
        if "id" not in df.columns:
            raise ValueError(
                f"Cosmos entity '{entity_metadata.entityid}' merge requires an 'id' "
                "column (exact name): documents are upserted by (id, partition key)"
            )
        self._save(df, entity_metadata, write_strategy=strategy)

    def _merge_write_strategy(self, entity_metadata: EntityMetadata, config: Dict[str, Any]) -> str:
        write_mode = str((entity_metadata.tags or {}).get("write.mode") or "").strip().lower()
        explicit = config.get("write_strategy")
        explicit_name = str(explicit).strip() if explicit else ""

        if write_mode == "insert":
            if explicit_name and explicit_name.lower() != INSERT_WRITE_STRATEGY.lower():
                raise ValueError(
                    f"Cosmos entity '{entity_metadata.entityid}' has write.mode 'insert' "
                    f"but provider.write_strategy '{explicit_name}'; insert-only merges "
                    f"use {INSERT_WRITE_STRATEGY} (drop the tag or set it to that)"
                )
            return INSERT_WRITE_STRATEGY

        allowed = {name.lower(): name for name in MERGE_WRITE_STRATEGIES}
        if not explicit_name:
            return MERGE_WRITE_STRATEGIES[0]
        if explicit_name.lower() not in allowed:
            raise ValueError(
                f"Cosmos entity '{entity_metadata.entityid}' cannot merge with "
                f"provider.write_strategy '{explicit_name}'; use one of "
                f"{', '.join(MERGE_WRITE_STRATEGIES)}, tag the entity write.mode: insert "
                f"for {INSERT_WRITE_STRATEGY}, or call append_to_entity"
            )
        return allowed[explicit_name.lower()]

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

    def _save(
        self,
        df: DataFrame,
        entity_metadata: EntityMetadata,
        write_strategy: Optional[str] = None,
    ) -> None:
        config = self._get_provider_config(entity_metadata)
        options = self._build_write_options(entity_metadata, config, write_strategy)
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
        # Named read tags first, connection options (which end with the
        # provider.option.* passthrough) last, so the passthrough always wins.
        options: Dict[str, Any] = {
            "spark.cosmos.read.inferSchema.enabled": config.get("infer_schema", True),
        }
        query = config.get("query") or config.get("custom_query")
        if query:
            options["spark.cosmos.read.customQuery"] = query
        options.update(self._connection_options(entity_metadata, config))
        return self._stringify_options(options)

    def _build_changefeed_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, str]:
        # Change-feed defaults are laid down first so that the connection
        # options -- which end with the generic provider.option.* passthrough
        # -- keep precedence: an explicit
        # provider.option.spark.cosmos.changeFeed.startFrom still wins.
        options: Dict[str, Any] = self._changefeed_options(entity_metadata, config)
        options["spark.cosmos.read.inferSchema.enabled"] = config.get("infer_schema", True)
        options.update(self._connection_options(entity_metadata, config))
        return self._stringify_options(options)

    def _changefeed_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        issues = self._changefeed_issues(config)
        if issues:
            raise ValueError(
                f"Cosmos entity '{entity_metadata.entityid}' change-feed configuration is "
                "invalid: " + "; ".join(str(issue) for issue in issues)
            )

        mode = str(config.get("changefeed.mode", "latest_version")).strip().lower()
        options: Dict[str, Any] = {
            "spark.cosmos.changeFeed.mode": CHANGEFEED_MODES[mode],
            "spark.cosmos.changeFeed.startFrom": str(
                config.get("changefeed.start_from", "Beginning")
            ).strip(),
        }
        items_per_trigger = config.get("changefeed.items_per_trigger")
        if items_per_trigger is not None and str(items_per_trigger).strip() != "":
            options["spark.cosmos.changeFeed.itemCountPerTriggerHint"] = int(items_per_trigger)
        return options

    def _changefeed_issues(self, config: Mapping[str, Any]) -> list:
        issues = []

        mode = str(config.get("changefeed.mode", "latest_version")).strip().lower()
        if mode not in CHANGEFEED_MODES:
            issues.append(
                SourceValidationIssue(
                    tag="provider.changefeed.mode",
                    constraint=f"must be one of: {', '.join(sorted(CHANGEFEED_MODES))}",
                    remediation=(
                        "use latest_version (default), or full_fidelity for a container "
                        "provisioned for all-versions-and-deletes change feed"
                    ),
                )
            )

        start_from = str(config.get("changefeed.start_from", "Beginning")).strip()
        if start_from.lower() not in {
            keyword.lower() for keyword in CHANGEFEED_START_FROM_KEYWORDS
        } and not CHANGEFEED_START_FROM_INSTANT.match(start_from):
            issues.append(
                SourceValidationIssue(
                    tag="provider.changefeed.start_from",
                    constraint=(
                        "must be Beginning, Now, or a UTC instant with a time component "
                        "and Z suffix (the connector parses it with ISO_INSTANT)"
                    ),
                    remediation="e.g. Beginning, Now, 2026-01-31T00:00:00Z",
                )
            )

        items_per_trigger = config.get("changefeed.items_per_trigger")
        if items_per_trigger is not None and str(items_per_trigger).strip() != "":
            try:
                if int(str(items_per_trigger)) <= 0:
                    raise ValueError
            except (TypeError, ValueError):
                issues.append(
                    SourceValidationIssue(
                        tag="provider.changefeed.items_per_trigger",
                        constraint="must be a positive integer",
                        remediation="remove it or set the approximate items per micro-batch",
                    )
                )
        return issues

    def _auth_issues(self, config: Mapping[str, Any]) -> List[SourceValidationIssue]:
        """Secret-safe auth completeness check (tag names only, never values)."""
        auth_mode = self._auth_mode(config)
        if auth_mode in SERVICE_PRINCIPAL_AUTH_MODES:
            return [
                SourceValidationIssue(
                    tag=f"provider.{tag}",
                    constraint="is required for service_principal auth",
                    remediation="set it, via a secret-backed tag where sensitive",
                )
                for _, tag, aliases in SERVICE_PRINCIPAL_CREDENTIALS
                if not self._credential(config, tag, aliases)
            ]
        if auth_mode in MASTER_KEY_AUTH_MODES:
            _, tag, aliases = MASTER_KEY_CREDENTIAL
            if self._credential(config, tag, aliases):
                return []
            return [
                SourceValidationIssue(
                    tag=f"provider.{tag}",
                    constraint="is required for master_key auth",
                    remediation="provide the account key through a secret-backed tag",
                )
            ]
        return [
            SourceValidationIssue(
                tag="provider.auth",
                constraint="must be one of: service_principal, master_key",
            )
        ]

    @staticmethod
    def _auth_mode(config: Mapping[str, Any]) -> str:
        return str(config.get("auth", config.get("auth_mode", "service_principal"))).lower()

    @staticmethod
    def _credential(config: Mapping[str, Any], tag: str, aliases: Tuple[str, ...]) -> Any:
        for name in (tag, *aliases):
            value = config.get(name)
            if value:
                return value
        return None

    def _build_write_options(
        self,
        entity_metadata: EntityMetadata,
        config: Dict[str, Any],
        write_strategy: Optional[str] = None,
    ) -> Dict[str, str]:
        options: Dict[str, Any] = {
            "spark.cosmos.write.strategy": write_strategy
            or config.get("write_strategy", MERGE_WRITE_STRATEGIES[0]),
        }
        options.update(self._connection_options(entity_metadata, config))
        return self._stringify_options(options)

    def _connection_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Endpoint, database/container, run-level defaults, auth, and passthrough.

        Precedence, lowest to highest: ``kindling.cosmos.*`` run-level
        defaults, then auth, then per-entity ``provider.option.*`` (applied
        last, so it always wins).
        """
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
        options.update(self._throughput_defaults())
        options.update(self._auth_options(entity_metadata, config))
        options.update(self._extra_connector_options(config))
        return options

    def connector_maven_coordinate(self, spark_version: Optional[str] = None) -> str:
        """Maven coordinate of the connector artifact this runtime needs.

        Resolves from the given Spark version, else the installed ``pyspark``
        (no Spark session is consulted). Useful for deployment tooling and
        diagnostics — the wheel does not install the JVM artifact. See
        :func:`resolve_cosmos_spark_connector_coordinate`.
        """
        return resolve_cosmos_spark_connector_coordinate(spark_version)

    # ---- kindling.cosmos.* run-level defaults ----

    def _config(self, key: str, default: Any = None) -> Any:
        # A missing key returns ``default`` from ConfigService.get itself; any
        # exception here is a real configuration failure and must surface
        # rather than silently disable, say, throughput control.
        if self.config_service is None:
            return default
        value = self.config_service.get(f"{CONFIG_PREFIX}.{key}", default)
        return default if value is None else value

    def _throughput_defaults(self) -> Dict[str, Any]:
        """Connector options sourced from ``kindling.cosmos.*``.

        Read partitioning strategy and page size always get an explicit value
        (the connector's own defaults unless configured). Throughput control
        is off unless ``kindling.cosmos.throughput_control.enabled`` is set;
        when it is, ``group_name`` and exactly one of ``target_threshold``
        (fraction of provisioned/autoscale RU/s, in (0, 1]) or
        ``target_throughput`` (absolute RU/s) must be supplied. A
        ``global_control`` database and container pair coordinates the budget
        across jobs through a shared control container; without one the
        connector's dedicated-container mode is switched off and the budget
        is split evenly across executors.
        """
        strategy = str(
            self._config("read.partitioning_strategy", DEFAULT_READ_PARTITIONING_STRATEGY)
        ).strip()
        canonical = {name.lower(): name for name in READ_PARTITIONING_STRATEGIES}
        if strategy.lower() not in canonical:
            raise ValueError(
                f"{CONFIG_PREFIX}.read.partitioning_strategy must be one of "
                f"{', '.join(READ_PARTITIONING_STRATEGIES)}; got '{strategy}'"
            )
        max_item_count = self._config("read.max_item_count", DEFAULT_READ_MAX_ITEM_COUNT)
        try:
            max_item_count = int(str(max_item_count))
            if max_item_count <= 0:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError(
                f"{CONFIG_PREFIX}.read.max_item_count must be a positive integer; "
                f"got '{max_item_count}'"
            ) from None

        defaults: Dict[str, Any] = {
            "spark.cosmos.read.partitioning.strategy": canonical[strategy.lower()],
            "spark.cosmos.read.maxItemCount": max_item_count,
        }

        enabled_raw = self._config("throughput_control.enabled", False)
        enabled = _coerce_bool(enabled_raw)
        if enabled is None:
            raise ValueError(
                f"{CONFIG_PREFIX}.throughput_control.enabled must be a boolean; "
                f"got '{enabled_raw}'"
            )
        if not enabled:
            return defaults

        defaults["spark.cosmos.throughputControl.enabled"] = True
        # The connector asserts the group name is present whenever throughput
        # control is enabled; fail here with the Kindling key rather than in
        # the JVM at query start.
        group_name = self._config("throughput_control.group_name")
        if not group_name or not str(group_name).strip():
            raise ValueError(
                f"{CONFIG_PREFIX}.throughput_control.enabled requires "
                f"{CONFIG_PREFIX}.throughput_control.group_name"
            )
        defaults["spark.cosmos.throughputControl.name"] = str(group_name).strip()

        threshold = self._config("throughput_control.target_threshold")
        target = self._config("throughput_control.target_throughput")
        # Alternatives, not companions: the connector cannot honour both.
        if threshold is not None and target is not None:
            raise ValueError(
                f"Set only one of {CONFIG_PREFIX}.throughput_control.target_threshold "
                f"and {CONFIG_PREFIX}.throughput_control.target_throughput"
            )
        if threshold is None and target is None:
            raise ValueError(
                f"{CONFIG_PREFIX}.throughput_control.enabled requires "
                f"{CONFIG_PREFIX}.throughput_control.target_threshold or .target_throughput"
            )
        if threshold is not None:
            try:
                threshold_value = float(threshold)
            except (TypeError, ValueError):
                raise ValueError(
                    f"{CONFIG_PREFIX}.throughput_control.target_threshold must be a number "
                    f"in (0, 1]; got '{threshold}'"
                ) from None
            if not 0 < threshold_value <= 1:
                raise ValueError(
                    f"{CONFIG_PREFIX}.throughput_control.target_threshold must be in (0, 1]; "
                    f"got {threshold}"
                )
            defaults["spark.cosmos.throughputControl.targetThroughputThreshold"] = threshold_value
        if target is not None:
            try:
                target_value = int(str(target))
                if target_value <= 0:
                    raise ValueError
            except (TypeError, ValueError):
                raise ValueError(
                    f"{CONFIG_PREFIX}.throughput_control.target_throughput must be a positive "
                    f"integer RU/s; got '{target}'"
                ) from None
            defaults["spark.cosmos.throughputControl.targetThroughput"] = target_value

        db = self._config("throughput_control.global_control.database")
        container = self._config("throughput_control.global_control.container")
        if bool(db) != bool(container):
            raise ValueError(
                f"{CONFIG_PREFIX}.throughput_control.global_control requires both "
                "database and container"
            )
        if db and container:
            defaults["spark.cosmos.throughputControl.globalControl.database"] = str(db)
            defaults["spark.cosmos.throughputControl.globalControl.container"] = str(container)
        else:
            # The connector defaults to a dedicated global-control container
            # and rejects the config when none is named. Without one, fall
            # back to the connector's local mode: the budget is split evenly
            # across executors instead of coordinated through a container.
            defaults["spark.cosmos.throughputControl.globalControl.useDedicatedContainer"] = False
        return defaults

    def _auth_options(
        self, entity_metadata: EntityMetadata, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        auth_mode = self._auth_mode(config)

        if auth_mode in SERVICE_PRINCIPAL_AUTH_MODES:
            options: Dict[str, Any] = {
                option: self._credential(config, tag, aliases)
                for option, tag, aliases in SERVICE_PRINCIPAL_CREDENTIALS
            }
            missing = [option for option, value in options.items() if not value]
            if missing:
                raise ValueError(
                    f"Cosmos entity '{entity_metadata.entityid}' service principal auth "
                    f"is missing options: {', '.join(missing)}"
                )
            options["spark.cosmos.auth.type"] = "ServicePrincipal"
            return options

        if auth_mode in MASTER_KEY_AUTH_MODES:
            option, tag, aliases = MASTER_KEY_CREDENTIAL
            account_key = self._credential(config, tag, aliases)
            if not account_key:
                raise ValueError(
                    f"Cosmos entity '{entity_metadata.entityid}' uses master_key auth "
                    f"but provider.{tag} is not set"
                )
            return {option: account_key}

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


def spark_family(spark_version: str) -> str:
    """Reduce a Spark version string ("3.5.5", "4.0.1-databricks") to "major.minor"."""
    parts = str(spark_version).strip().split(".")
    if len(parts) < 2 or not parts[0].isdigit() or not parts[1].split("-")[0].isdigit():
        raise ValueError(f"Unrecognised Spark version '{spark_version}'")
    return f"{parts[0]}.{parts[1].split('-')[0]}"


def resolve_cosmos_spark_connector_coordinate(spark_version: Optional[str] = None) -> str:
    """Return the Cosmos Spark connector Maven coordinate for a Spark line.

    ``spark_version`` defaults to the installed ``pyspark`` version: on a
    managed runtime that is the runtime's Spark, and in a standalone
    environment it is whichever family the ``spark_3_x`` / ``spark_4_x``
    extra pinned at install time. No Spark session is consulted or created.
    Raises ``ValueError`` naming the supported Spark lines when the version
    has no published connector artifact in this extension's table.
    """
    if spark_version is None:
        import pyspark

        spark_version = str(pyspark.__version__)
    family = spark_family(spark_version)
    try:
        return COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES[family]
    except KeyError:
        supported = ", ".join(sorted(COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES))
        raise ValueError(
            f"No Cosmos Spark connector coordinate for Spark {spark_version} "
            f"(family {family}); supported Spark lines: {supported}"
        ) from None


def register_provider(provider_type: str = "cosmos") -> None:
    """Register the Cosmos provider with Kindling's entity provider registry."""
    registry = GlobalInjector.get(EntityProviderRegistry)
    registry.register_provider(provider_type, CosmosEntityProvider)
