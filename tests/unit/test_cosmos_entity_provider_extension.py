import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from kindling.data_entities import EntityMetadata

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_cosmos"
)


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


def _entity(tags):
    return EntityMetadata(
        entityid="gold.orders",
        name="orders",
        partition_columns=[],
        merge_columns=[],
        tags={"provider_type": "cosmos", **tags},
        schema=None,
    )


BASE_TAGS = {
    "provider.auth": "service_principal",
    "provider.account_endpoint": "https://fawkes.documents.azure.com:443/",
    "provider.database": "Kindling",
    "provider.container": "Orders",
    "provider.client_id": "client-id",
    "provider.client_secret": "client-secret",
    "provider.tenant_id": "tenant-id",
    "provider.subscription_id": "sub-id",
    "provider.resource_group": "rg-name",
}


class _Writer:
    def __init__(self):
        self.format_name = None
        self.options = {}
        self.mode_name = None
        self.saved = False

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def mode(self, name):
        self.mode_name = name
        return self

    def save(self):
        self.saved = True


class _StreamWriter:
    def __init__(self):
        self.format_name = None
        self.options = {}
        self.output_mode = None
        self.query_name = None
        self.started = False
        self.query = MagicMock(name="streaming_query")

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def outputMode(self, name):
        self.output_mode = name
        return self

    def queryName(self, name):
        self.query_name = name
        return self

    def start(self):
        self.started = True
        return self.query


class _Reader:
    def __init__(self):
        self.format_name = None
        self.options = {}
        self.loaded_df = MagicMock(name="read_df")

    def format(self, name):
        self.format_name = name
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def load(self):
        return self.loaded_df


class _StreamReader(_Reader):
    def __init__(self):
        super().__init__()
        self.loaded_df = MagicMock(name="stream_df")


class _ConfigService:
    """Dict-backed stand-in for ConfigService.get(key, default)."""

    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)


def _provider(config=None):
    from kindling_ext_cosmos import CosmosEntityProvider

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    config_service = None if config is None else _ConfigService(config)
    return CosmosEntityProvider(logger_provider, config_service)


def _patched_spark_read(reader):
    spark = MagicMock()
    spark.read = reader
    return patch(
        "kindling_ext_cosmos.entity_provider_cosmos.get_or_create_spark_session",
        return_value=spark,
    )


def _patched_spark_read_stream(reader):
    spark = MagicMock()
    spark.readStream = reader
    return patch(
        "kindling_ext_cosmos.entity_provider_cosmos.get_or_create_spark_session",
        return_value=spark,
    )


def test_import_registers_cosmos_provider():
    for module_name in list(sys.modules):
        if module_name == "kindling_ext_cosmos" or module_name.startswith("kindling_ext_cosmos."):
            del sys.modules[module_name]

    registry = MagicMock()

    with patch("kindling.injection.GlobalInjector.get", return_value=registry):
        import kindling_ext_cosmos  # noqa: F401

    provider_class = registry.register_provider.call_args.args[1]
    assert registry.register_provider.call_args.args[0] == "cosmos"
    assert provider_class.__name__ == "CosmosEntityProvider"


def test_write_upserts_with_service_principal_options():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer

    provider.write_to_entity(df, _entity(BASE_TAGS))

    assert writer.format_name == "cosmos.oltp"
    assert writer.options["spark.cosmos.accountEndpoint"] == (
        "https://fawkes.documents.azure.com:443/"
    )
    assert writer.options["spark.cosmos.database"] == "Kindling"
    assert writer.options["spark.cosmos.container"] == "Orders"
    assert writer.options["spark.cosmos.auth.type"] == "ServicePrincipal"
    assert writer.options["spark.cosmos.auth.aad.clientId"] == "client-id"
    assert writer.options["spark.cosmos.auth.aad.clientSecret"] == "client-secret"
    assert writer.options["spark.cosmos.account.tenantId"] == "tenant-id"
    assert writer.options["spark.cosmos.account.subscriptionId"] == "sub-id"
    assert writer.options["spark.cosmos.account.resourceGroupName"] == "rg-name"
    assert writer.options["spark.cosmos.write.strategy"] == "ItemOverwrite"
    assert writer.mode_name == "Append"
    assert writer.saved is True


def test_write_strategy_is_configurable():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer

    provider.append_to_entity(df, _entity({**BASE_TAGS, "provider.write_strategy": "ItemAppend"}))

    assert writer.options["spark.cosmos.write.strategy"] == "ItemAppend"


def test_master_key_auth():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer
    tags = {
        "provider.auth": "master_key",
        "provider.account_endpoint": "https://fawkes.documents.azure.com:443/",
        "provider.database": "Kindling",
        "provider.container": "Orders",
        "provider.account_key": "s3cret==",
    }

    provider.write_to_entity(df, _entity(tags))

    assert writer.options["spark.cosmos.accountKey"] == "s3cret=="
    assert "spark.cosmos.auth.type" not in writer.options


def test_service_principal_requires_all_auth_fields():
    provider = _provider()
    df = MagicMock()
    df.write = _Writer()
    tags = {key: value for key, value in BASE_TAGS.items() if key != "provider.client_secret"}

    with pytest.raises(ValueError, match="clientSecret"):
        provider.write_to_entity(df, _entity(tags))


def test_container_is_required():
    provider = _provider()
    df = MagicMock()
    df.write = _Writer()
    tags = {key: value for key, value in BASE_TAGS.items() if key != "provider.container"}

    with pytest.raises(ValueError, match="provider.container"):
        provider.write_to_entity(df, _entity(tags))


def test_extra_connector_options_are_passed_through():
    provider = _provider()
    writer = _Writer()
    df = MagicMock()
    df.write = writer

    provider.write_to_entity(
        df,
        _entity({**BASE_TAGS, "provider.option.spark.cosmos.write.bulk.enabled": "false"}),
    )

    assert writer.options["spark.cosmos.write.bulk.enabled"] == "false"


def test_read_entity_scans_container_with_inferred_schema():
    provider = _provider()
    reader = _Reader()

    with _patched_spark_read(reader):
        df = provider.read_entity(_entity(BASE_TAGS))

    assert df is reader.loaded_df
    assert reader.format_name == "cosmos.oltp"
    assert reader.options["spark.cosmos.container"] == "Orders"
    assert reader.options["spark.cosmos.read.inferSchema.enabled"] == "true"
    assert reader.options["spark.cosmos.auth.type"] == "ServicePrincipal"
    # Write-only options must not leak into reads
    assert "spark.cosmos.write.strategy" not in reader.options


def test_read_entity_uses_custom_query():
    provider = _provider()
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(
            _entity({**BASE_TAGS, "provider.query": "SELECT c.id FROM c WHERE c.amount > 15"})
        )

    assert reader.options["spark.cosmos.read.customQuery"] == (
        "SELECT c.id FROM c WHERE c.amount > 15"
    )


def test_read_infer_schema_can_be_disabled():
    provider = _provider()
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(_entity({**BASE_TAGS, "provider.infer_schema": "false"}))

    assert reader.options["spark.cosmos.read.inferSchema.enabled"] == "false"


def test_stream_append_starts_query_with_cosmos_sink_options():
    provider = _provider()
    writer = _StreamWriter()
    df = MagicMock()
    df.writeStream = writer

    query = provider.append_as_stream(
        df, _entity({**BASE_TAGS, "provider.query_name": "orders_to_cosmos"}), "/chk/orders"
    )

    assert query is writer.query
    assert writer.format_name == "cosmos.oltp"
    assert writer.output_mode == "append"
    assert writer.query_name == "orders_to_cosmos"
    assert writer.options["checkpointLocation"] == "/chk/orders"
    assert writer.options["spark.cosmos.container"] == "Orders"
    assert writer.options["spark.cosmos.write.strategy"] == "ItemOverwrite"
    assert writer.started is True


def test_check_entity_exists_defaults_to_true():
    assert _provider().check_entity_exists(_entity(BASE_TAGS)) is True


def test_unsupported_auth_mode_raises():
    provider = _provider()
    df = MagicMock()
    df.write = _Writer()

    with pytest.raises(ValueError, match="Unsupported Cosmos auth mode"):
        provider.write_to_entity(df, _entity({**BASE_TAGS, "provider.auth": "magic"}))


# ---------------------------------------------------------------------------
# Capability surface
# ---------------------------------------------------------------------------


def test_provider_declares_streaming_source_capabilities():
    from kindling.entity_provider import (
        is_declarable_streaming_source,
        is_stream_writable,
        is_streamable,
        is_writable,
    )

    provider = _provider()

    assert is_streamable(provider)
    assert is_declarable_streaming_source(provider)
    assert is_writable(provider)
    assert is_stream_writable(provider)
    # The persist path selects merge via hasattr(provider, "merge_to_entity").
    assert hasattr(provider, "merge_to_entity")


# ---------------------------------------------------------------------------
# merge_to_entity
# ---------------------------------------------------------------------------


def _df_with_columns(*columns):
    df = MagicMock()
    df.columns = list(columns)
    df.write = _Writer()
    return df


def test_merge_to_entity_upserts_by_id():
    provider = _provider()
    df = _df_with_columns("id", "name")

    provider.merge_to_entity(df, _entity(BASE_TAGS))

    assert df.write.format_name == "cosmos.oltp"
    assert df.write.options["spark.cosmos.write.strategy"] == "ItemOverwrite"
    assert df.write.mode_name == "Append"
    assert df.write.saved is True


@pytest.mark.parametrize("columns", [("order_key", "name"), ("Id", "name"), ("ID",)])
def test_merge_to_entity_requires_exact_id_column(columns):
    provider = _provider()
    df = _df_with_columns(*columns)

    with pytest.raises(ValueError, match="'id' column"):
        provider.merge_to_entity(df, _entity(BASE_TAGS))

    assert df.write.saved is False


@pytest.mark.parametrize("strategy", ["ItemAppend", "ItemDelete"])
def test_merge_to_entity_rejects_non_merge_write_strategy(strategy):
    provider = _provider()
    df = _df_with_columns("id")

    with pytest.raises(ValueError, match="cannot merge"):
        provider.merge_to_entity(df, _entity({**BASE_TAGS, "provider.write_strategy": strategy}))

    assert df.write.saved is False


@pytest.mark.parametrize("strategy", ["ItemOverwriteIfNotModified", "ItemPatch"])
def test_merge_to_entity_accepts_other_upsert_strategies(strategy):
    provider = _provider()
    df = _df_with_columns("id")

    provider.merge_to_entity(df, _entity({**BASE_TAGS, "provider.write_strategy": strategy}))

    assert df.write.options["spark.cosmos.write.strategy"] == strategy


def test_merge_to_entity_insert_mode_uses_item_append():
    # The persist path routes write.mode=insert to merge_to_entity; Delta and
    # memory only add new keys in that mode, so Cosmos must insert-if-absent
    # rather than silently upsert.
    provider = _provider()
    df = _df_with_columns("id", "name")

    provider.merge_to_entity(df, _entity({**BASE_TAGS, "write.mode": "insert"}))

    assert df.write.options["spark.cosmos.write.strategy"] == "ItemAppend"
    assert df.write.saved is True


def test_merge_to_entity_insert_mode_accepts_explicit_item_append():
    provider = _provider()
    df = _df_with_columns("id")

    provider.merge_to_entity(
        df,
        _entity({**BASE_TAGS, "write.mode": "insert", "provider.write_strategy": "ItemAppend"}),
    )

    assert df.write.options["spark.cosmos.write.strategy"] == "ItemAppend"


def test_merge_to_entity_insert_mode_rejects_conflicting_strategy():
    provider = _provider()
    df = _df_with_columns("id")

    with pytest.raises(ValueError, match="write.mode 'insert'"):
        provider.merge_to_entity(
            df,
            _entity(
                {**BASE_TAGS, "write.mode": "insert", "provider.write_strategy": "ItemOverwrite"}
            ),
        )

    assert df.write.saved is False


def test_merge_to_entity_merge_mode_upserts():
    provider = _provider()
    df = _df_with_columns("id")

    provider.merge_to_entity(df, _entity({**BASE_TAGS, "write.mode": "merge"}))

    assert df.write.options["spark.cosmos.write.strategy"] == "ItemOverwrite"


def test_append_to_entity_ignores_write_mode():
    # Only the merge path interprets write.mode; append keeps the explicit or
    # default strategy exactly as before.
    provider = _provider()
    df = MagicMock()
    df.write = _Writer()

    provider.append_to_entity(df, _entity({**BASE_TAGS, "write.mode": "insert"}))

    assert df.write.options["spark.cosmos.write.strategy"] == "ItemOverwrite"


# ---------------------------------------------------------------------------
# read_entity_as_stream (change feed)
# ---------------------------------------------------------------------------


def test_stream_read_uses_change_feed_source_with_defaults():
    provider = _provider()
    reader = _StreamReader()

    with _patched_spark_read_stream(reader):
        df = provider.read_entity_as_stream(_entity(BASE_TAGS))

    assert df is reader.loaded_df
    assert reader.format_name == "cosmos.oltp.changeFeed"
    assert reader.options["spark.cosmos.changeFeed.mode"] == "LatestVersion"
    assert reader.options["spark.cosmos.changeFeed.startFrom"] == "Beginning"
    assert "spark.cosmos.changeFeed.itemCountPerTriggerHint" not in reader.options
    assert reader.options["spark.cosmos.container"] == "Orders"
    assert reader.options["spark.cosmos.auth.type"] == "ServicePrincipal"
    assert reader.options["spark.cosmos.read.inferSchema.enabled"] == "true"
    # Write-only and batch-only options must not leak into the stream read
    assert "spark.cosmos.write.strategy" not in reader.options
    assert "spark.cosmos.read.customQuery" not in reader.options
    # Nothing checkpoint-like on the read side: Spark owns continuation tokens
    assert not any("checkpoint" in key.lower() for key in reader.options)


def test_stream_read_full_fidelity_with_start_from_and_items_per_trigger():
    provider = _provider()
    reader = _StreamReader()

    with _patched_spark_read_stream(reader):
        provider.read_entity_as_stream(
            _entity(
                {
                    **BASE_TAGS,
                    "provider.changefeed.mode": "full_fidelity",
                    "provider.changefeed.start_from": "2026-01-31T00:00:00Z",
                    "provider.changefeed.items_per_trigger": "5000",
                }
            )
        )

    assert reader.options["spark.cosmos.changeFeed.mode"] == "AllVersionsAndDeletes"
    assert reader.options["spark.cosmos.changeFeed.startFrom"] == "2026-01-31T00:00:00Z"
    assert reader.options["spark.cosmos.changeFeed.itemCountPerTriggerHint"] == "5000"


def test_stream_read_start_from_now():
    provider = _provider()
    reader = _StreamReader()

    with _patched_spark_read_stream(reader):
        provider.read_entity_as_stream(
            _entity({**BASE_TAGS, "provider.changefeed.start_from": "Now"})
        )

    assert reader.options["spark.cosmos.changeFeed.startFrom"] == "Now"


def test_stream_read_options_override_tags_and_declarative_marker_is_dropped():
    from kindling.entity_provider import DECLARATIVE_SOURCE_OPTION

    provider = _provider()
    reader = _StreamReader()

    with _patched_spark_read_stream(reader):
        provider.read_entity_as_stream(
            _entity({**BASE_TAGS, "provider.changefeed.start_from": "Beginning"}),
            options={"changefeed.start_from": "Now", DECLARATIVE_SOURCE_OPTION: True},
        )

    assert reader.options["spark.cosmos.changeFeed.startFrom"] == "Now"
    assert DECLARATIVE_SOURCE_OPTION not in reader.options


def test_stream_read_provider_option_passthrough_wins_over_changefeed_defaults():
    provider = _provider()
    reader = _StreamReader()

    with _patched_spark_read_stream(reader):
        provider.read_entity_as_stream(
            _entity(
                {
                    **BASE_TAGS,
                    "provider.option.spark.cosmos.changeFeed.startFrom": "Now",
                    "provider.option.spark.cosmos.changeFeed.mode": "Incremental",
                }
            )
        )

    assert reader.options["spark.cosmos.changeFeed.startFrom"] == "Now"
    assert reader.options["spark.cosmos.changeFeed.mode"] == "Incremental"


def test_provider_option_passthrough_wins_over_named_read_tags():
    provider = _provider()

    reader = _Reader()
    with _patched_spark_read(reader):
        provider.read_entity(
            _entity(
                {
                    **BASE_TAGS,
                    "provider.query": "SELECT c.id FROM c",
                    "provider.option.spark.cosmos.read.inferSchema.enabled": "false",
                    "provider.option.spark.cosmos.read.customQuery": "SELECT * FROM c",
                }
            )
        )
    assert reader.options["spark.cosmos.read.inferSchema.enabled"] == "false"
    assert reader.options["spark.cosmos.read.customQuery"] == "SELECT * FROM c"

    stream_reader = _StreamReader()
    with _patched_spark_read_stream(stream_reader):
        provider.read_entity_as_stream(
            _entity({**BASE_TAGS, "provider.option.spark.cosmos.read.inferSchema.enabled": "false"})
        )
    assert stream_reader.options["spark.cosmos.read.inferSchema.enabled"] == "false"

    df = MagicMock()
    df.write = _Writer()
    provider.write_to_entity(
        df, _entity({**BASE_TAGS, "provider.option.spark.cosmos.write.strategy": "ItemPatch"})
    )
    assert df.write.options["spark.cosmos.write.strategy"] == "ItemPatch"


def test_stream_read_rejects_unknown_mode():
    provider = _provider()

    with pytest.raises(ValueError, match="provider.changefeed.mode"):
        provider.read_entity_as_stream(
            _entity({**BASE_TAGS, "provider.changefeed.mode": "everything"})
        )


@pytest.mark.parametrize(
    "start_from",
    [
        "yesterday",
        "2026-01-31",  # date only
        "2026-01-31T00:00:00",  # zone-naive
        "2026-01-31T00:00:00+02:00",  # non-UTC offset
        "2026-01-31T00:00:00+00:00",  # UTC offset spelled out; connector wants Z
        "2026-01-31 00:00:00Z",  # space separator
    ],
)
def test_stream_read_rejects_start_from_the_connector_cannot_parse(start_from):
    # The connector parses non-keyword values with DateTimeFormatter.ISO_INSTANT,
    # so only the Z-suffixed instant form must pass validation.
    provider = _provider()

    with pytest.raises(ValueError, match="provider.changefeed.start_from"):
        provider.read_entity_as_stream(
            _entity({**BASE_TAGS, "provider.changefeed.start_from": start_from})
        )


@pytest.mark.parametrize(
    "start_from", ["2026-01-31T00:00:00Z", "2026-01-31T00:00:00.123Z", "beginning", "NOW"]
)
def test_stream_read_accepts_iso_instant_and_keywords(start_from):
    provider = _provider()
    reader = _StreamReader()

    with _patched_spark_read_stream(reader):
        provider.read_entity_as_stream(
            _entity({**BASE_TAGS, "provider.changefeed.start_from": start_from})
        )

    assert reader.options["spark.cosmos.changeFeed.startFrom"] == start_from


def test_stream_read_rejects_non_positive_items_per_trigger():
    provider = _provider()

    with pytest.raises(ValueError, match="provider.changefeed.items_per_trigger"):
        provider.read_entity_as_stream(
            _entity({**BASE_TAGS, "provider.changefeed.items_per_trigger": "0"})
        )


def test_stream_read_rejects_foreign_format():
    provider = _provider()

    with pytest.raises(ValueError, match="cosmos.oltp.changeFeed"):
        provider.read_entity_as_stream(_entity(BASE_TAGS), format="cosmos.oltp")


def test_stream_read_accepts_explicit_change_feed_format():
    provider = _provider()
    reader = _StreamReader()

    with _patched_spark_read_stream(reader):
        provider.read_entity_as_stream(_entity(BASE_TAGS), format="cosmos.oltp.changeFeed")

    assert reader.format_name == "cosmos.oltp.changeFeed"


# ---------------------------------------------------------------------------
# streaming_source_spec (declarative, inert, secret-safe)
# ---------------------------------------------------------------------------


def test_streaming_source_spec_is_valid_and_secret_safe():
    provider = _provider()

    with patch(
        "kindling_ext_cosmos.entity_provider_cosmos.get_or_create_spark_session"
    ) as spark_factory:
        spec = provider.streaming_source_spec(
            _entity({**BASE_TAGS, "provider.changefeed.mode": "full_fidelity"})
        )

    spark_factory.assert_not_called()
    assert spec.is_valid
    assert spec.provider_type == "cosmos"
    assert spec.source_format == "cosmos.oltp.changeFeed"
    assert spec.source_identity == "Kindling/Orders"
    assert "provider.changefeed.mode" in spec.supported_option_names
    assert "provider.client_secret" in spec.applied_option_names
    rendered = repr(spec)
    assert "client-secret" not in rendered
    assert "fawkes.documents.azure.com" not in rendered


def test_streaming_source_spec_reports_missing_and_invalid_tags():
    provider = _provider()
    tags = {key: value for key, value in BASE_TAGS.items() if key != "provider.client_secret"}
    tags.pop("provider.container")
    tags["provider.changefeed.mode"] = "everything"

    spec = provider.streaming_source_spec(_entity(tags))

    assert not spec.is_valid
    issue_tags = {issue.tag for issue in spec.validation_issues}
    assert issue_tags == {
        "provider.container",
        "provider.client_secret",
        "provider.changefeed.mode",
    }


def test_streaming_source_spec_master_key_requires_account_key():
    provider = _provider()
    tags = {
        "provider.auth": "master_key",
        "provider.account_endpoint": "https://fawkes.documents.azure.com:443/",
        "provider.database": "Kindling",
        "provider.container": "Orders",
    }

    spec = provider.streaming_source_spec(_entity(tags))

    assert [issue.tag for issue in spec.validation_issues] == ["provider.account_key"]


# ---------------------------------------------------------------------------
# kindling.cosmos.* throughput defaults
# ---------------------------------------------------------------------------


def test_read_pins_connector_defaults_without_config_service():
    provider = _provider()
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(_entity(BASE_TAGS))

    assert reader.options["spark.cosmos.read.partitioning.strategy"] == "Default"
    assert reader.options["spark.cosmos.read.maxItemCount"] == "1000"
    assert "spark.cosmos.throughputControl.enabled" not in reader.options


def test_read_pins_connector_defaults_with_empty_config():
    provider = _provider(config={})
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(_entity(BASE_TAGS))

    assert reader.options["spark.cosmos.read.partitioning.strategy"] == "Default"
    assert reader.options["spark.cosmos.read.maxItemCount"] == "1000"
    assert "spark.cosmos.throughputControl.enabled" not in reader.options


def test_config_read_tuning_is_applied_to_reads_writes_and_streams():
    provider = _provider(
        config={
            "kindling.cosmos.read.partitioning_strategy": "restrictive",
            "kindling.cosmos.read.max_item_count": "250",
        }
    )

    reader = _Reader()
    with _patched_spark_read(reader):
        provider.read_entity(_entity(BASE_TAGS))
    assert reader.options["spark.cosmos.read.partitioning.strategy"] == "Restrictive"
    assert reader.options["spark.cosmos.read.maxItemCount"] == "250"

    stream_reader = _StreamReader()
    with _patched_spark_read_stream(stream_reader):
        provider.read_entity_as_stream(_entity(BASE_TAGS))
    assert stream_reader.options["spark.cosmos.read.maxItemCount"] == "250"

    df = MagicMock()
    df.write = _Writer()
    provider.write_to_entity(df, _entity(BASE_TAGS))
    assert df.write.options["spark.cosmos.read.maxItemCount"] == "250"


def test_config_invalid_partitioning_strategy_raises():
    provider = _provider(config={"kindling.cosmos.read.partitioning_strategy": "Turbo"})

    with pytest.raises(ValueError, match="partitioning_strategy"):
        with _patched_spark_read(_Reader()):
            provider.read_entity(_entity(BASE_TAGS))


def test_config_invalid_max_item_count_raises():
    provider = _provider(config={"kindling.cosmos.read.max_item_count": "lots"})

    with pytest.raises(ValueError, match="max_item_count"):
        with _patched_spark_read(_Reader()):
            provider.read_entity(_entity(BASE_TAGS))


def test_throughput_control_with_threshold():
    provider = _provider(
        config={
            "kindling.cosmos.throughput_control.enabled": "true",
            "kindling.cosmos.throughput_control.group_name": "kindling-etl",
            "kindling.cosmos.throughput_control.target_threshold": "0.9",
            "kindling.cosmos.throughput_control.global_control.database": "ThroughputDb",
            "kindling.cosmos.throughput_control.global_control.container": "ThroughputCtl",
        }
    )
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(_entity(BASE_TAGS))

    assert reader.options["spark.cosmos.throughputControl.enabled"] == "true"
    assert reader.options["spark.cosmos.throughputControl.name"] == "kindling-etl"
    assert reader.options["spark.cosmos.throughputControl.targetThroughputThreshold"] == "0.9"
    assert "spark.cosmos.throughputControl.targetThroughput" not in reader.options
    assert reader.options["spark.cosmos.throughputControl.globalControl.database"] == (
        "ThroughputDb"
    )
    assert reader.options["spark.cosmos.throughputControl.globalControl.container"] == (
        "ThroughputCtl"
    )


def test_throughput_control_with_absolute_target_and_no_control_container():
    provider = _provider(
        config={
            "kindling.cosmos.throughput_control.enabled": True,
            "kindling.cosmos.throughput_control.group_name": "kindling-etl",
            "kindling.cosmos.throughput_control.target_throughput": 4000,
        }
    )
    df = MagicMock()
    df.write = _Writer()

    provider.write_to_entity(df, _entity(BASE_TAGS))

    assert df.write.options["spark.cosmos.throughputControl.enabled"] == "true"
    assert df.write.options["spark.cosmos.throughputControl.name"] == "kindling-etl"
    assert df.write.options["spark.cosmos.throughputControl.targetThroughput"] == "4000"
    assert "spark.cosmos.throughputControl.targetThroughputThreshold" not in df.write.options
    # The connector defaults to a dedicated global-control container and
    # rejects the config when none is named; without one we switch that off.
    assert (
        df.write.options["spark.cosmos.throughputControl.globalControl.useDedicatedContainer"]
        == "false"
    )
    assert "spark.cosmos.throughputControl.globalControl.database" not in df.write.options


def test_throughput_control_with_control_container_keeps_dedicated_mode():
    provider = _provider(
        config={
            "kindling.cosmos.throughput_control.enabled": True,
            "kindling.cosmos.throughput_control.group_name": "kindling-etl",
            "kindling.cosmos.throughput_control.target_throughput": 4000,
            "kindling.cosmos.throughput_control.global_control.database": "ThroughputDb",
            "kindling.cosmos.throughput_control.global_control.container": "ThroughputCtl",
        }
    )
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(_entity(BASE_TAGS))

    assert "spark.cosmos.throughputControl.globalControl.useDedicatedContainer" not in (
        reader.options
    )
    assert reader.options["spark.cosmos.throughputControl.globalControl.database"] == (
        "ThroughputDb"
    )


def test_throughput_control_requires_group_name():
    # The connector asserts the group name whenever throughput control is on.
    provider = _provider(
        config={
            "kindling.cosmos.throughput_control.enabled": True,
            "kindling.cosmos.throughput_control.target_throughput": 4000,
        }
    )

    with pytest.raises(ValueError, match="group_name"):
        with _patched_spark_read(_Reader()):
            provider.read_entity(_entity(BASE_TAGS))


def test_throughput_control_enabled_must_be_boolean():
    provider = _provider(config={"kindling.cosmos.throughput_control.enabled": "maybe"})

    with pytest.raises(ValueError, match="enabled must be a boolean"):
        with _patched_spark_read(_Reader()):
            provider.read_entity(_entity(BASE_TAGS))


def test_config_service_errors_propagate():
    class _BrokenConfigService:
        def get(self, key, default=None):
            raise RuntimeError("config store unavailable")

    from kindling_ext_cosmos import CosmosEntityProvider

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    provider = CosmosEntityProvider(logger_provider, _BrokenConfigService())

    with pytest.raises(RuntimeError, match="config store unavailable"):
        with _patched_spark_read(_Reader()):
            provider.read_entity(_entity(BASE_TAGS))


def test_throughput_control_rejects_threshold_and_target_together():
    provider = _provider(
        config={
            "kindling.cosmos.throughput_control.enabled": True,
            "kindling.cosmos.throughput_control.group_name": "kindling-etl",
            "kindling.cosmos.throughput_control.target_threshold": 0.5,
            "kindling.cosmos.throughput_control.target_throughput": 4000,
        }
    )

    with pytest.raises(ValueError, match="only one of"):
        with _patched_spark_read(_Reader()):
            provider.read_entity(_entity(BASE_TAGS))


def test_throughput_control_requires_a_target_when_enabled():
    provider = _provider(
        config={
            "kindling.cosmos.throughput_control.enabled": True,
            "kindling.cosmos.throughput_control.group_name": "kindling-etl",
        }
    )

    with pytest.raises(ValueError, match="requires"):
        with _patched_spark_read(_Reader()):
            provider.read_entity(_entity(BASE_TAGS))


@pytest.mark.parametrize("threshold", ["0", "1.5", "-0.2", "most"])
def test_throughput_control_threshold_must_be_a_fraction(threshold):
    provider = _provider(
        config={
            "kindling.cosmos.throughput_control.enabled": True,
            "kindling.cosmos.throughput_control.group_name": "kindling-etl",
            "kindling.cosmos.throughput_control.target_threshold": threshold,
        }
    )

    with pytest.raises(ValueError, match="target_threshold"):
        with _patched_spark_read(_Reader()):
            provider.read_entity(_entity(BASE_TAGS))


def test_throughput_control_global_control_needs_both_names():
    provider = _provider(
        config={
            "kindling.cosmos.throughput_control.enabled": True,
            "kindling.cosmos.throughput_control.group_name": "kindling-etl",
            "kindling.cosmos.throughput_control.target_throughput": 4000,
            "kindling.cosmos.throughput_control.global_control.database": "ThroughputDb",
        }
    )

    with pytest.raises(ValueError, match="global_control"):
        with _patched_spark_read(_Reader()):
            provider.read_entity(_entity(BASE_TAGS))


def test_throughput_control_disabled_emits_nothing():
    provider = _provider(
        config={
            "kindling.cosmos.throughput_control.enabled": "false",
            "kindling.cosmos.throughput_control.target_throughput": 4000,
        }
    )
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(_entity(BASE_TAGS))

    assert not any(key.startswith("spark.cosmos.throughputControl") for key in reader.options)


def test_provider_option_tags_override_config_defaults():
    provider = _provider(
        config={
            "kindling.cosmos.read.max_item_count": 250,
            "kindling.cosmos.throughput_control.enabled": True,
            "kindling.cosmos.throughput_control.group_name": "kindling-etl",
            "kindling.cosmos.throughput_control.target_throughput": 4000,
        }
    )
    reader = _Reader()

    with _patched_spark_read(reader):
        provider.read_entity(
            _entity(
                {
                    **BASE_TAGS,
                    "provider.option.spark.cosmos.read.maxItemCount": "50",
                    "provider.option.spark.cosmos.throughputControl.enabled": "false",
                }
            )
        )

    assert reader.options["spark.cosmos.read.maxItemCount"] == "50"
    assert reader.options["spark.cosmos.throughputControl.enabled"] == "false"


# ---------------------------------------------------------------------------
# Spark-line connector coordinate resolution
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spark_version, expected_artifact",
    [
        ("3.4.3", "azure-cosmos-spark_3-4_2-12"),
        ("3.5.5", "azure-cosmos-spark_3-5_2-12"),
        ("3.5.0-fabric", "azure-cosmos-spark_3-5_2-12"),
        ("4.0.0", "azure-cosmos-spark_4-0_2-13"),
        ("4.0.1-databricks", "azure-cosmos-spark_4-0_2-13"),
        ("4.1.0", "azure-cosmos-spark_4-1_2-13"),
    ],
)
def test_connector_coordinate_follows_spark_line(spark_version, expected_artifact):
    from kindling_ext_cosmos import (
        COSMOS_SPARK_CONNECTOR_VERSION,
        resolve_cosmos_spark_connector_coordinate,
    )

    coordinate = resolve_cosmos_spark_connector_coordinate(spark_version)

    assert coordinate == (
        f"com.azure.cosmos.spark:{expected_artifact}:{COSMOS_SPARK_CONNECTOR_VERSION}"
    )


def test_connector_coordinate_scala_binary_matches_spark_major():
    from kindling_ext_cosmos import COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES

    for family, coordinate in COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES.items():
        scala = "2-13" if family.startswith("4.") else "2-12"
        assert coordinate.endswith(f"_{scala}:" + coordinate.rsplit(":", 1)[1]), coordinate


def test_connector_coordinate_rejects_unpublished_spark_line():
    from kindling_ext_cosmos import resolve_cosmos_spark_connector_coordinate

    with pytest.raises(ValueError, match="supported Spark lines"):
        resolve_cosmos_spark_connector_coordinate("3.2.1")


def test_connector_coordinate_rejects_garbage_version():
    from kindling_ext_cosmos import resolve_cosmos_spark_connector_coordinate

    with pytest.raises(ValueError, match="Unrecognised Spark version"):
        resolve_cosmos_spark_connector_coordinate("latest")


def test_connector_coordinate_defaults_to_installed_pyspark_version():
    import pyspark
    from kindling_ext_cosmos import resolve_cosmos_spark_connector_coordinate

    with patch.object(pyspark, "__version__", "4.0.0"):
        coordinate = resolve_cosmos_spark_connector_coordinate()

    assert "azure-cosmos-spark_4-0_2-13" in coordinate


def test_legacy_coordinate_constant_is_the_spark_35_artifact():
    from kindling_ext_cosmos import (
        COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE,
        COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES,
    )

    assert (
        COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATE == COSMOS_SPARK_CONNECTOR_MAVEN_COORDINATES["3.5"]
    )


def test_provider_reports_connector_coordinate_for_runtime():
    provider = _provider()

    assert "azure-cosmos-spark_3-5_2-12" in provider.connector_maven_coordinate("3.5.1")
    assert "azure-cosmos-spark_4-1_2-13" in provider.connector_maven_coordinate("4.1.0")
