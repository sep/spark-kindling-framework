"""Databricks lowering tests for provider-owned streaming sources."""

from pathlib import Path

import pytest
from kindling.data_entities import EntityMetadata
from kindling.data_pipes import PipeMetadata
from kindling.entity_provider import (
    DeclarableStreamingSource,
    StreamableEntityProvider,
    StreamingSourceSpec,
)
from kindling_ext_databricks import DatabricksSdpEngine
from kindling_ext_sdp import DatasetType, InputClassification


class FakeRegistry(dict):
    def get_entity_ids(self):
        return self.keys()

    get_entity_definition = dict.get

    def get_pipe_ids(self):
        return self.keys()

    get_pipe_definition = dict.get


class FakeLakeflowDp:
    def __init__(self):
        self.materialized_views = {}
        self.streaming_tables = {}
        self.append_flows = []
        self.expectations = []

    def materialized_view(self, **kwargs):
        def decorator(fn):
            self.materialized_views[kwargs["name"]] = (kwargs, fn)
            return fn

        return decorator

    def create_streaming_table(self, name, **kwargs):
        self.streaming_tables[name] = kwargs

    def append_flow(self, target, name=None, **kwargs):
        def decorator(fn):
            self.append_flows.append(
                {"target": target, "name": name or fn.__name__, "kwargs": kwargs, "fn": fn}
            )
            return fn

        return decorator

    def expect_all(self, expectations):
        def decorator(fn):
            self.expectations.append(dict(expectations))
            return fn

        return decorator


class FakeSession:
    def __init__(self):
        self.reads = []

    def table(self, name):
        self.reads.append(name)
        return f"static:{name}"


class FakeStreamingProvider(DeclarableStreamingSource, StreamableEntityProvider):
    def __init__(self):
        self.spec = StreamingSourceSpec(
            provider_type="fake_stream",
            source_format="kafka",
            source_identity="telemetry@example.servicebus.windows.net",
            supported_option_names=("provider.fake",),
        )

    def streaming_source_spec(self, entity_metadata):
        return self.spec

    def read_entity_as_stream(self, entity_metadata, format=None, options=None):
        return f"provider-stream:{entity_metadata.entityid}:{options}"


def make_entity(entityid, tags=None, **overrides):
    params = dict(
        entityid=entityid,
        name=entityid.split(".")[-1],
        merge_columns=["id"],
        tags=tags or {},
        schema=None,
    )
    params.update(overrides)
    return EntityMetadata(**params)


def make_pipe(pipeid, input_entity_ids, output_entity_id, execute=None, **overrides):
    params = dict(
        pipeid=pipeid,
        name=pipeid,
        execute=execute or (lambda **dfs: "df"),
        tags={},
        input_entity_ids=input_entity_ids,
        output_entity_id=output_entity_id,
        output_type="delta",
    )
    params.update(overrides)
    return PipeMetadata(**params)


def streaming_graph(output_tags=None, execute=None, schema=None):
    entities = FakeRegistry(
        {
            "landing.telemetry": make_entity(
                "landing.telemetry", tags={"provider_type": "fake_stream"}
            ),
            "ref.devices": make_entity("ref.devices"),
            "bronze.devices": make_entity("bronze.devices"),
            "silver.telemetry": make_entity(
                "silver.telemetry",
                tags=output_tags or {},
                partition_columns=["event_date"],
                cluster_columns=["device_id"],
                schema=schema,
            ),
        }
    )
    pipes = FakeRegistry(
        {
            "prepare.devices": make_pipe(
                "prepare.devices",
                ["ref.devices"],
                "bronze.devices",
            ),
            "ingest.telemetry": make_pipe(
                "ingest.telemetry",
                ["landing.telemetry", "bronze.devices", "ref.devices"],
                "silver.telemetry",
                execute=execute,
            ),
        }
    )
    return entities, pipes


def provider_resolver(provider):
    def resolver(entity):
        if (entity.tags or {}).get("provider_type") == "fake_stream":
            return provider
        return None

    return resolver


@pytest.mark.parametrize(
    "mode, expected_target, expected_internal_input",
    [
        ("normalized", "silver_telemetry", "bronze_devices"),
        ("leaf", "telemetry", "devices"),
    ],
)
def test_streaming_source_dataset_emits_one_table_and_one_append_flow(
    mode, expected_target, expected_internal_input
):
    provider = FakeStreamingProvider()
    captured = {}
    entities, pipes = streaming_graph(execute=lambda **dfs: captured.update(dfs) or "df:out")
    dp = FakeLakeflowDp()
    session = FakeSession()
    provider_reads = []

    def provider_stream_resolver(_spark, entity_id):
        provider_reads.append(entity_id)
        return f"stream:{entity_id}"

    engine = DatabricksSdpEngine(
        entities,
        pipes,
        dp_module=dp,
        dataset_naming=mode,
        session_provider=lambda: session,
        provider_resolver=provider_resolver(provider),
        provider_stream_resolver=provider_stream_resolver,
    )

    plan = engine.build_plan()
    dataset = plan.get_dataset("silver.telemetry")
    source_input, internal_input, ref_input = dataset.inputs
    engine.declare_pipeline(plan)
    flow = dp.append_flows[0]
    result = flow["fn"]()

    assert dataset.dataset_type is DatasetType.STREAMING_TABLE
    assert source_input.classification is InputClassification.EXTERNAL_STREAMING_SOURCE
    assert source_input.streaming_source is provider.spec
    assert internal_input.classification is InputClassification.INTERNAL
    assert ref_input.classification is InputClassification.EXTERNAL
    assert set(dp.streaming_tables) == {expected_target}
    assert len(dp.append_flows) == 1
    assert flow["target"] == expected_target
    assert flow["name"] == f"{expected_target}_flow"
    assert result == "df:out"
    assert provider_reads == ["landing.telemetry"]
    assert session.reads == [expected_internal_input, "ref.devices"]
    assert captured == {
        "landing_telemetry": "stream:landing.telemetry",
        "bronze_devices": f"static:{expected_internal_input}",
        "ref_devices": "static:ref.devices",
    }


def test_streaming_source_lowering_composes_with_table_metadata_and_expectations():
    provider = FakeStreamingProvider()
    entities, pipes = streaming_graph(
        output_tags={
            "comment": "Clean telemetry",
            "sdp.table_properties.quality": "bronze",
        },
        schema="schema-sentinel",
    )
    dp = FakeLakeflowDp()
    engine = DatabricksSdpEngine(
        entities,
        pipes,
        dp_module=dp,
        provider_resolver=provider_resolver(provider),
        provider_stream_resolver=lambda _spark, entity_id: f"stream:{entity_id}",
        engine_config={
            "ingest.telemetry": {
                "databricks_sdp": {
                    "expectations": {"valid_device": "device_id IS NOT NULL"},
                    "table_properties": {"owner": "kindling"},
                }
            }
        },
    )

    engine.declare_pipeline(engine.build_plan())

    table_kwargs = dp.streaming_tables["silver_telemetry"]
    assert table_kwargs["comment"] == "Clean telemetry"
    assert table_kwargs["table_properties"] == {"owner": "kindling", "quality": "bronze"}
    assert table_kwargs["partition_cols"] == ["event_date"]
    assert table_kwargs["cluster_by"] == ["device_id"]
    assert "schema" not in table_kwargs
    assert dp.expectations == [{"valid_device": "device_id IS NOT NULL"}]


def test_streaming_source_lowering_adds_no_imperative_streaming_lifecycle_calls():
    repo_root = Path(__file__).resolve().parents[2]
    roots = [
        repo_root / "packages/extensions/kindling_ext_sdp/kindling_ext_sdp",
        repo_root / "packages/extensions/kindling_ext_databricks/kindling_ext_databricks",
    ]
    files = [path for root in roots for path in root.rglob("*.py")]
    text = "\n".join(path.read_text(encoding="utf-8") for path in files)

    assert files
    assert "writeStream" not in text
    assert "checkpointLocation" not in text
    assert "_jvm" not in text
    assert "_jsc" not in text
