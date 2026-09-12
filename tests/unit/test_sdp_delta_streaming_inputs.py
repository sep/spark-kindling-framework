"""Opt-in incremental reads of external Delta inputs.

A provider-owned streaming source (Event Hub) has always lowered to a
streaming table plus an append flow. An ordinary Delta table could not: the
read machinery existed — ``_build_dataset_function`` streams driving
EXTERNAL inputs with ``spark.readStream.table`` and keeps the rest as batch
reads — but nothing ever set the field that selects that emission path. The
``streaming_inputs`` engine-config key is the explicit opt-in that does,
never inferred, because an incremental read changes a pipe's semantics.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from kindling.data_entities import EntityMetadata
from kindling.data_pipes import PipeMetadata
from kindling_ext_databricks import DatabricksSdpEngine
from kindling_ext_sdp import DatasetType, InputClassification
from kindling_ext_sdp.declaration_engine import DeclarationValidationError


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

    def materialized_view(self, **kwargs):
        def decorator(fn):
            self.materialized_views[kwargs["name"]] = (kwargs, fn)
            return fn

        return decorator

    def create_streaming_table(self, name, **kwargs):
        self.streaming_tables[name] = kwargs

    def append_flow(self, target, name=None, **kwargs):
        def decorator(fn):
            self.append_flows.append({"target": target, "name": name or fn.__name__, "fn": fn})
            return fn

        return decorator


class FakeSession:
    """Records batch vs streaming reads separately."""

    def __init__(self):
        self.batch_reads = []
        self.stream_reads = []
        self.readStream = self._ReadStream(self)

    def table(self, name):
        self.batch_reads.append(name)
        return f"static:{name}"

    class _ReadStream:
        def __init__(self, session):
            self._session = session

        def table(self, name):
            self._session.stream_reads.append(name)
            return f"stream:{name}"


def make_entity(entityid, tags=None, **overrides):
    params = dict(
        entityid=entityid,
        name=entityid.split(".")[-1],
        merge_columns=["id"],
        tags=tags if tags is not None else {"provider_type": "delta"},
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


def telemetry_graph(execute=None, output_tags=None):
    """One pipe: a Delta bronze driving input plus a static reference join."""
    entities = FakeRegistry(
        {
            "bronze.device_telemetry": make_entity("bronze.device_telemetry"),
            "ref.devices": make_entity("ref.devices"),
            "silver.device_telemetry": make_entity(
                "silver.device_telemetry", tags=output_tags or {"provider_type": "delta"}
            ),
        }
    )
    pipes = FakeRegistry(
        {
            "curate.telemetry": make_pipe(
                "curate.telemetry",
                ["bronze.device_telemetry", "ref.devices"],
                "silver.device_telemetry",
                execute=execute,
            ),
        }
    )
    return entities, pipes


def build_engine(
    entities, pipes, dp=None, session=None, streaming_inputs=None, engine="databricks_sdp"
):
    engine_config = None
    if streaming_inputs is not None:
        engine_config = {"curate.telemetry": {engine: {"streaming_inputs": streaming_inputs}}}
    return DatabricksSdpEngine(
        entities,
        pipes,
        dp_module=dp,
        session_provider=(lambda: session) if session is not None else None,
        # Both resolvers stubbed symmetrically on entity ids: these tests
        # assert which inputs stream, not how names resolve. Physical-name
        # resolution has its own test below, against the real defaults.
        external_read_resolver=lambda _spark, entity_id: f"static:{entity_id}",
        external_stream_read_resolver=lambda spark, entity_id: spark.readStream.table(entity_id),
        provider_resolver=lambda _entity: None,
        engine_config=engine_config,
    )


def test_opted_in_delta_input_lowers_to_a_streaming_table_and_append_flow():
    captured = {}
    entities, pipes = telemetry_graph(execute=lambda **dfs: captured.update(dfs) or "df:out")
    dp = FakeLakeflowDp()
    session = FakeSession()
    engine = build_engine(
        entities, pipes, dp=dp, session=session, streaming_inputs=["bronze.device_telemetry"]
    )

    plan = engine.build_plan()
    dataset = plan.get_dataset("silver.device_telemetry")
    engine.declare_pipeline(plan)

    assert dataset.streamed_external_inputs == ("bronze.device_telemetry",)
    assert dataset.dataset_type is DatasetType.STREAMING_TABLE
    assert set(dp.streaming_tables) == {"silver_device_telemetry"}
    assert dp.materialized_views == {}
    assert len(dp.append_flows) == 1

    dp.append_flows[0]["fn"]()
    # The driving input streams; the reference join stays a batch read.
    assert session.stream_reads == ["bronze.device_telemetry"]
    assert captured == {
        "bronze_device_telemetry": "stream:bronze.device_telemetry",
        "ref_devices": "static:ref.devices",
    }


def test_without_the_opt_in_the_same_pipe_stays_a_batch_materialized_view():
    entities, pipes = telemetry_graph()
    dp = FakeLakeflowDp()
    session = FakeSession()
    engine = build_engine(entities, pipes, dp=dp, session=session)

    plan = engine.build_plan()
    dataset = plan.get_dataset("silver.device_telemetry")
    engine.declare_pipeline(plan)

    assert dataset.streamed_external_inputs == ()
    assert dataset.dataset_type is DatasetType.MATERIALIZED_VIEW
    assert set(dp.materialized_views) == {"silver_device_telemetry"}
    assert dp.streaming_tables == {}
    assert dp.append_flows == []

    dp.materialized_views["silver_device_telemetry"][1]()
    assert session.stream_reads == []


def test_opt_in_infers_a_streaming_table_over_an_explicit_materialized_view_request():
    entities, pipes = telemetry_graph(
        output_tags={"provider_type": "delta", "sdp.dataset_type": "materialized_view"}
    )
    engine = build_engine(entities, pipes, streaming_inputs=["bronze.device_telemetry"])

    with pytest.raises(DeclarationValidationError, match="streaming_dataset_type_conflict"):
        engine.build_plan()


@pytest.mark.parametrize(
    "streaming_inputs, expected_code",
    [
        (["bronze.not_an_input"], "streaming_input_not_an_input"),
        (["ref.devices"], "streaming_input_not_driving"),
        (
            ["bronze.device_telemetry", "ref.devices"],
            "multiple_streaming_inputs",
        ),
    ],
)
def test_unsupported_opt_in_shapes_are_rejected(streaming_inputs, expected_code):
    entities, pipes = telemetry_graph()
    engine = build_engine(entities, pipes, streaming_inputs=streaming_inputs)

    with pytest.raises(DeclarationValidationError, match=expected_code):
        engine.build_plan()


def test_a_non_delta_opted_in_input_is_rejected():
    entities, pipes = telemetry_graph()
    entities["bronze.device_telemetry"].tags["provider_type"] = "parquet"
    engine = build_engine(entities, pipes, streaming_inputs=["bronze.device_telemetry"])

    with pytest.raises(DeclarationValidationError, match="streaming_input_not_delta"):
        engine.build_plan()


def test_an_entity_with_no_provider_type_tag_is_treated_as_delta():
    """Absent provider_type means delta everywhere else in SDP validation."""
    entities, pipes = telemetry_graph()
    entities["bronze.device_telemetry"].tags.pop("provider_type")
    engine = build_engine(entities, pipes, streaming_inputs=["bronze.device_telemetry"])

    dataset = engine.build_plan().get_dataset("silver.device_telemetry")
    assert dataset.streamed_external_inputs == ("bronze.device_telemetry",)


def test_the_streamed_input_resolves_its_physical_table_name():
    """A streamed external read must resolve names exactly as the batch read.

    ``provider.table_*`` tags and non-default naming strategies mean the
    logical entity id is often not the physical table; streaming the id
    directly would read the wrong table or fail to resolve.
    """
    from unittest.mock import MagicMock

    from kindling.data_entities import EntityNameMapper
    from kindling.injection import GlobalInjector

    entities, pipes = telemetry_graph()
    dp = FakeLakeflowDp()
    session = FakeSession()
    mapper = SimpleNamespace(
        get_table_name=lambda entity: f"raw_catalog.landing.{entity.entityid.split('.')[-1]}"
    )

    engine = DatabricksSdpEngine(
        entities,
        pipes,
        dp_module=dp,
        session_provider=lambda: session,
        provider_resolver=lambda _entity: None,
        engine_config={
            "curate.telemetry": {
                "databricks_sdp": {"streaming_inputs": ["bronze.device_telemetry"]}
            }
        },
    )
    plan = engine.build_plan()
    engine.declare_pipeline(plan)

    with patch.object(
        GlobalInjector, "get", lambda cls: mapper if cls is EntityNameMapper else MagicMock()
    ):
        dp.append_flows[0]["fn"]()

    assert session.stream_reads == ["raw_catalog.landing.device_telemetry"]
    assert session.batch_reads == ["raw_catalog.landing.devices"]


def test_an_internal_input_cannot_be_opted_in():
    """In-pipeline producers are streamed by the driving contract already."""
    entities, pipes = telemetry_graph()
    entities["bronze.device_telemetry"] = make_entity("bronze.device_telemetry")
    pipes["produce.bronze"] = make_pipe(
        "produce.bronze", ["ref.devices"], "bronze.device_telemetry"
    )
    engine = build_engine(entities, pipes, streaming_inputs=["bronze.device_telemetry"])

    with pytest.raises(DeclarationValidationError, match="streaming_input_internal"):
        engine.build_plan()


def test_opt_in_must_name_every_driving_input():
    """A partial opt-in would silently stream an input nobody asked to stream."""
    entities, pipes = telemetry_graph()
    pipes["curate.telemetry"] = make_pipe(
        "curate.telemetry",
        ["bronze.device_telemetry", "ref.devices"],
        "silver.device_telemetry",
        driving_entity_ids=["bronze.device_telemetry", "ref.devices"],
    )
    engine = build_engine(entities, pipes, streaming_inputs=["bronze.device_telemetry"])

    with pytest.raises(DeclarationValidationError, match="streaming_inputs_partial"):
        engine.build_plan()


def test_the_opt_in_is_adapter_tier_and_rejected_against_oss_sdp():
    from kindling_ext_sdp import OssSdpEngine

    entities, pipes = telemetry_graph()
    engine = OssSdpEngine(
        entities,
        pipes,
        provider_resolver=lambda _entity: None,
        engine_config={
            "curate.telemetry": {"sdp": {"streaming_inputs": ["bronze.device_telemetry"]}}
        },
    )

    with pytest.raises(DeclarationValidationError, match="capability_not_supported"):
        engine.build_plan()
