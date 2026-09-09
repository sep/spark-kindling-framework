"""Declaration-shape tests for the stratified temporal SDP lowering."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

EXTENSION_ROOTS = [
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_temporal",
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_databricks",
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_sdp",
]


@pytest.fixture(autouse=True)
def _extensions_on_path(monkeypatch):
    for root in EXTENSION_ROOTS:
        monkeypatch.syspath_prepend(str(root))


class FakeDp:
    """Records every declaration the lowering emits."""

    def __init__(self):
        self.streaming_tables = []
        self.append_flows = []
        self.append_flow_functions = {}
        self.auto_cdc_snapshot_flows = []
        self.views = {}
        self.materialized_views = {}

    def create_streaming_table(self, name, **_kwargs):
        self.streaming_tables.append(name)

    def append_flow(self, target, name=None, **_kwargs):
        def decorator(fn):
            flow_name = name or fn.__name__
            self.append_flows.append((target, flow_name))
            self.append_flow_functions[flow_name] = fn
            return fn

        return decorator

    def create_auto_cdc_from_snapshot_flow(self, **kwargs):
        self.auto_cdc_snapshot_flows.append(kwargs)

    def temporary_view(self, name=None, **_kwargs):
        def decorator(fn):
            self.views[name or fn.__name__] = fn
            return fn

        return decorator

    def materialized_view(self, name=None, **_kwargs):
        def decorator(fn):
            self.materialized_views[name or fn.__name__] = fn
            return fn

        return decorator


def _logger_provider():
    provider = MagicMock()
    provider.get_logger.return_value = MagicMock()
    return provider


def _config_service(sections):
    config_service = MagicMock()
    config_service.get.side_effect = lambda key, default=None: sections.get(key, default)
    return config_service


def _temporal_leaf_config():
    return {
        "kindling.storage.table_naming": "leaf",
        "kindling.storage.table_schema": "cwmdp",
        "kindling.features.databricks.uc_enabled": False,
        "kindling.temporal.max_generations": 2,
        "dataentities": {
            "silver.**": {
                "tags": {
                    "provider.table_naming": "leaf",
                    "provider.table_schema": "cwmdp",
                }
            }
        },
        "dataentities-bytag": {
            "temporal.kind": {
                "events": {"tags": {"provider.table_catalog": "dev_events"}},
                "conditions": {"tags": {"provider.table_catalog": "dev_conditions"}},
                "episodes": {"tags": {"provider.table_catalog": "dev_episodes"}},
            }
        },
    }


def _higher_order_rule():
    return SimpleNamespace(
        condition_id="condition.after_episode",
        consumes_event_type=["episode.temperature_high_active.closed"],
        produced_event_types=[
            "condition.after_episode.entered",
            "condition.after_episode.exited",
        ],
    )


def _declare_leaf_named_temporal_graph(monkeypatch, overlay_timing):
    from kindling.data_entities import (
        DataEntityManager,
        DataEntityRegistry,
        EntityNameMapper,
    )
    from kindling.data_pipes import DataPipesManager, DataPipesRegistry
    from kindling.entity_naming import TableNamingPolicy
    from kindling.entity_resolution import ConfigDrivenEntityNameMapper
    from kindling.spark_config import ConfigService
    from kindling_ext_databricks import DatabricksSdpEngine, temporal_lowering
    from kindling_ext_databricks.auto_cdc import SCD_SOURCE_SUFFIX
    from kindling_ext_temporal import (
        DataEpisodes,
        DataEvents,
        SimpleTemporalEntityResolver,
        TemporalEntityResolver,
        TemporalEpisodeRegistry,
        TemporalEpisodeRegistryManager,
        TemporalEventRegistry,
        TemporalEventRegistryManager,
        declare_temporal_chain,
    )

    DataEvents.reset()
    DataEpisodes.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    episode_registry = TemporalEpisodeRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    entity_registry.register_entity(
        "silver.device_telemetry",
        name="device_telemetry",
        schema=None,
        merge_columns=["device_id"],
        tags={"provider_type": "delta"},
    )
    pipe_registry = DataPipesManager(_logger_provider())
    resolver = SimpleTemporalEntityResolver()
    config_service = _config_service(_temporal_leaf_config())
    name_mapper = ConfigDrivenEntityNameMapper(config_service, _logger_provider())

    if overlay_timing == "before":
        entity_registry.apply_config_overrides(config_service)

    services = {
        TemporalEntityResolver: resolver,
        TemporalEventRegistry: event_registry,
        TemporalEpisodeRegistry: episode_registry,
        DataEntityRegistry: entity_registry,
        DataPipesRegistry: pipe_registry,
        EntityNameMapper: name_mapper,
        ConfigService: config_service,
    }

    def service_get(dep):
        try:
            return services[dep]
        except KeyError as exc:
            raise AssertionError(f"Unexpected service request: {dep}") from exc

    physical_condition_reads = []

    def read_rules(_spark, conditions_entity):
        physical_condition_reads.append(temporal_lowering._physical_table_name(conditions_entity))
        return {1: [_higher_order_rule()]}, 1

    class FakeConditionEngineRunner:
        def execute_rules(self, *_args, **_kwargs):
            return MagicMock(name="condition_rules")

    class FakeEpisodeRunner:
        def execute(self, *_args, **_kwargs):
            return MagicMock(name="episodes")

    spark = MagicMock()
    monkeypatch.setattr(temporal_lowering, "_spark", lambda: spark)
    monkeypatch.setattr(temporal_lowering, "_read_rules", read_rules)
    monkeypatch.setattr(
        temporal_lowering.TemporalPipeTranslator,
        "select_event_envelope",
        lambda df, _metadata: df,
    )
    monkeypatch.setattr(
        "kindling_ext_temporal.engine.ConditionEngineRunner",
        FakeConditionEngineRunner,
    )
    monkeypatch.setattr("kindling_ext_temporal.engine.EpisodeRunner", FakeEpisodeRunner)
    monkeypatch.setattr(
        temporal_lowering, "_project_determination_events", lambda *_args: MagicMock()
    )

    with patch("kindling.injection.GlobalInjector.get", side_effect=service_get):

        @DataEvents.base_event(
            eventid="telemetry.base",
            input_entity_id="silver.device_telemetry",
            subject_type="machine",
            subject_keys=["machine_id"],
            time_column="reading_ts",
            event_type="telemetry.observed",
            payload_columns=["temperature"],
            use_watermark=True,
        )
        def normalize(df):
            return df

        DataEvents.condition_engine(engineid="default")
        DataEpisodes.episode(
            episodeid="episode.temperature_high_active",
            start_event="condition.temperature_high.entered",
            end_event="condition.temperature_high.exited",
            subject_type="machine",
            expires_after_seconds=300,
        )
        selected = declare_temporal_chain()

        if overlay_timing == "after":
            entity_registry.apply_config_overrides(config_service)

        # The SDP lowering asks the temporal resolver for canonical entities
        # while the config overlay is materialized on the registry copy.
        resolver._events_entity = entity_registry.get_entity_definition("silver.events")
        resolver._conditions_entity = entity_registry.get_entity_definition("silver.conditions")
        resolver._episodes_entity = entity_registry.get_entity_definition("silver.episodes")

        dp = FakeDp()
        engine = DatabricksSdpEngine(
            entity_registry,
            pipe_registry,
            dp_module=dp,
            shared_naming=TableNamingPolicy.from_config_value(
                config_service.get("kindling.storage.table_naming")
            ),
        )
        plan = engine.build_plan(selected)
        engine.declare_pipeline(plan)

    spark.readStream.table.reset_mock()
    dp.append_flow_functions["events__g0_telemetry_base"]()
    source_stream_reads = tuple(call.args[0] for call in spark.readStream.table.call_args_list)

    spark.table.reset_mock()
    dp.views["episodes__episode_snapshot"]()
    episode_snapshot_reads = tuple(call.args[0] for call in spark.table.call_args_list)

    spark.table.reset_mock()
    dp.materialized_views["events__determinations"]()
    determinations_reads = tuple(call.args[0] for call in spark.table.call_args_list)

    spark.table.reset_mock()
    dp.materialized_views["events__ghi"]()
    higher_order_reads = tuple(call.args[0] for call in spark.table.call_args_list)

    spark.table.reset_mock()
    dp.materialized_views["events"]()
    events_union_reads = tuple(call.args[0] for call in spark.table.call_args_list)

    temporal_ids = ("silver.events", "silver.conditions", "silver.episodes")
    helper_suffixes = (
        "__g0",
        "__g1",
        "__g2",
        "__ghi",
        "__determinations",
        "__episode_snapshot",
        SCD_SOURCE_SUFFIX,
    )
    entity_ids = tuple(entity_registry.get_entity_ids())
    helper_entity_ids = tuple(
        entity_id for entity_id in entity_ids if entity_id.endswith(helper_suffixes)
    )

    return {
        "plan_datasets": tuple(dataset.name for dataset in plan.datasets),
        "plan_inputs": tuple(
            (dataset.name, tuple(input_.entity_id for input_ in dataset.inputs))
            for dataset in plan.datasets
        ),
        "streaming_tables": tuple(dp.streaming_tables),
        "append_flows": tuple(dp.append_flows),
        "auto_cdc_snapshot_flows": tuple(
            (
                flow["target"],
                flow["source"],
                tuple(flow["keys"]),
                flow["stored_as_scd_type"],
            )
            for flow in dp.auto_cdc_snapshot_flows
        ),
        "views": tuple(sorted(dp.views)),
        "materialized_views": tuple(sorted(dp.materialized_views)),
        "source_stream_reads": source_stream_reads,
        "episode_snapshot_reads": episode_snapshot_reads,
        "determinations_reads": determinations_reads,
        "higher_order_reads": higher_order_reads,
        "events_union_reads": events_union_reads,
        "physical_condition_reads": tuple(physical_condition_reads),
        "physical_names": tuple(
            (
                entity_id,
                name_mapper.get_table_name(entity_registry.get_entity_definition(entity_id)),
            )
            for entity_id in temporal_ids
        ),
        "temporal_entity_tags": tuple(
            (entity_id, dict(entity_registry.get_entity_definition(entity_id).tags))
            for entity_id in temporal_ids
        ),
        "provider_table_name_tags": tuple(
            entity_id
            for entity_id in entity_ids
            if "provider.table_name"
            in (entity_registry.get_entity_definition(entity_id).tags or {})
        ),
        "helper_entity_ids": helper_entity_ids,
    }


@pytest.mark.parametrize("mode", ["normalized", "leaf"])
def test_stratified_lowering_emits_the_full_dataset_graph(monkeypatch, mode):
    from kindling.data_entities import (
        DataEntityManager,
        DataEntityRegistry,
        EntityMetadata,
        EntityNameMapper,
    )
    from kindling.data_pipes import DataPipesManager, DataPipesRegistry
    from kindling.spark_config import ConfigService
    from kindling_ext_databricks import DatabricksSdpEngine, temporal_lowering
    from kindling_ext_temporal import (
        DataEpisodes,
        DataEvents,
        SimpleTemporalEntityResolver,
        TemporalEntityResolver,
        TemporalEpisodeRegistry,
        TemporalEpisodeRegistryManager,
        TemporalEventRegistry,
        TemporalEventRegistryManager,
        declare_temporal_chain,
    )

    DataEvents.reset()
    DataEpisodes.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    episode_registry = TemporalEpisodeRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    entity_registry.registry["bronze.telemetry"] = EntityMetadata(
        entityid="bronze.telemetry", name="telemetry", schema=None, merge_columns=[], tags={}
    )
    pipe_registry = DataPipesManager(_logger_provider())
    resolver = SimpleTemporalEntityResolver()

    name_mapper = SimpleNamespace(get_table_name=lambda entity: f"cat.sch.{entity.entityid}")
    services = {
        TemporalEntityResolver: resolver,
        TemporalEventRegistry: event_registry,
        TemporalEpisodeRegistry: episode_registry,
        DataEntityRegistry: entity_registry,
        DataPipesRegistry: pipe_registry,
        EntityNameMapper: name_mapper,
        ConfigService: SimpleNamespace(get=lambda key, default=None: 2),
    }

    def service_get(dep):
        try:
            return services[dep]
        except KeyError as exc:
            raise AssertionError(f"Unexpected service request: {dep}") from exc

    with patch("kindling.injection.GlobalInjector.get", side_effect=service_get):

        @DataEvents.base_event(
            eventid="telemetry.base",
            input_entity_id="bronze.telemetry",
            subject_type="machine",
            subject_keys=["machine_id"],
            time_column="reading_ts",
            event_type="telemetry.observed",
            payload_columns=["temperature"],
            use_watermark=True,
        )
        def normalize(df):
            return df

        DataEvents.condition_engine(engineid="default")
        DataEpisodes.episode(
            episodeid="episode.temperature_high_active",
            start_event="condition.temperature_high.entered",
            end_event="condition.temperature_high.exited",
            subject_type="machine",
            expires_after_seconds=300,
        )
        selected = declare_temporal_chain()

        # Rules are unreadable in this hermetic test (first-run path).
        spark = MagicMock()
        monkeypatch.setattr(temporal_lowering, "_spark", lambda: spark)

        dp = FakeDp()
        engine = DatabricksSdpEngine(
            entity_registry, pipe_registry, dp_module=dp, dataset_naming=mode
        )
        engine.declare_pipeline(engine.build_plan(selected))

    events_name = "events" if mode == "leaf" else "silver_events"
    episodes_name = "episodes" if mode == "leaf" else "silver_episodes"
    assert dp.streaming_tables == [
        f"{events_name}__g0",
        f"{events_name}__g1",
        f"{events_name}__g2",
        episodes_name,
    ]
    assert [target for target, _ in dp.append_flows] == [
        f"{events_name}__g0",
        f"{events_name}__g1",
        f"{events_name}__g2",
    ]
    assert len(dp.auto_cdc_snapshot_flows) == 1
    flow = dp.auto_cdc_snapshot_flows[0]
    assert flow["target"] == episodes_name
    assert flow["source"] == f"{episodes_name}__episode_snapshot"
    assert flow["keys"] == ["episode_id"]
    assert flow["stored_as_scd_type"] == 2
    assert f"{episodes_name}__episode_snapshot" in dp.views
    assert set(dp.materialized_views) == {f"{events_name}__determinations", events_name}

    # Evaluate the public graph surfaces to verify their references as well.
    spark.table.reset_mock()
    dp.materialized_views[events_name]()
    assert [call.args[0] for call in spark.table.call_args_list] == [
        f"{events_name}__g0",
        f"{events_name}__g1",
        f"{events_name}__g2",
        f"{events_name}__determinations",
    ]
    monkeypatch.setattr(temporal_lowering, "_project_determination_events", lambda *args: None)
    spark.table.reset_mock()
    dp.materialized_views[f"{events_name}__determinations"]()
    spark.table.assert_called_once_with(episodes_name)


def test_temporal_chain_leaf_naming_uses_wildcard_and_temporal_kind_overlays(
    monkeypatch,
):
    before = _declare_leaf_named_temporal_graph(monkeypatch, "before")
    after = _declare_leaf_named_temporal_graph(monkeypatch, "after")

    assert before == after
    assert before["plan_datasets"] == ("silver.events", "silver.episodes")
    assert before["plan_inputs"] == (
        ("silver.events", ("silver.device_telemetry", "silver.conditions.current")),
        ("silver.episodes", ("silver.events",)),
    )
    assert before["streaming_tables"] == (
        "events__g0",
        "events__g1",
        "events__g2",
        "episodes",
    )
    assert before["append_flows"] == (
        ("events__g0", "events__g0_telemetry_base"),
        ("events__g1", "events__g1_flow"),
        ("events__g2", "events__g2_flow"),
    )
    assert before["auto_cdc_snapshot_flows"] == (
        ("episodes", "episodes__episode_snapshot", ("episode_id",), 2),
    )
    assert before["views"] == ("episodes__episode_snapshot",)
    assert before["materialized_views"] == (
        "events",
        "events__determinations",
        "events__ghi",
    )
    assert before["source_stream_reads"] == ("cwmdp.device_telemetry",)
    assert before["episode_snapshot_reads"] == ("events__g0", "events__g1", "events__g2")
    assert before["determinations_reads"] == ("episodes",)
    assert before["higher_order_reads"] == ("events__determinations",)
    assert before["events_union_reads"] == (
        "events__g0",
        "events__g1",
        "events__g2",
        "events__determinations",
        "events__ghi",
    )
    assert before["physical_condition_reads"] == ("dev_conditions.cwmdp.conditions",)
    assert before["physical_names"] == (
        ("silver.events", "dev_events.cwmdp.events"),
        ("silver.conditions", "dev_conditions.cwmdp.conditions"),
        ("silver.episodes", "dev_episodes.cwmdp.episodes"),
    )
    assert before["temporal_entity_tags"] == (
        (
            "silver.events",
            {
                "provider.table_catalog": "dev_events",
                "provider.table_naming": "leaf",
                "provider.table_schema": "cwmdp",
                "provider_type": "delta",
                "temporal.kind": "events",
            },
        ),
        (
            "silver.conditions",
            {
                "provider.table_catalog": "dev_conditions",
                "provider.table_naming": "leaf",
                "provider.table_schema": "cwmdp",
                "provider_type": "delta",
                "scd.close_on_missing": "true",
                "scd.current_entity_id": "silver.conditions.current",
                "scd.routing_key": "hash",
                "scd.tracked": (
                    "consumes_event_type,subject_type,parameters,enabled,valid_from,valid_to"
                ),
                "scd.type": "2",
                "temporal.kind": "conditions",
            },
        ),
        (
            "silver.episodes",
            {
                "provider.table_catalog": "dev_episodes",
                "provider.table_naming": "leaf",
                "provider.table_schema": "cwmdp",
                "provider_type": "delta",
                "temporal.kind": "episodes",
            },
        ),
    )
    assert before["provider_table_name_tags"] == ()
    assert before["helper_entity_ids"] == ()


def test_stratified_lowering_fans_in_multiple_driving_entities_natively():
    """The Databricks lowering lands every base event declaration as its
    own independent append_flow into one shared stratum-0 streaming table,
    each reading its own source entity, exactly like the single-entity case
    above but with N sources instead of one.
    """
    from kindling.data_entities import (
        DataEntityManager,
        DataEntityRegistry,
        EntityNameMapper,
    )
    from kindling.data_pipes import DataPipesManager, DataPipesRegistry
    from kindling_ext_databricks import temporal_lowering
    from kindling_ext_temporal import (
        DataEvents,
        SimpleTemporalEntityResolver,
        TemporalEntityResolver,
        TemporalEpisodeRegistry,
        TemporalEpisodeRegistryManager,
        TemporalEventRegistry,
        TemporalEventRegistryManager,
        declare_temporal_chain,
    )

    DataEvents.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    episode_registry = TemporalEpisodeRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())
    resolver = SimpleTemporalEntityResolver()

    name_mapper = SimpleNamespace(get_table_name=lambda entity: f"cat.sch.{entity.entityid}")
    services = {
        TemporalEntityResolver: resolver,
        TemporalEventRegistry: event_registry,
        TemporalEpisodeRegistry: episode_registry,
        DataEntityRegistry: entity_registry,
        DataPipesRegistry: pipe_registry,
        EntityNameMapper: name_mapper,
    }

    def service_get(dep):
        try:
            return services[dep]
        except KeyError as exc:
            raise AssertionError(f"Unexpected service request: {dep}") from exc

    with patch("kindling.injection.GlobalInjector.get", side_effect=service_get):

        @DataEvents.base_event(
            eventid="telemetry.base",
            input_entity_id="silver.device_telemetry",
            subject_type="device",
            subject_keys=["device_id"],
            time_column="reading_ts",
            event_type="telemetry.observed",
            payload_columns=["temperature"],
        )
        def normalize_telemetry(df):
            return df

        @DataEvents.base_event(
            eventid="twin_change.base",
            input_entity_id="silver.device_twin_change",
            subject_type="device",
            subject_keys=["device_id"],
            time_column="changed_at",
            event_type="twin.changed",
            payload_columns=["property_name", "property_value"],
        )
        def normalize_twin_change(df):
            return df

        # This is the fix under test: two distinct input_entity_id values,
        # with no shared staging entity, no longer raise on this engine.
        declare_temporal_chain()

        with patch.object(temporal_lowering, "_spark", lambda: MagicMock()):
            dp = FakeDp()
            temporal_lowering.declare_stratified_temporal(
                dp, events_name="silver_events", episodes_name=None, max_generations=0
            )

    assert dp.streaming_tables == ["silver_events__g0"]
    flow_targets_and_names = dict(dp.append_flows)
    assert set(flow_targets_and_names) == {"silver_events__g0"}
    flow_names = [name for _, name in dp.append_flows]
    assert flow_names == [
        "silver_events__g0_telemetry_base",
        "silver_events__g0_twin_change_base",
    ]
    # Exactly one shared stratum-0 target for both sources -- the fan-in.
    assert [target for target, _ in dp.append_flows] == [
        "silver_events__g0",
        "silver_events__g0",
    ]
