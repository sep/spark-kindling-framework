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
        self.auto_cdc_snapshot_flows = []
        self.views = {}
        self.materialized_views = {}

    def create_streaming_table(self, name, **_kwargs):
        self.streaming_tables.append(name)

    def append_flow(self, target, name=None, **_kwargs):
        def decorator(fn):
            self.append_flows.append((target, name or fn.__name__))
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


def _config_service(**overrides):
    """Key-aware ConfigService stub.

    A stub that answered every key with one value used to be harmless, but
    the declaration path now reads more than one key: returning the
    generation ceiling for ``kindling.lakeflow.temporal_mode`` would raise on
    an unrecognized mode. Unknown keys fall back to the caller's default.
    """
    values = {"kindling.temporal.max_generations": 2}
    values.update(overrides)

    def get(key, default=None):
        return values.get(key, default)

    return SimpleNamespace(get=get)


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
        ConfigService: _config_service(),
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


def _declare_chain_through_engine(monkeypatch, chainid, **config_overrides):
    """Build one telemetry-and-episodes chain under ``chainid`` and declare it
    through ``DatabricksSdpEngine``, returning ``(FakeDp, selected_pipe_ids)``.

    Goes through the engine rather than calling ``declare_stratified_temporal``
    directly so the config-key wiring is covered too.
    """
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
        collapse_temporal_chain,
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

    services = {
        TemporalEntityResolver: SimpleTemporalEntityResolver(),
        TemporalEventRegistry: event_registry,
        TemporalEpisodeRegistry: episode_registry,
        DataEntityRegistry: entity_registry,
        DataPipesRegistry: pipe_registry,
        EntityNameMapper: SimpleNamespace(
            get_table_name=lambda entity: f"cat.sch.{entity.entityid}"
        ),
        ConfigService: _config_service(**config_overrides),
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
        selected = collapse_temporal_chain(chainid)

        monkeypatch.setattr(temporal_lowering, "_spark", lambda: MagicMock())

        dp = FakeDp()
        engine = DatabricksSdpEngine(
            entity_registry, pipe_registry, dp_module=dp, dataset_naming="leaf"
        )
        engine.declare_pipeline(engine.build_plan(selected))

    return dp, selected


def test_batch_chain_under_a_custom_chain_id_still_declares_the_episode_branch(monkeypatch):
    """Regression: the episode half of a chain vanished unless its id was
    ``default``.

    ``collapse_temporal_chain("cwmdp")`` stamps ``temporal.chain_id`` onto the
    two composite chain PIPES, but the engine resolved it from the output
    ENTITY's tags — which never carry it. The sibling lookup therefore asked
    for ``temporal.chain.episodes.default``, missed, and handed
    ``episodes_name=None`` to the lowering: event strata were declared, the
    episode snapshot / Auto CDC target / determinations view silently were
    not. Batch mode here because that is where it was first observed; the
    defect is mode-independent.
    """
    dp, selected = _declare_chain_through_engine(
        monkeypatch, "cwmdp", **{"kindling.lakeflow.temporal_mode": "batch"}
    )

    # The chain pipes really are registered under the custom id: this test
    # would otherwise pass vacuously if the id were ignored.
    assert "temporal.chain.events.cwmdp" in selected
    assert "temporal.chain.episodes.cwmdp" in selected

    # Batch mode: strata are materialized views, and the episodes target is
    # the only streaming table.
    assert dp.streaming_tables == ["episodes"]
    assert "episodes__episode_snapshot" in dp.views
    assert set(dp.materialized_views) == {
        "events__g0",
        "events__g1",
        "events__g2",
        "events__determinations",
        "events",
    }

    assert len(dp.auto_cdc_snapshot_flows) == 1
    flow = dp.auto_cdc_snapshot_flows[0]
    assert flow["target"] == "episodes"
    assert flow["source"] == "episodes__episode_snapshot"
    assert flow["stored_as_scd_type"] == 2


def test_strata_materialization_view_declares_generations_as_temporary_views(monkeypatch):
    """``temporal_strata_materialization: view`` moves only the numbered
    generations off the catalog; everything downstream is byte-identical to
    the materialized-view lowering."""
    dp, _ = _declare_chain_through_engine(
        monkeypatch,
        "cwmdp",
        **{
            "kindling.lakeflow.temporal_mode": "batch",
            "kindling.lakeflow.temporal_strata_materialization": "view",
        },
    )

    # The generations are views now -- no __g* tables are created at all.
    assert {"events__g0", "events__g1", "events__g2"} <= set(dp.views)
    assert not any(name.startswith("events__g") for name in dp.materialized_views)

    # Everything from the determinations view downstream is unchanged.
    assert dp.streaming_tables == ["episodes"]
    assert "episodes__episode_snapshot" in dp.views
    assert set(dp.materialized_views) == {"events__determinations", "events"}
    assert len(dp.auto_cdc_snapshot_flows) == 1


def test_strata_materialization_defaults_to_materialized_views(monkeypatch):
    """Omitting the key keeps the pre-existing batch lowering."""
    dp, _ = _declare_chain_through_engine(
        monkeypatch, "cwmdp", **{"kindling.lakeflow.temporal_mode": "batch"}
    )

    assert {"events__g0", "events__g1", "events__g2"} <= set(dp.materialized_views)
    assert not any(name.startswith("events__g") for name in dp.views)


def test_strata_materialization_view_is_rejected_in_streaming_mode(monkeypatch):
    """A streaming stratum is an append-flow target, which a temporary view
    cannot be — so the combination is a config error, not a silent downgrade
    to batch reads."""
    with pytest.raises(ValueError, match="requires .*temporal_mode='batch'"):
        _declare_chain_through_engine(
            monkeypatch,
            "cwmdp",
            **{"kindling.lakeflow.temporal_strata_materialization": "view"},
        )


def test_strata_materialization_rejects_an_unknown_value(monkeypatch):
    with pytest.raises(ValueError, match="expected 'table' or 'view'"):
        _declare_chain_through_engine(
            monkeypatch,
            "cwmdp",
            **{
                "kindling.lakeflow.temporal_mode": "batch",
                "kindling.lakeflow.temporal_strata_materialization": "temporary",
            },
        )


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
