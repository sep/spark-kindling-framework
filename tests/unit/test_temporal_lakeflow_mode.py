"""Execution-mode selection for the stratified temporal Lakeflow lowering.

``kindling.lakeflow.temporal_mode`` chooses between the default streaming
lowering (streaming tables + append flows) and the batch lowering
(materialized views + batch reads). These tests pin the emitted declaration
shape of both, the parity of the default with today's behavior, and the
validation of the configured value.
"""

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


def _declare_chain(mode, *, with_episodes, max_generations=2, transform=None):
    """Declare one chain through the lowering and return (dp, spark, names).

    ``mode`` is passed to the lowerer exactly as the engine would pass it, so
    the tests exercise the same normalization path as a configured value.
    """
    from kindling.data_entities import (
        DataEntityManager,
        DataEntityRegistry,
        EntityMetadata,
        EntityNameMapper,
    )
    from kindling.data_pipes import DataPipesManager, DataPipesRegistry
    from kindling_ext_databricks import temporal_lowering
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
    services = {
        TemporalEntityResolver: SimpleTemporalEntityResolver(),
        TemporalEventRegistry: event_registry,
        TemporalEpisodeRegistry: episode_registry,
        DataEntityRegistry: entity_registry,
        DataPipesRegistry: pipe_registry,
        EntityNameMapper: SimpleNamespace(
            get_table_name=lambda entity: f"cat.sch.{entity.entityid}"
        ),
    }

    def service_get(dep):
        try:
            return services[dep]
        except KeyError as exc:
            raise AssertionError(f"Unexpected service request: {dep}") from exc

    spark = MagicMock()
    with patch("kindling.injection.GlobalInjector.get", side_effect=service_get):

        @DataEvents.base_event(
            eventid="telemetry.base",
            input_entity_id="bronze.telemetry",
            subject_type="machine",
            subject_keys=["machine_id"],
            time_column="reading_ts",
            event_type="telemetry.observed",
            payload_columns=["temperature"],
        )
        def normalize(df):
            return transform(df) if transform else df

        DataEvents.condition_engine(engineid="default")
        if with_episodes:
            DataEpisodes.episode(
                episodeid="episode.temperature_high_active",
                start_event="condition.temperature_high.entered",
                end_event="condition.temperature_high.exited",
                subject_type="machine",
                expires_after_seconds=300,
            )
        declare_temporal_chain()

        with patch.object(temporal_lowering, "_spark", lambda: spark):
            dp = FakeDp()
            kwargs = {} if mode is None else {"mode": mode}
            temporal_lowering.declare_stratified_temporal(
                dp,
                events_name="silver_events",
                episodes_name="silver_episodes" if with_episodes else None,
                max_generations=max_generations,
                **kwargs,
            )
    return dp, spark


STRATA = ["silver_events__g0", "silver_events__g1", "silver_events__g2"]


def test_batch_mode_declares_every_stratum_as_a_materialized_view():
    dp, _spark = _declare_chain("batch", with_episodes=False)

    assert set(dp.materialized_views) == {*STRATA, "silver_events"}
    # No streaming primitive anywhere in the event strata.
    assert dp.streaming_tables == []
    assert dp.append_flows == []


def test_batch_mode_keeps_the_streaming_table_only_for_snapshot_cdc():
    dp, _spark = _declare_chain("batch", with_episodes=True)

    # Snapshot CDC requires a streaming-table target; that is the sole
    # streaming declaration a batch chain may make.
    assert dp.streaming_tables == ["silver_episodes"]
    assert dp.append_flows == []
    assert len(dp.auto_cdc_snapshot_flows) == 1
    flow = dp.auto_cdc_snapshot_flows[0]
    assert flow["target"] == "silver_episodes"
    assert flow["source"] == "silver_episodes__episode_snapshot"
    assert flow["stored_as_scd_type"] == 2
    assert "silver_episodes__episode_snapshot" in dp.views
    assert set(dp.materialized_views) == {
        *STRATA,
        "silver_events__determinations",
        "silver_events",
    }


def test_batch_public_events_union_keeps_every_stratum():
    dp, spark = _declare_chain("batch", with_episodes=False)

    spark.table.reset_mock()
    dp.materialized_views["silver_events"]()
    assert [call.args[0] for call in spark.table.call_args_list] == STRATA


def test_batch_fans_in_multiple_base_declarations_inside_one_view():
    """Fan-in stays native: one MV, each input transformed on its own."""
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
    services = {
        TemporalEntityResolver: SimpleTemporalEntityResolver(),
        TemporalEventRegistry: TemporalEventRegistryManager(_logger_provider()),
        TemporalEpisodeRegistry: TemporalEpisodeRegistryManager(_logger_provider()),
        DataEntityRegistry: DataEntityManager(),
        DataPipesRegistry: DataPipesManager(_logger_provider()),
        EntityNameMapper: SimpleNamespace(get_table_name=lambda entity: "unused"),
    }

    with patch("kindling.injection.GlobalInjector.get", side_effect=lambda dep: services[dep]):

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
            payload_columns=["property_name"],
        )
        def normalize_twin_change(df):
            return df

        declare_temporal_chain()
        spark = MagicMock()
        with patch.object(temporal_lowering, "_spark", lambda: spark):
            dp = FakeDp()
            temporal_lowering.declare_stratified_temporal(
                dp,
                events_name="silver_events",
                episodes_name=None,
                max_generations=0,
                mode="batch",
            )

    # Both declarations land in ONE stratum-0 materialized view -- the
    # fan-in -- with no per-source helper dataset and no append flow. What
    # that view reads is asserted against real Spark in
    # tests/integration/test_temporal_lakeflow_batch_integration.py.
    assert list(dp.materialized_views) == ["silver_events__g0", "silver_events"]
    assert dp.streaming_tables == []
    assert dp.append_flows == []


@pytest.mark.parametrize("mode", [None, "streaming", " STREAMING ", "Streaming"])
def test_default_and_explicit_streaming_are_the_current_topology(mode):
    dp, spark = _declare_chain(mode, with_episodes=True)

    assert dp.streaming_tables == [*STRATA, "silver_episodes"]
    assert [target for target, _ in dp.append_flows] == STRATA
    assert set(dp.materialized_views) == {
        "silver_events__determinations",
        "silver_events",
    }

    dp.materialized_views["silver_events"]()
    assert [call.args[0] for call in spark.table.call_args_list][-4:] == [
        *STRATA,
        "silver_events__determinations",
    ]


@pytest.mark.parametrize("mode", ["batch", " BATCH ", "Batch"])
def test_batch_value_ignores_case_and_surrounding_whitespace(mode):
    dp, _spark = _declare_chain(mode, with_episodes=False)

    assert set(dp.materialized_views) == {*STRATA, "silver_events"}
    assert dp.streaming_tables == []


@pytest.mark.parametrize(
    "value",
    ["", "   ", "stream", "batched", "2", None, True, False, 2, 0, 2.5, ["batch"], {"m": "b"}],
)
def test_invalid_mode_is_rejected_before_any_declaration(value):
    """An unusable value must fail before a single Lakeflow call is made.

    ``None`` reaches the lowerer only when a caller passes it explicitly —
    an absent config key resolves to ``streaming`` at the read site — so it
    is invalid here rather than a silent default.
    """
    from kindling_ext_databricks import temporal_lowering

    dp = FakeDp()
    with pytest.raises(ValueError, match="kindling.lakeflow.temporal_mode"):
        temporal_lowering.declare_stratified_temporal(
            dp,
            events_name="silver_events",
            episodes_name=None,
            max_generations=1,
            mode=value,
        )

    assert dp.streaming_tables == []
    assert dp.append_flows == []
    assert dp.materialized_views == {}
    assert dp.views == {}
