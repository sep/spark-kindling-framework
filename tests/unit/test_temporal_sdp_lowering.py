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


def test_stratified_lowering_emits_the_full_dataset_graph(monkeypatch):
    from kindling.data_entities import (
        DataEntityManager,
        DataEntityRegistry,
        EntityNameMapper,
    )
    from kindling.data_pipes import DataPipesManager, DataPipesRegistry
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
        sdp_lowering,
    )

    DataEvents.reset()
    DataEpisodes.reset()
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
        declare_temporal_chain()

        # Rules are unreadable in this hermetic test (first-run path).
        monkeypatch.setattr(sdp_lowering, "_spark", lambda: MagicMock())

        dp = FakeDp()
        sdp_lowering.declare_stratified_temporal(
            dp, events_name="silver_events", episodes_name="silver_episodes", max_generations=2
        )

    assert dp.streaming_tables == [
        "silver_events__g0",
        "silver_events__g1",
        "silver_events__g2",
        "silver_episodes",
    ]
    assert [target for target, _ in dp.append_flows] == [
        "silver_events__g0",
        "silver_events__g1",
        "silver_events__g2",
    ]
    assert len(dp.auto_cdc_snapshot_flows) == 1
    flow = dp.auto_cdc_snapshot_flows[0]
    assert flow["target"] == "silver_episodes"
    assert flow["source"] == "silver_episodes__episode_snapshot"
    assert flow["keys"] == ["episode_id"]
    assert flow["stored_as_scd_type"] == 2
    assert "silver_episodes__episode_snapshot" in dp.views
    assert set(dp.materialized_views) == {"silver_events__determinations", "silver_events"}
