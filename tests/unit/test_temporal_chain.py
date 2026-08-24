"""Unit coverage for collapse_temporal_chain and the autocollapse hook.

Chain-graph correctness (the composite pipes reproducing per-pipe lowering
semantics) is covered by tests/integration/test_temporal_chain_integration.py
with real Spark data. This file covers the registry bookkeeping and signal
wiring collapse_temporal_chain/_autocollapse_before_run own: which pipes get
unregistered, what order survives, and that a declaration gap never crashes
an unrelated run.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_temporal"
)


@pytest.fixture(autouse=True)
def _extension_package_on_path(monkeypatch):
    monkeypatch.syspath_prepend(str(EXTENSION_PACKAGE_ROOT))


def _logger_provider():
    provider = MagicMock()
    provider.get_logger.return_value = MagicMock()
    return provider


def _temporal_service_get(
    *,
    event_registry=None,
    episode_registry=None,
    condition_registry=None,
    entity_registry=None,
    pipe_registry=None,
    config_service=None,
):
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.spark_config import ConfigService
    from kindling_ext_temporal import (
        SimpleTemporalEntityResolver,
        TemporalConditionRegistry,
        TemporalEntityResolver,
        TemporalEpisodeRegistry,
        TemporalEpisodeRegistryManager,
        TemporalEventRegistry,
    )

    # declare_temporal_chain always consults the episode registry (for zero
    # declared episodes, an empty one is correct), even when a test only
    # cares about base events.
    episode_registry = episode_registry or TemporalEpisodeRegistryManager(_logger_provider())

    def _get(dep):
        if dep is TemporalEntityResolver:
            return SimpleTemporalEntityResolver()
        if dep is TemporalEventRegistry and event_registry is not None:
            return event_registry
        if dep is TemporalEpisodeRegistry:
            return episode_registry
        if dep is TemporalConditionRegistry and condition_registry is not None:
            return condition_registry
        if dep is DataEntityRegistry and entity_registry is not None:
            return entity_registry
        if dep is DataPipesRegistry and pipe_registry is not None:
            return pipe_registry
        if dep is ConfigService and config_service is not None:
            return config_service
        raise AssertionError(f"Unexpected service request: {dep}")

    return _get


def _register_base_event(
    event_registry, entity_registry, pipe_registry, eventid, input_entity_id="bronze.telemetry"
):
    from kindling_ext_temporal import DataEvents

    DataEvents.reset()
    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):

        @DataEvents.base_event(
            eventid=eventid,
            input_entity_id=input_entity_id,
            subject_type="machine",
            subject_keys=["machine_id"],
            time_column="event_ts",
            event_type="telemetry.observed",
        )
        def normalize(df):
            return df


def test_collapse_temporal_chain_unregisters_declared_pipes():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import TemporalEventRegistryManager
    from kindling_ext_temporal.chain import (
        chain_events_pipe_id,
        collapse_temporal_chain,
    )

    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    _register_base_event(event_registry, entity_registry, pipe_registry, "telemetry.base")
    declared_pipe = "temporal.event.telemetry.base"
    assert pipe_registry.get_pipe_definition(declared_pipe) is not None

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):
        remaining = collapse_temporal_chain("t1")

    assert pipe_registry.get_pipe_definition(declared_pipe) is None
    events_pipe = chain_events_pipe_id("t1")
    assert events_pipe in remaining
    assert pipe_registry.get_pipe_definition(events_pipe).tags["temporal.lowering"] == "chain"

    # Idempotent: nothing left tagged declared, second call just re-confirms
    # the chain pipe.
    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):
        remaining_again = collapse_temporal_chain("t1")
    assert remaining_again == remaining


# --- gh#222: registry-declared temporal conditions in declare_temporal_chain -


def test_declare_temporal_chain_with_zero_condition_engines_still_includes_conditions_current():
    """Pre-existing behavior, deliberately left untouched by gh#222: a chain
    with no declared condition engine at all still wires the conditions
    entity unconditionally. Only a chain with at least one declared engine,
    every one of them registry-sourced, omits it (see the next test)."""
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import TemporalEventRegistryManager
    from kindling_ext_temporal.chain import declare_temporal_chain

    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    _register_base_event(event_registry, entity_registry, pipe_registry, "telemetry.base")

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):
        declare_temporal_chain("t1")

    events_pipe = pipe_registry.get_pipe_definition("temporal.chain.events.t1")
    assert events_pipe.input_entity_ids == ["bronze.telemetry", "silver.conditions.current"]


def test_declare_temporal_chain_registry_only_omits_conditions_current():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager
    from kindling_ext_temporal.chain import declare_temporal_chain

    DataEvents.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())
    condition_registry = MagicMock()
    condition_registry.get_all_conditions.return_value = []

    _register_base_event(event_registry, entity_registry, pipe_registry, "telemetry.base")

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
            condition_registry=condition_registry,
        ),
    ):
        DataEvents.condition_engine(engineid="static_conditions", condition_source="registry")
        declare_temporal_chain("t1")

    events_pipe = pipe_registry.get_pipe_definition("temporal.chain.events.t1")
    assert events_pipe.input_entity_ids == ["bronze.telemetry"]
    assert entity_registry.get_entity_definition("silver.conditions") is None
    assert entity_registry.get_entity_definition("silver.conditions.current") is None


def test_declare_temporal_chain_mixed_sources_still_includes_conditions_current():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import DataEvents, TemporalEventRegistryManager
    from kindling_ext_temporal.chain import declare_temporal_chain

    DataEvents.reset()
    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())
    condition_registry = MagicMock()
    condition_registry.get_all_conditions.return_value = []

    _register_base_event(event_registry, entity_registry, pipe_registry, "telemetry.base")

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
            condition_registry=condition_registry,
        ),
    ):
        DataEvents.condition_engine(engineid="dynamic_conditions", condition_source="table")
        DataEvents.condition_engine(engineid="static_conditions", condition_source="registry")
        declare_temporal_chain("t1")

    events_pipe = pipe_registry.get_pipe_definition("temporal.chain.events.t1")
    assert events_pipe.input_entity_ids == ["bronze.telemetry", "silver.conditions.current"]
    assert entity_registry.get_entity_definition("silver.conditions") is not None
    assert entity_registry.get_entity_definition("silver.conditions.current") is not None


# --- multi-source chains: single-driving-entity guard scoped to the engine -


class _FakeConfigService:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)


def _register_two_base_events_with_different_entities(
    event_registry, entity_registry, pipe_registry
):
    _register_base_event(
        event_registry, entity_registry, pipe_registry, "telemetry.base", "silver.device_telemetry"
    )
    _register_base_event(
        event_registry,
        entity_registry,
        pipe_registry,
        "twin_change.base",
        "silver.device_twin_change",
    )


def test_declare_temporal_chain_multi_source_raises_by_default():
    """No engine configured (or one that doesn't declare
    supports_multi_source_temporal_chain): the single-driving-entity
    restriction still applies exactly as before this fix."""
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import TemporalEventRegistryManager
    from kindling_ext_temporal.chain import declare_temporal_chain

    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())

    _register_two_base_events_with_different_entities(
        event_registry, entity_registry, pipe_registry
    )

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
        ),
    ):
        with pytest.raises(ValueError, match="multiple entities"):
            declare_temporal_chain("t1")


def test_declare_temporal_chain_multi_source_raises_when_engine_explicitly_unsupported():
    """A ConfigService that resolves but doesn't set the flag (e.g. the
    plain OSS pyspark.pipelines engine, which sets owns_incrementality but
    NOT supports_multi_source_temporal_chain) must still raise."""
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import TemporalEventRegistryManager
    from kindling_ext_temporal.chain import declare_temporal_chain

    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())
    config_service = _FakeConfigService({"engine_owns_incrementality": True})

    _register_two_base_events_with_different_entities(
        event_registry, entity_registry, pipe_registry
    )

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
            config_service=config_service,
        ),
    ):
        with pytest.raises(ValueError, match="multiple entities"):
            declare_temporal_chain("t1")


def test_declare_temporal_chain_multi_source_succeeds_when_engine_supports_it():
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import TemporalEventRegistryManager
    from kindling_ext_temporal.chain import declare_temporal_chain

    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())
    config_service = _FakeConfigService({"engine_supports_multi_source_temporal_chain": True})

    _register_two_base_events_with_different_entities(
        event_registry, entity_registry, pipe_registry
    )

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
            config_service=config_service,
        ),
    ):
        pipe_ids = declare_temporal_chain("t1")

    events_pipe = pipe_registry.get_pipe_definition("temporal.chain.events.t1")
    assert events_pipe.input_entity_ids == [
        "silver.device_telemetry",
        "silver.device_twin_change",
        "silver.conditions.current",
    ]
    assert "temporal.chain.events.t1" in pipe_ids


def test_declare_temporal_chain_multi_source_execute_raises_a_clear_error_if_run_directly():
    """Defensive coverage: the composite pipe's execute body is never
    called by the declarative engine that unlocked registration (it
    re-derives everything from the registry directly), but a stray
    generic-engine run of this pipe id must fail loudly, not with a
    confusing KeyError from _chain_events_execute."""
    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager
    from kindling_ext_temporal import TemporalEventRegistryManager
    from kindling_ext_temporal.chain import declare_temporal_chain

    event_registry = TemporalEventRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    pipe_registry = DataPipesManager(_logger_provider())
    config_service = _FakeConfigService({"engine_supports_multi_source_temporal_chain": True})

    _register_two_base_events_with_different_entities(
        event_registry, entity_registry, pipe_registry
    )

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=_temporal_service_get(
            event_registry=event_registry,
            entity_registry=entity_registry,
            pipe_registry=pipe_registry,
            config_service=config_service,
        ),
    ):
        declare_temporal_chain("t1")

    events_pipe = pipe_registry.get_pipe_definition("temporal.chain.events.t1")
    with pytest.raises(RuntimeError, match="multiple driving entities"):
        events_pipe.execute()


class _FakePipeDef:
    def __init__(self, tags):
        self.tags = tags


class _FakePipeRegistry:
    def __init__(self, defs):
        self._defs = dict(defs)

    def get_pipe_ids(self):
        return list(self._defs.keys())

    def get_pipe_definition(self, pipeid):
        return self._defs.get(pipeid)

    def register_pipe(self, pipeid, **params):
        self._defs[pipeid] = _FakePipeDef(params.get("tags", {}))

    def unregister_pipe(self, pipeid):
        self._defs.pop(pipeid, None)


def test_autocollapse_before_run_preserves_registry_order_for_chain_additions():
    """The fixed bug: chain_additions must come from the ORDERED collapse
    result, not a set (which scrambles order under hash randomization)."""
    from kindling_ext_temporal import chain as chain_module

    declared_pipe = "temporal.event.telemetry"
    episodes_chain = "temporal.chain.episodes.default"
    events_chain = "temporal.chain.events.default"

    # declared_pipe is still present at guard-check time -- the real
    # collapse_temporal_chain() call (mocked below) is what would remove it;
    # the handler's own guard only inspects current registry state.
    registry = _FakePipeRegistry(
        {
            declared_pipe: _FakePipeDef({"temporal.lowering": "declared"}),
            episodes_chain: _FakePipeDef({"temporal.lowering": "chain"}),
            events_chain: _FakePipeDef({"temporal.lowering": "chain"}),
        }
    )

    with (
        patch.object(chain_module, "_autocollapse_enabled", return_value=True),
        patch("kindling.injection.GlobalInjector.get", return_value=registry),
        patch.object(
            chain_module,
            "collapse_temporal_chain",
            # Deliberately episodes-before-events: proves the handler preserves
            # whatever order collapse_temporal_chain returns rather than
            # re-deriving its own (or scrambling it through a set).
            return_value=[episodes_chain, events_chain],
        ),
    ):
        pipe_ids = [declared_pipe]
        chain_module._autocollapse_before_run(None, pipe_ids=pipe_ids)

    assert pipe_ids == [episodes_chain, events_chain]


def test_autocollapse_before_run_preserves_unknown_pipe_ids_verbatim():
    """Copilot review finding on PR #218: survivors must drop exactly the
    declared pipes collapse_temporal_chain removed -- never everything not
    present in the post-collapse registry. An unrelated/typo'd pipe id in
    the request (never registered at all, so also absent from the
    post-collapse registry) must survive untouched, so run_datapipes still
    fails loudly on it instead of autocollapse silently swallowing it."""
    from kindling_ext_temporal import chain as chain_module

    declared_pipe = "temporal.event.telemetry"
    events_chain = "temporal.chain.events.default"
    unknown_pipe = "typo.does_not_exist"

    registry = _FakePipeRegistry(
        {
            declared_pipe: _FakePipeDef({"temporal.lowering": "declared"}),
            events_chain: _FakePipeDef({"temporal.lowering": "chain"}),
        }
    )

    with (
        patch.object(chain_module, "_autocollapse_enabled", return_value=True),
        patch("kindling.injection.GlobalInjector.get", return_value=registry),
        patch.object(
            chain_module,
            "collapse_temporal_chain",
            return_value=[events_chain],
        ),
    ):
        pipe_ids = [declared_pipe, unknown_pipe]
        chain_module._autocollapse_before_run(None, pipe_ids=pipe_ids)

    assert pipe_ids == [unknown_pipe, events_chain]


def test_autocollapse_before_run_ignores_unrelated_runs():
    """A run whose pipe_ids never touch a declared pipe must be untouched,
    even if declared pipes exist elsewhere in the registry."""
    from kindling_ext_temporal import chain as chain_module

    registry = _FakePipeRegistry(
        {
            "temporal.event.telemetry": _FakePipeDef({"temporal.lowering": "declared"}),
            "unrelated.pipe": _FakePipeDef({}),
        }
    )

    collapse_spy = MagicMock()
    with (
        patch.object(chain_module, "_autocollapse_enabled", return_value=True),
        patch("kindling.injection.GlobalInjector.get", return_value=registry),
        patch.object(chain_module, "collapse_temporal_chain", collapse_spy),
    ):
        pipe_ids = ["unrelated.pipe"]
        chain_module._autocollapse_before_run(None, pipe_ids=pipe_ids)

    collapse_spy.assert_not_called()
    assert pipe_ids == ["unrelated.pipe"]


def test_autocollapse_before_run_swallows_collapse_failure():
    """A declaration gap (e.g. episodes/condition-engines with zero base
    events) makes collapse_temporal_chain raise -- that must never crash an
    otherwise-unrelated run; the requested pipes just run uncollapsed."""
    from kindling_ext_temporal import chain as chain_module

    declared_pipe = "temporal.episode.foo"
    registry = _FakePipeRegistry({declared_pipe: _FakePipeDef({"temporal.lowering": "declared"})})

    def _raise(chainid="default"):
        raise ValueError("no base events registered")

    with (
        patch.object(chain_module, "_autocollapse_enabled", return_value=True),
        patch("kindling.injection.GlobalInjector.get", return_value=registry),
        patch.object(chain_module, "collapse_temporal_chain", side_effect=_raise),
    ):
        pipe_ids = [declared_pipe]
        chain_module._autocollapse_before_run(None, pipe_ids=pipe_ids)

    assert pipe_ids == [declared_pipe]


def test_autocollapse_disabled_leaves_pipe_ids_untouched():
    from kindling_ext_temporal import chain as chain_module

    with patch.object(chain_module, "_autocollapse_enabled", return_value=False):
        pipe_ids = ["temporal.event.telemetry"]
        chain_module._autocollapse_before_run(None, pipe_ids=pipe_ids)

    assert pipe_ids == ["temporal.event.telemetry"]


def test_ensure_autocollapse_connected_reconnects_after_provider_change():
    """A rebuilt DI container hands out a new SignalProvider instance;
    the hook must reconnect to it rather than staying attached to the old
    one forever (the bug: a bare "connected once" flag would miss this)."""
    from kindling_ext_temporal import chain as chain_module

    chain_module._autocollapse_connected_provider = None
    first_provider = MagicMock()
    first_signal = MagicMock()
    first_provider.get_signal.return_value = None
    first_provider.create_signal.return_value = first_signal

    with patch("kindling.injection.GlobalInjector.get", return_value=first_provider):
        chain_module.ensure_autocollapse_connected()
    first_signal.connect.assert_called_once()

    with patch("kindling.injection.GlobalInjector.get", return_value=first_provider):
        chain_module.ensure_autocollapse_connected()
    # Same provider instance: no reconnect.
    first_signal.connect.assert_called_once()

    second_provider = MagicMock()
    second_signal = MagicMock()
    second_provider.get_signal.return_value = None
    second_provider.create_signal.return_value = second_signal

    with patch("kindling.injection.GlobalInjector.get", return_value=second_provider):
        chain_module.ensure_autocollapse_connected()
    second_signal.connect.assert_called_once()

    chain_module._autocollapse_connected_provider = None
