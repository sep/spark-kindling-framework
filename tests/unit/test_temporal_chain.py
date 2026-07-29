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
    entity_registry=None,
    pipe_registry=None,
):
    from kindling_ext_temporal import (
        SimpleTemporalEntityResolver,
        TemporalEntityResolver,
        TemporalEpisodeRegistry,
        TemporalEpisodeRegistryManager,
        TemporalEventRegistry,
    )

    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry

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
        if dep is DataEntityRegistry and entity_registry is not None:
            return entity_registry
        if dep is DataPipesRegistry and pipe_registry is not None:
            return pipe_registry
        raise AssertionError(f"Unexpected service request: {dep}")

    return _get


def _register_base_event(event_registry, entity_registry, pipe_registry, eventid):
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
            input_entity_id="bronze.telemetry",
            subject_type="machine",
            subject_keys=["machine_id"],
            time_column="event_ts",
            event_type="telemetry.observed",
        )
        def normalize(df):
            return df


def test_collapse_temporal_chain_unregisters_declared_pipes():
    from kindling_ext_temporal import TemporalEventRegistryManager
    from kindling_ext_temporal.chain import (
        chain_events_pipe_id,
        collapse_temporal_chain,
    )

    from kindling.data_entities import DataEntityManager
    from kindling.data_pipes import DataPipesManager

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
