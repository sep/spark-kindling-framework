"""The declared stratum count follows rule depth, not the ceiling.

``kindling.temporal.max_generations`` is a ceiling — the lowering rejects a
rule set that reaches past it — but the topology used to be built from it
directly, so a single-condition app declared eleven strata at the default of
10, nine of them permanently empty. Emission and ``validate()``'s
name-reservation path must agree exactly, and reservation runs with no Spark
session, so the count can only collapse where the depth is knowable
in-process: registry-declared rules, which are code.
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


def _logger_provider():
    provider = MagicMock()
    provider.get_logger.return_value = MagicMock()
    return provider


def _rule(condition_id, consumes, enabled=True):
    from kindling_ext_temporal.validation import ConditionRule

    return ConditionRule(
        condition_id=condition_id,
        consumes_event_type=list(consumes),
        subject_type="shower",
        parameters={"enter_when": lambda df: df, "exit_when": lambda df: df},
        enabled=enabled,
    )


def _count(*, condition_source, registry_conditions=(), max_generations=10):
    """Register engines/conditions, then resolve the declared stratum count."""
    from kindling_ext_databricks import temporal_lowering
    from kindling_ext_temporal import (
        TemporalConditionRegistry,
        TemporalConditionRegistryManager,
        TemporalEventRegistry,
        TemporalEventRegistryManager,
    )

    event_registry = TemporalEventRegistryManager(_logger_provider())
    condition_registry = TemporalConditionRegistryManager(_logger_provider())
    for rule in registry_conditions:
        condition_registry.register_condition(rule)
    event_registry.register_condition_engine(
        "default",
        events_entity_id="silver.events",
        condition_source=condition_source,
        tags={},
    )

    services = {
        TemporalEventRegistry: event_registry,
        TemporalConditionRegistry: condition_registry,
    }
    with patch("kindling.injection.GlobalInjector.get", side_effect=services.__getitem__):
        return temporal_lowering.declared_stratum_count(max_generations)


def test_a_single_generation_registry_chain_declares_one_stratum():
    rule = _rule("cond.remote_warmup_done", ["shower.remote_warmup_started"])
    assert _count(condition_source="registry", registry_conditions=[rule]) == 1


def test_a_two_generation_registry_chain_declares_two():
    first = _rule("cond.first", ["shower.remote_warmup_started"])
    second = _rule("cond.second", ["cond.first.entered"])
    assert _count(condition_source="registry", registry_conditions=[first, second]) == 2


def test_disabled_registry_rules_do_not_deepen_the_topology():
    first = _rule("cond.first", ["shower.remote_warmup_started"])
    second = _rule("cond.second", ["cond.first.entered"], enabled=False)
    assert _count(condition_source="registry", registry_conditions=[first, second]) == 1


def test_the_ceiling_still_caps_a_deeper_registry_chain():
    first = _rule("cond.first", ["shower.remote_warmup_started"])
    second = _rule("cond.second", ["cond.first.entered"])
    assert (
        _count(condition_source="registry", registry_conditions=[first, second], max_generations=1)
        == 1
    )


def test_a_table_sourced_chain_keeps_the_ceiling():
    """Rule rows change between updates and reservation cannot read them."""
    assert _count(condition_source="table") == 10


def test_a_registry_chain_with_no_rules_yet_keeps_the_ceiling():
    assert _count(condition_source="registry") == 10


def test_unavailable_registries_fall_back_to_the_ceiling():
    """A degraded lookup must never fail a declaration pass."""
    from kindling_ext_databricks import temporal_lowering

    with patch(
        "kindling.injection.GlobalInjector.get",
        side_effect=RuntimeError("no injector"),
    ):
        assert temporal_lowering.declared_stratum_count(10) == 10


def test_emission_and_reservation_agree_on_the_collapsed_topology():
    """The reserved names must be exactly the datasets the lowering emits."""
    from kindling.data_entities import (
        DataEntityManager,
        DataEntityRegistry,
        EntityMetadata,
        EntityNameMapper,
    )
    from kindling.data_pipes import DataPipesManager, DataPipesRegistry
    from kindling_ext_databricks import DatabricksSdpEngine, temporal_lowering
    from kindling_ext_temporal import (
        DataConditions,
        DataEpisodes,
        DataEvents,
        SimpleTemporalEntityResolver,
        TemporalConditionRegistry,
        TemporalConditionRegistryManager,
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
    condition_registry = TemporalConditionRegistryManager(_logger_provider())
    entity_registry = DataEntityManager()
    entity_registry.registry["bronze.showers"] = EntityMetadata(
        entityid="bronze.showers", name="showers", schema=None, merge_columns=[], tags={}
    )
    pipe_registry = DataPipesManager(_logger_provider())
    services = {
        TemporalEntityResolver: SimpleTemporalEntityResolver(),
        TemporalEventRegistry: event_registry,
        TemporalEpisodeRegistry: episode_registry,
        TemporalConditionRegistry: condition_registry,
        DataEntityRegistry: entity_registry,
        DataPipesRegistry: pipe_registry,
        EntityNameMapper: SimpleNamespace(
            get_table_name=lambda entity: f"cat.sch.{entity.entityid}"
        ),
    }

    with patch("kindling.injection.GlobalInjector.get", side_effect=services.__getitem__):
        DataConditions.reset()

        @DataEvents.base_event(
            eventid="shower.base",
            input_entity_id="bronze.showers",
            subject_type="shower",
            subject_keys=["shower_id"],
            time_column="observed_ts",
            event_type="shower.remote_warmup_started",
            payload_columns=["temperature"],
        )
        def normalize(df):
            return df

        DataConditions.register(
            condition_id="cond.remote_warmup_done",
            consumes_event_type=["shower.remote_warmup_started"],
            subject_type="shower",
            enter_when=lambda df: df,
            exit_when=lambda df: df,
        )
        DataEvents.condition_engine(engineid="default", condition_source="registry")
        selected = declare_temporal_chain()

        engine = DatabricksSdpEngine(entity_registry, pipe_registry, dp_module=MagicMock())
        chain_pipe = pipe_registry.get_pipe_definition(selected[0])
        reserved = {name for _owner, name in engine._emitted_dataset_names(chain_pipe)}

        spark = MagicMock()
        with patch.object(temporal_lowering, "_spark", lambda: spark):
            assert temporal_lowering.declared_stratum_count(10) == 1

    strata = {name for name in reserved if "__g" in name}
    assert strata == {"silver_events__g0", "silver_events__g1"}
