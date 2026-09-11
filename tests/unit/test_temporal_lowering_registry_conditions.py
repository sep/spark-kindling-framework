"""Registry-declared conditions in the stratified Lakeflow lowering.

The lowering used to resolve rules from the conditions table only, so an
app declaring its conditions through ``DataConditions.register`` lowered a
pipeline that emitted no boundary events at all: episodes opened by a base
event and closed by a registry-backed condition ran to their synthetic
expiry with no resolvable end. These tests pin the parity with
``declare_temporal_chain``, the layering over the combined rule set, and
the diagnostics that keep a silent empty rule set from recurring.
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
    """Records every declaration the lowering emits, query bodies included."""

    def __init__(self):
        self.streaming_tables = []
        self.append_flows = {}
        self.auto_cdc_snapshot_flows = []
        self.views = {}
        self.materialized_views = {}

    def create_streaming_table(self, name, **_kwargs):
        self.streaming_tables.append(name)

    def append_flow(self, target, name=None, **_kwargs):
        def decorator(fn):
            self.append_flows[name or fn.__name__] = (target, fn)
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


def _rule(condition_id, consumes, subject_type="shower"):
    """A registry-shaped ConditionRule, built the way the registry builds it."""
    from kindling_ext_temporal.validation import ConditionRule

    return ConditionRule(
        condition_id=condition_id,
        consumes_event_type=list(consumes),
        subject_type=subject_type,
        parameters={"enter_when": lambda df: df, "exit_when": lambda df: df},
    )


def _declare(
    *,
    condition_source="registry",
    registry_conditions=(),
    mode="batch",
    max_generations=2,
    spark=None,
):
    """Declare one chain through the lowering; return ``(dp, spark)``.

    Batch mode by default so the per-stratum query bodies land in
    ``dp.materialized_views`` where a test can invoke them and observe which
    rules the stratum actually runs.
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

    def service_get(dep):
        try:
            return services[dep]
        except KeyError as exc:
            raise AssertionError(f"Unexpected service request: {dep}") from exc

    spark = spark or MagicMock()
    with patch("kindling.injection.GlobalInjector.get", side_effect=service_get):
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

        for rule in registry_conditions:
            DataConditions.register(
                condition_id=rule.condition_id,
                consumes_event_type=rule.consumes_event_type,
                subject_type=rule.subject_type,
                enter_when=rule.parameters["enter_when"],
                exit_when=rule.parameters["exit_when"],
            )

        DataEvents.condition_engine(engineid="default", condition_source=condition_source)
        DataEpisodes.episode(
            episodeid="episode.remote_warmup",
            start_event="shower.remote_warmup_started",
            end_event="cond.remote_warmup_done.entered",
            subject_type="shower",
            expires_after_seconds=1800,
        )
        declare_temporal_chain()

        with patch.object(temporal_lowering, "_spark", lambda: spark):
            dp = FakeDp()
            temporal_lowering.declare_stratified_temporal(
                dp,
                events_name="silver_events",
                episodes_name="silver_episodes",
                max_generations=max_generations,
                mode=mode,
            )
    return dp, spark


def _rules_run_by_strata(dp, spark):
    """Invoke each boundary stratum body; return {stratum: [condition_id]}."""
    from kindling_ext_temporal.engine import ConditionEngineRunner

    seen = {}

    def record(self, events, rules):
        seen.setdefault(_current[0], []).extend(rule.condition_id for rule in rules)
        return events

    _current = [None]
    with patch.object(ConditionEngineRunner, "execute_rules", record):
        for name, body in dp.materialized_views.items():
            if "__g" not in name or name.endswith("__g0"):
                continue
            _current[0] = name
            body()
    return seen


def test_registry_rules_reach_the_boundary_stratum():
    """The regression: a registry-declared rule must actually be run.

    ``max_generations=1`` keeps every declared stratum non-empty: an empty
    stratum's body is a ``F.lit(False)`` filter, which needs a live Spark
    context to build and so cannot be invoked in a hermetic test.
    """
    rule = _rule("cond.remote_warmup_done", ["shower.remote_warmup_started"])
    dp, spark = _declare(registry_conditions=[rule], max_generations=1)

    assert _rules_run_by_strata(dp, spark) == {
        "silver_events__g1": ["cond.remote_warmup_done"],
    }


def test_registry_only_chain_never_reads_the_conditions_table():
    """Parity with declare_temporal_chain: no table involvement at all."""
    rule = _rule("cond.remote_warmup_done", ["shower.remote_warmup_started"])
    spark = MagicMock()
    _declare(registry_conditions=[rule], spark=spark)

    read_tables = [call.args[0] for call in spark.read.table.call_args_list if call.args]
    assert not any("conditions" in name for name in read_tables)


def test_table_sourced_engine_still_reads_the_conditions_table():
    """The pre-existing path is untouched by the registry support."""
    spark = MagicMock()
    _declare(condition_source="table", spark=spark)

    read_tables = [call.args[0] for call in spark.read.table.call_args_list if call.args]
    assert "cat.sch.silver.conditions" in read_tables


def test_layering_uses_the_combined_graph_not_one_source():
    """A rule consuming another source's output belongs one stratum later.

    Layering per source would assign the consumer the default generation 1
    and run it in the same stratum as the rule it depends on — in a fixed
    topology that silently drops its input.
    """
    from kindling_ext_temporal.validation import layer_rules_by_generation

    first = _rule("cond.first", ["shower.remote_warmup_started"])
    second = _rule("cond.second", ["cond.first.entered"])

    by_generation, max_generation = layer_rules_by_generation([first, second])

    assert max_generation == 2
    assert [rule.condition_id for rule in by_generation[1]] == ["cond.first"]
    assert [rule.condition_id for rule in by_generation[2]] == ["cond.second"]


def test_layering_of_an_empty_rule_set_is_generation_zero():
    from kindling_ext_temporal.validation import layer_rules_by_generation

    assert layer_rules_by_generation([]) == ({}, 0)


def test_combine_drops_disabled_rules():
    """``enabled=False`` must mean the rule does not run.

    ``validate()`` already excludes disabled table rows, but
    ``get_all_conditions()`` returns every registration and
    ``execute_rules`` runs whatever it is handed — so the filter has to
    live where the two sources meet.
    """
    from dataclasses import replace

    from kindling_ext_temporal.validation import combine_condition_rules

    enabled = _rule("cond.on", ["shower.remote_warmup_started"])
    disabled = replace(_rule("cond.off", ["shower.remote_warmup_started"]), enabled=False)

    combined = combine_condition_rules(
        [],
        [enabled, disabled],
        has_table_engine=False,
        has_registry_engine=True,
    )

    assert [rule.condition_id for rule in combined] == ["cond.on"]


def test_a_storage_path_failure_is_not_treated_as_a_first_run():
    """A registered table whose data is unreadable is corruption, not absence."""
    from kindling_ext_databricks import temporal_lowering

    spark = MagicMock()
    spark.read.table.side_effect = Exception(
        "[PATH_NOT_FOUND] Path does not exist: abfss://lake/silver/conditions"
    )
    entity = SimpleNamespace(entityid="silver.conditions", tags={})

    with patch.object(
        temporal_lowering, "_physical_table_name", lambda _entity: "silver.conditions"
    ):
        with pytest.raises(Exception, match="PATH_NOT_FOUND"):
            temporal_lowering._read_table_rules(spark, entity)


def test_combine_rejects_a_condition_id_declared_in_both_sources():
    from kindling_ext_temporal.validation import (
        ConditionValidationError,
        combine_condition_rules,
    )

    duplicate = "cond.remote_warmup_done"
    with pytest.raises(ConditionValidationError, match=duplicate):
        combine_condition_rules(
            [_rule(duplicate, ["shower.remote_warmup_started"])],
            [_rule(duplicate, ["shower.remote_warmup_started"])],
            has_table_engine=True,
            has_registry_engine=True,
        )


def test_combine_returns_both_sources_when_they_do_not_collide():
    from kindling_ext_temporal.validation import combine_condition_rules

    combined = combine_condition_rules(
        [_rule("cond.table", ["shower.remote_warmup_started"])],
        [_rule("cond.registry", ["shower.remote_warmup_started"])],
        has_table_engine=True,
        has_registry_engine=True,
    )

    assert [rule.condition_id for rule in combined] == ["cond.table", "cond.registry"]


def test_a_missing_conditions_table_is_still_the_first_run_path():
    from kindling_ext_databricks import temporal_lowering

    spark = MagicMock()
    spark.read.table.side_effect = Exception(
        "[TABLE_OR_VIEW_NOT_FOUND] The table or view `silver`.`conditions` cannot be found"
    )
    entity = SimpleNamespace(entityid="silver.conditions", tags={})

    with patch.object(
        temporal_lowering, "_physical_table_name", lambda _entity: "silver.conditions"
    ):
        assert temporal_lowering._read_table_rules(spark, entity) == []


def test_a_conditions_read_failure_is_no_longer_swallowed():
    """Anything but an absent table must fail loudly, not lower zero rules."""
    from kindling_ext_databricks import temporal_lowering

    spark = MagicMock()
    spark.read.table.side_effect = Exception("PERMISSION_DENIED: user lacks SELECT on conditions")
    entity = SimpleNamespace(entityid="silver.conditions", tags={})

    with patch.object(
        temporal_lowering, "_physical_table_name", lambda _entity: "silver.conditions"
    ):
        with pytest.raises(Exception, match="PERMISSION_DENIED"):
            temporal_lowering._read_table_rules(spark, entity)
