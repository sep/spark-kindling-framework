"""Chained lowering: the temporal graph as two ordinary Kindling pipes.

The per-declaration lowering (translation.py) registers N pipes that
communicate by round-tripping through the shared events table — the
condition engine and determination pipes read AND write ``silver.events``
(self-reads), and every base-event pipe writes it too (multi-writer). That
shape forces one scheduled run per generation hop, couples pipes through
persisted state mid-run, and cannot be declared on engines that require an
acyclic single-writer dataset graph (Lakeflow/SDP).

``declare_temporal_chain`` lowers the SAME registered declarations into two
composite pipes that any Kindling execution engine can run as-is:

``temporal.chain.events.<chainid>``
    inputs: the base events' shared driving entity (watermarked) + the
    conditions current view. Sole writer of the events entity. Its body
    computes base envelopes, then loops: condition boundary passes over the
    newest stratum, episode-determination events over the accumulated
    union (prior episode state via the existing engine-owned read) — until
    quiescence or ``kindling.temporal.max_generations``. Higher-order
    conditions (consuming determination events) therefore converge in ONE
    run instead of one run per generation.

``temporal.chain.episodes.<chainid>``
    input: the events entity (watermarked — its slice is exactly the strata
    the events pipe just persisted). Sole writer of the episodes entity.
    Pairs boundaries per episode declaration against prior state and merges
    by ``episode_id``.

Determination events and episode rows both derive from the same persisted
events slice and the same pre-revision prior state, so the
revision-ordering compensation (``reconstruct_batch_closed``) is
structurally unnecessary here (it remains harmless: pre-revision state has
no batch-closed rows to reconstruct).

Declaration-only apps call this AFTER all ``DataEvents``/``DataEpisodes``
declarations; the per-declaration pipes stay registered and independently
executable — the chain is an alternative lowering over the same metadata,
not a replacement.

``collapse_temporal_chain`` (below) is the one apps should reach for in
practice: it does everything ``declare_temporal_chain`` does, then also
unregisters the per-declaration pipes the chain supersedes, so a run over
"every registered pipe" doesn't execute both lowerings at once. It runs
automatically before every ``run_datapipes`` call that touches a
per-declaration temporal pipe — see ``kindling.temporal.autocollapse`` —
so most apps never need to call either function directly; keep
``declare_temporal_chain`` around when you deliberately want both
lowerings registered side by side (e.g. inspecting one hop in isolation).

Phase-1 constraint: all base events must share one input entity (the
chain's driving source). Heterogeneous bronze sources should normalize
into a shared staging entity first; native multi-source chaining is a
planned follow-up.
"""

from collections import Counter
from functools import reduce
from typing import Any, Dict, List, Optional

from kindling.data_entities import DataEntityRegistry
from kindling.data_pipes import DataPipesRegistry
from kindling.injection import GlobalInjector

from .entities import TemporalEntityResolver
from .registry import (
    TemporalConditionRegistry,
    TemporalEpisodeRegistry,
    TemporalEventRegistry,
)
from .translation import (
    TEMPORAL_LOWERING_CHAIN,
    TEMPORAL_LOWERING_DECLARED,
    TEMPORAL_LOWERING_TAG,
    TemporalPipeTranslator,
    parse_bool_config,
)

CHAIN_EVENTS_PIPE_PREFIX = "temporal.chain.events."
CHAIN_EPISODES_PIPE_PREFIX = "temporal.chain.episodes."
MAX_GENERATIONS_CONFIG_KEY = "kindling.temporal.max_generations"
DEFAULT_MAX_GENERATIONS = 10

AUTOCOLLAPSE_CONFIG_KEY = "kindling.temporal.autocollapse"
DEFAULT_AUTOCOLLAPSE = True


def chain_events_pipe_id(chainid: str) -> str:
    return f"{CHAIN_EVENTS_PIPE_PREFIX}{chainid}"


def chain_episodes_pipe_id(chainid: str) -> str:
    return f"{CHAIN_EPISODES_PIPE_PREFIX}{chainid}"


def _checkpoint(df):
    """Materialize a stratum so multi-consumer reuse doesn't recompute it.

    ``localCheckpoint`` also truncates lineage, keeping plans bounded as
    generations stack; executor-local storage is job-scoped so nothing
    needs explicit cleanup.
    """
    if not hasattr(df, "localCheckpoint"):  # pragma: no cover - exotic runtimes
        return df.persist()
    return df.localCheckpoint(eager=True)


def _union(frames):
    return reduce(lambda left, right: left.unionByName(right), frames)


def _resolve_max_generations(entity_dfs: Dict[str, Any]) -> int:
    if "temporal_max_generations" in entity_dfs:
        return int(entity_dfs["temporal_max_generations"])
    try:
        from kindling.spark_config import ConfigService

        value = GlobalInjector.get(ConfigService).get(MAX_GENERATIONS_CONFIG_KEY, None)
    except Exception:  # noqa: BLE001 - config service unavailable in bare tests
        return DEFAULT_MAX_GENERATIONS
    if value is None:
        return DEFAULT_MAX_GENERATIONS
    # A malformed value must be loud, not silently reverted to the default.
    return int(value)


def _chain_events_execute(
    driving_entity_id,
    conditions_current_id,
    base_defs,
    episode_defs,
    *,
    has_table_engine,
    has_registry_engine,
):
    """Build the events-chain body: strata in memory, one returned frame.

    ``conditions_current_id`` is ``None`` for a purely-registry chain (at
    least one condition engine declared, none of them table-sourced) — the
    conditions entity is never read at all in that case. A chain with zero
    declared condition engines still receives a real
    ``conditions_current_id`` (pre-existing behavior, left untouched — that
    edge case isn't part of this feature), so gating the table read on
    ``conditions_current_id is not None`` (rather than on
    ``has_table_engine``) is what keeps it unchanged.

    ``has_table_engine``/``has_registry_engine`` reflect which kinds of
    condition engine are actually declared for this chain; they gate
    whether registry rules are pulled in and whether the cross-source cycle
    check below runs.
    """

    def execute(**entity_dfs):
        driving_key = driving_entity_id.replace(".", "_")
        try:
            driving_df = entity_dfs[driving_key]
        except KeyError as exc:
            available = ", ".join(sorted(entity_dfs.keys()))
            raise ValueError(
                f"Temporal events chain expected input '{driving_key}', got: {available}"
            ) from exc

        from .engine import ConditionEngineRunner, EpisodeRunner
        from .validation import (
            ActiveSparkSqlExpressionParser,
            ConditionValidationError,
            TemporalConditionValidator,
        )

        evaluation_time = TemporalPipeTranslator.resolve_evaluation_time(entity_dfs)

        stratum = _checkpoint(
            _union(
                [
                    TemporalPipeTranslator.select_event_envelope(
                        metadata.transform(driving_df) if metadata.transform else driving_df,
                        metadata,
                    )
                    for metadata in base_defs
                ]
            )
        )

        table_rules: List[Any] = []
        if conditions_current_id is not None:
            conditions_key = conditions_current_id.replace(".", "_")
            try:
                conditions_df = entity_dfs[conditions_key]
            except KeyError as exc:
                available = ", ".join(sorted(entity_dfs.keys()))
                raise ValueError(
                    f"Temporal events chain expected input '{conditions_key}', got: {available}"
                ) from exc
            validator = TemporalConditionValidator(
                expression_parser=ActiveSparkSqlExpressionParser(driving_df.sparkSession)
            )
            table_rules = validator.validate_or_raise(conditions_df.collect()).valid_rules

        registry_rules = (
            GlobalInjector.get(TemporalConditionRegistry).get_all_conditions()
            if has_registry_engine
            else []
        )
        combined_rules = table_rules + registry_rules

        # Each source validates its own rules in isolation -- table rows via
        # validate_or_raise above, registry rules at condition_engine()
        # declaration time (registry.py). Neither ever checks a rule from
        # ONE source consuming a produced event type from the OTHER, nor a
        # condition_id collision between the two sources: the genuinely new
        # cross-source interactions this feature introduces, so both are
        # checked here, once, whenever both kinds of engine are declared on
        # this chain.
        if has_table_engine and has_registry_engine and combined_rules:
            id_counts = Counter(rule.condition_id for rule in combined_rules)
            duplicate_ids = sorted(
                condition_id for condition_id, count in id_counts.items() if count > 1
            )
            if duplicate_ids:
                raise ConditionValidationError(
                    "Conditions set is not ingestible: condition_id(s) "
                    f"{', '.join(duplicate_ids)} are declared in both the table and "
                    "registry sources -- condition_id drives the "
                    "{condition_id}.entered/.exited boundary event types, so a "
                    "collision would produce ambiguous, duplicated events"
                )

            cross_validator = TemporalConditionValidator()
            graph = cross_validator.build_event_type_graph(combined_rules)
            cycles = cross_validator.graph_builder.detect_cycles(graph)
            if cycles:
                raise ConditionValidationError(f"Conditions set is not ingestible:\n{cycles[0]}")

        # Prior episode state is resolved ONCE, before anything persists —
        # both this pipe's determination events and (later) the episodes
        # pipe see the same pre-revision state.
        priors = {
            episode.episodeid: TemporalPipeTranslator.resolve_prior_episodes(entity_dfs, episode)
            for episode in episode_defs
        }

        engine = ConditionEngineRunner()
        episode_runner = EpisodeRunner()
        accumulated = stratum
        for _ in range(_resolve_max_generations(entity_dfs)):
            fresh: List[Any] = []

            if combined_rules:
                boundaries = _checkpoint(engine.execute_rules(stratum, combined_rules))
                if not boundaries.isEmpty():
                    accumulated = accumulated.unionByName(boundaries)
                    fresh.append(boundaries)

            if episode_defs:
                determinations = _union(
                    [
                        episode_runner.execute_determination_events(
                            accumulated,
                            episode,
                            evaluation_time=evaluation_time,
                            existing_episodes_df=priors[episode.episodeid],
                        )
                        for episode in episode_defs
                    ]
                )
                new_determinations = _checkpoint(
                    determinations.join(
                        accumulated.select("event_id"), on="event_id", how="left_anti"
                    )
                )
                if not new_determinations.isEmpty():
                    accumulated = accumulated.unionByName(new_determinations)
                    fresh.append(new_determinations)

            if not fresh:
                break
            stratum = _union(fresh)

        return accumulated.dropDuplicates(["event_id"])

    return execute


def _chain_episodes_execute(events_entity_id, episode_defs):
    """Build the episodes body: pair every declaration, one merged frame."""

    def execute(**entity_dfs):
        events_key = events_entity_id.replace(".", "_")
        try:
            events_df = entity_dfs[events_key]
        except KeyError as exc:
            available = ", ".join(sorted(entity_dfs.keys()))
            raise ValueError(
                f"Temporal episodes chain expected input '{events_key}', got: {available}"
            ) from exc

        from .engine import EpisodeRunner

        evaluation_time = TemporalPipeTranslator.resolve_evaluation_time(entity_dfs)
        runner = EpisodeRunner()
        return _union(
            [
                runner.execute(
                    events_df,
                    episode,
                    evaluation_time=evaluation_time,
                    existing_episodes_df=TemporalPipeTranslator.resolve_prior_episodes(
                        entity_dfs, episode
                    ),
                )
                for episode in episode_defs
            ]
        )

    return execute


def declare_temporal_chain(chainid: str = "default") -> List[str]:
    """Lower all registered temporal declarations into the two chain pipes.

    Call after every ``DataEvents.base_event`` / ``DataEvents
    .condition_engine`` / ``DataEpisodes.episode`` declaration. Returns the
    registered pipe ids in execution order — pass them straight to
    ``run_datapipes``.
    """
    event_registry = GlobalInjector.get(TemporalEventRegistry)
    episode_registry = GlobalInjector.get(TemporalEpisodeRegistry)
    entity_registry = GlobalInjector.get(DataEntityRegistry)
    pipe_registry = GlobalInjector.get(DataPipesRegistry)
    resolver = GlobalInjector.get(TemporalEntityResolver)

    base_defs = [
        event_registry.get_base_event_definition(eventid)
        for eventid in event_registry.get_base_event_ids()
    ]
    episode_defs = [
        episode_registry.get_episode_definition(episodeid)
        for episodeid in episode_registry.get_episode_ids()
    ]
    if not base_defs:
        raise ValueError(
            f"Temporal chain '{chainid}': no base events are registered; declare "
            "DataEvents.base_event(...) before declaring the chain."
        )

    driving_entities = sorted({metadata.input_entity_id for metadata in base_defs})
    if len(driving_entities) > 1:
        raise ValueError(
            f"Temporal chain '{chainid}': base events read from multiple entities "
            f"({', '.join(driving_entities)}); the chain needs one driving entity. "
            "Normalize heterogeneous sources into a shared staging entity first."
        )
    driving_entity_id = driving_entities[0]

    # A chain with zero declared condition engines still wires the
    # conditions entity unconditionally -- pre-existing behavior, left
    # untouched (that edge case isn't part of this feature). A chain with at
    # least one declared engine omits it only when EVERY declared engine is
    # registry-sourced: a purely-registry chain reads events only, per the
    # proposal's compatibility section.
    engine_defs = [
        event_registry.get_condition_engine_definition(engineid)
        for engineid in event_registry.get_condition_engine_ids()
    ]
    has_table_engine = any(metadata.condition_source == "table" for metadata in engine_defs)
    has_registry_engine = any(metadata.condition_source == "registry" for metadata in engine_defs)
    include_conditions_current = not engine_defs or has_table_engine

    events_entity = resolver.get_events_entity()
    TemporalPipeTranslator.ensure_entity(entity_registry, events_entity)

    conditions_current_id = None
    if include_conditions_current:
        conditions_entity = resolver.get_conditions_entity()
        conditions_current_id = resolver.get_conditions_current_entity_id()
        TemporalPipeTranslator.ensure_entity(entity_registry, conditions_entity)

    events_input_entity_ids = [driving_entity_id]
    if conditions_current_id is not None:
        events_input_entity_ids.append(conditions_current_id)

    events_pipe = chain_events_pipe_id(chainid)
    pipe_registry.register_pipe(
        events_pipe,
        name=f"Temporal events chain: {chainid}",
        execute=_chain_events_execute(
            driving_entity_id,
            conditions_current_id,
            base_defs,
            episode_defs,
            has_table_engine=has_table_engine,
            has_registry_engine=has_registry_engine,
        ),
        tags={
            "pipe_type": "temporal.chain_events",
            "temporal.kind": "chain_events",
            TEMPORAL_LOWERING_TAG: TEMPORAL_LOWERING_CHAIN,
            "temporal.chain_id": chainid,
            "temporal.reads_prior_state": "true",
        },
        input_entity_ids=events_input_entity_ids,
        output_entity_id=events_entity.entityid,
        output_type=(events_entity.tags or {}).get("provider_type", "delta"),
        use_watermark=True,
    )

    pipe_ids = [events_pipe]
    if episode_defs:
        episodes_entity = resolver.get_episodes_entity()
        TemporalPipeTranslator.ensure_entity(entity_registry, episodes_entity)
        episodes_pipe = chain_episodes_pipe_id(chainid)
        pipe_registry.register_pipe(
            episodes_pipe,
            name=f"Temporal episodes chain: {chainid}",
            execute=_chain_episodes_execute(events_entity.entityid, episode_defs),
            tags={
                "pipe_type": "temporal.chain_episodes",
                "temporal.kind": "chain_episodes",
                TEMPORAL_LOWERING_TAG: TEMPORAL_LOWERING_CHAIN,
                "temporal.chain_id": chainid,
                "temporal.reads_prior_state": "true",
            },
            input_entity_ids=[events_entity.entityid],
            output_entity_id=episodes_entity.entityid,
            output_type=(episodes_entity.tags or {}).get("provider_type", "delta"),
            use_watermark=True,
        )
        pipe_ids.append(episodes_pipe)

    return pipe_ids


def _pipe_tags(pipe_registry: DataPipesRegistry, pipeid: str) -> Dict[str, str]:
    definition = pipe_registry.get_pipe_definition(pipeid)
    return (definition.tags or {}) if definition is not None else {}


def _pipe_ids_tagged(pipe_registry: DataPipesRegistry, lowering: str) -> List[str]:
    """Every currently-registered pipe tagged ``temporal.lowering=<lowering>``.

    Never by pipeid prefix: a declaration's ``pipeid`` is user-overridable
    (``BaseEventMetadata.pipeid`` etc.), so the tag set in translation.py/
    chain.py is the only reliable signal. Registry order preserved (Python
    dicts are insertion-ordered) so callers that care about relative order
    -- e.g. the events chain pipe must run before the episodes chain pipe
    -- can rely on it; never route this through a ``set``.
    """
    return [
        pipeid
        for pipeid in pipe_registry.get_pipe_ids()
        if _pipe_tags(pipe_registry, pipeid).get(TEMPORAL_LOWERING_TAG) == lowering
    ]


def collapse_temporal_chain(chainid: str = "default") -> List[str]:
    """Lower the registered temporal declarations into the chain pipes AND
    retire the per-declaration pipes they supersede.

    ``declare_temporal_chain`` only ever adds the two composite chain pipes,
    leaving every base-event/condition-engine/episode/episode-event pipe
    ``DataEvents``/``DataEpisodes`` registered eagerly still sitting in the
    registry -- so a naive "run everything registered" ends up executing
    both lowerings over the same declarations. That combination is never
    wanted: the per-declaration pipes are the broken, cyclic, multi-writer
    lowering the chain exists to replace (see the module docstring).

    This collapses: every pipe tagged ``temporal.lowering=declared`` is
    unregistered, and the two chain pipes take their place. Returns every
    pipe id left in the registry afterward (untouched pipes + the chain
    pipes), in registry order -- pass it straight to ``run_datapipes``.

    Idempotent: a second call finds nothing left tagged ``declared`` and
    just re-confirms the chain pipes.
    """
    pipe_registry = GlobalInjector.get(DataPipesRegistry)
    superseded = _pipe_ids_tagged(pipe_registry, TEMPORAL_LOWERING_DECLARED)

    declare_temporal_chain(chainid)

    for pipeid in superseded:
        pipe_registry.unregister_pipe(pipeid)

    return list(pipe_registry.get_pipe_ids())


def _autocollapse_enabled() -> bool:
    try:
        from kindling.spark_config import ConfigService

        value = GlobalInjector.get(ConfigService).get(AUTOCOLLAPSE_CONFIG_KEY, None)
    except Exception:  # noqa: BLE001 - config service unavailable in bare tests
        return DEFAULT_AUTOCOLLAPSE
    return parse_bool_config(value, default=DEFAULT_AUTOCOLLAPSE)


def _autocollapse_before_run(sender, *, pipe_ids=None, **kwargs) -> None:
    """``datapipes.before_run`` handler: auto-collapse a run that touches
    per-declaration temporal pipes, in place, before execution starts.

    Scoped to the run at hand: only fires when THIS run's requested
    ``pipe_ids`` actually reference a ``declared`` pipe -- an unrelated run
    (some other pipe entirely) is left untouched even if temporal
    declarations exist elsewhere in the process. ``pipe_ids`` is the exact
    list object the emitting call site is about to iterate; mutating it in
    place (rather than returning a new list, which a signal receiver's
    return value can't feed back in) is what makes the swap land before
    that iteration starts -- see ``run_datapipes``/``run_datapipes_dag`` in
    ``kindling.data_pipes``, both of which now emit this signal before
    handing ``pipes`` to their respective execution path.

    ``collapse_temporal_chain`` can raise (e.g. episodes/condition-engines
    declared with zero base events) -- caught here so a declaration gap
    elsewhere never crashes an otherwise-unrelated run; the requested pipes
    just execute uncollapsed, same as if autocollapse were off.

    Survivors are computed by removing exactly the pipe ids tagged
    ``declared`` *in this request* (captured before collapsing) -- never by
    intersecting the request with "what's still in the registry after
    collapse". The latter would also silently drop any unrelated/typo'd
    pipe id the caller passed (never registered to begin with, so absent
    from the post-collapse registry too), swallowing a bug that would
    otherwise fail loudly when ``run_datapipes`` tries to resolve it.

    Disable with ``kindling.temporal.autocollapse: false`` (e.g. to run one
    granular per-declaration pipe standalone for debugging).
    """
    if pipe_ids is None or not _autocollapse_enabled():
        return

    pipe_registry = GlobalInjector.get(DataPipesRegistry)
    requested = list(pipe_ids)
    declared_requested = {
        pipeid
        for pipeid in requested
        if _pipe_tags(pipe_registry, pipeid).get(TEMPORAL_LOWERING_TAG)
        == TEMPORAL_LOWERING_DECLARED
    }
    if not declared_requested:
        return

    try:
        remaining_ordered = collapse_temporal_chain()
    except Exception:  # noqa: BLE001 - a declaration gap must not crash an unrelated run
        return

    survivors = [pipeid for pipeid in requested if pipeid not in declared_requested]
    # Iterate remaining_ordered (registry/insertion order), not a set, so the
    # events-chain pipe always lands before the episodes-chain pipe -- a set
    # would scramble that via hash randomization.
    chain_additions = [
        pipeid
        for pipeid in remaining_ordered
        if pipeid not in requested
        and _pipe_tags(pipe_registry, pipeid).get(TEMPORAL_LOWERING_TAG) == TEMPORAL_LOWERING_CHAIN
    ]
    pipe_ids[:] = survivors + chain_additions


_autocollapse_connected_provider = None


def ensure_autocollapse_connected() -> None:
    """Wire the autocollapse handler onto ``datapipes.before_run``. Idempotent.

    Called from every ``DataEvents``/``DataEpisodes`` declaration (see
    registry.py) rather than at module import time: by the time any
    temporal primitive has been successfully declared, the DI container and
    signal provider are guaranteed configured, so there's no bootstrap-
    ordering risk to reason about.

    Tracks the specific ``SignalProvider`` instance connected, not just
    whether a connection has ever happened -- a test or app that rebuilds
    the DI container (``GlobalInjector`` reset) gets a new provider, and
    this reconnects to it rather than silently staying attached to the old
    (now-orphaned) one.
    """
    global _autocollapse_connected_provider
    from kindling.signaling import SignalProvider

    try:
        signal_provider = GlobalInjector.get(SignalProvider)
    except Exception:  # noqa: BLE001 - no signal provider bound (bare tests)
        return
    if signal_provider is _autocollapse_connected_provider:
        return
    signal = signal_provider.get_signal("datapipes.before_run") or signal_provider.create_signal(
        "datapipes.before_run"
    )
    signal.connect(_autocollapse_before_run, weak=False)
    _autocollapse_connected_provider = signal_provider
