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

Phase-1 constraint: all base events must share one input entity (the
chain's driving source). Heterogeneous bronze sources should normalize
into a shared staging entity first; native multi-source chaining is a
planned follow-up.
"""

from functools import reduce
from typing import Any, Dict, List, Optional

from kindling.data_entities import DataEntityRegistry
from kindling.data_pipes import DataPipesRegistry
from kindling.injection import GlobalInjector

from .entities import TemporalEntityResolver
from .registry import TemporalEpisodeRegistry, TemporalEventRegistry
from .translation import TemporalPipeTranslator

CHAIN_EVENTS_PIPE_PREFIX = "temporal.chain.events."
CHAIN_EPISODES_PIPE_PREFIX = "temporal.chain.episodes."
MAX_GENERATIONS_CONFIG_KEY = "kindling.temporal.max_generations"
DEFAULT_MAX_GENERATIONS = 10


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


def _chain_events_execute(driving_entity_id, conditions_current_id, base_defs, episode_defs):
    """Build the events-chain body: strata in memory, one returned frame."""

    def execute(**entity_dfs):
        driving_key = driving_entity_id.replace(".", "_")
        conditions_key = conditions_current_id.replace(".", "_")
        try:
            driving_df = entity_dfs[driving_key]
            conditions_df = entity_dfs[conditions_key]
        except KeyError as exc:
            available = ", ".join(sorted(entity_dfs.keys()))
            raise ValueError(
                f"Temporal events chain expected inputs '{driving_key}' and "
                f"'{conditions_key}', got: {available}"
            ) from exc

        from .engine import ConditionEngineRunner, EpisodeRunner
        from .validation import (
            ActiveSparkSqlExpressionParser,
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

        validator = TemporalConditionValidator(
            expression_parser=ActiveSparkSqlExpressionParser(driving_df.sparkSession)
        )
        valid_rules = validator.validate_or_raise(conditions_df.collect()).valid_rules

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

            if valid_rules:
                boundaries = _checkpoint(engine.execute_rules(stratum, valid_rules))
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

    events_entity = resolver.get_events_entity()
    conditions_entity = resolver.get_conditions_entity()
    conditions_tags = conditions_entity.tags or {}
    conditions_current_id = conditions_tags.get(
        "scd.current_entity_id", f"{conditions_entity.entityid}.current"
    )
    TemporalPipeTranslator.ensure_entity(entity_registry, events_entity)
    TemporalPipeTranslator.ensure_entity(entity_registry, conditions_entity)

    events_pipe = chain_events_pipe_id(chainid)
    pipe_registry.register_pipe(
        events_pipe,
        name=f"Temporal events chain: {chainid}",
        execute=_chain_events_execute(
            driving_entity_id, conditions_current_id, base_defs, episode_defs
        ),
        tags={
            "pipe_type": "temporal.chain_events",
            "temporal.kind": "chain_events",
            "temporal.chain_id": chainid,
            "temporal.reads_prior_state": "true",
        },
        input_entity_ids=[driving_entity_id, conditions_current_id],
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
