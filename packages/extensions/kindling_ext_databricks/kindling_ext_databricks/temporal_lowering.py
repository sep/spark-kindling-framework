"""Stratified Lakeflow lowering of the temporal chain pipes.

Lakeflow execution is a capability of THIS (databricks) extension; the
temporal extension stays engine-agnostic. spark-kindling-ext-temporal is a soft
dependency, imported only when an app registered chain pipes.

Lakeflow gives a dataset no access to its own prior state — evaluation-time
reads of pipeline datasets are rejected (REFERENCE_DLT_DATASET_OUTSIDE_
QUERY_DEFINITION) and in-view self-target reads fail at runtime (both
probed on a real workspace). The chain pipes therefore lower to
a stratified dataset graph in which every dependency is a real edge between
distinct datasets and all cross-update state lives in platform-owned
primitives:

    events__g0        streaming table; one append flow per base-event
                      declaration (multi-source lands here natively)
    events__g1..gK    streaming tables; stratum g's flow runs exactly the
                      rules whose generation is g (stateless per-event
                      boundary emission — genuinely incremental)
    episodes          AUTO CDC from-snapshot flow, keys=[episode_id],
                      stored_as_scd_type=2: the snapshot is the pure
                      episode state recomputed from the strata tables, so
                      late-end revision chains SCD2 versions and the
                      expired->closed history is preserved by the platform
    determinations    MV projecting determination events from ALL episode
                      versions — corrective and historical events coexist
                      because their deterministic event ids differ
    higher strata     MV per post-determination generation (higher-order
                      conditions); episodes over higher-order boundaries
                      are out of scope phase 1 and fail fast
    events (union)    MV union of strata + determinations + higher strata —
                      the canonical events surface for external consumers

Stratum tables are physical routing, not semantic partitions: an event's
own ``generation`` column is the semantic truth, so a rule whose generation
shifts (an upstream rule re-ingested one level deeper moves its consumers
transitively) changes future routing only — history never migrates.

The rule set comes from both condition sources, exactly as the chain
lowering resolves them: rows read from the conditions current view at
source-evaluation time (an EXTERNAL table — ingestion happens outside the
pipeline by the write-guard's design), plus any rules declared in-process
through ``DataConditions.register``. Generation layering is computed over
the combined set, so the wiring is exact per update while rules remain
data. Physical topology is fixed by ``kindling.temporal.max_generations``
so rule changes alter contents, never shape.

``kindling.lakeflow.temporal_mode: batch`` lowers the event strata
(``__g0..gK``) as materialized views reading with ``spark.table`` instead of
streaming tables fed by append flows, so a base-event transform may use
ordered analytic windows — ``row_number``, ``lag``, unbounded forward fill —
that Structured Streaming rejects. Everything from the episode snapshot
downstream is identical in both modes: the snapshot already reads the strata
in batch. Batch strata carry batch-query semantics, not an append-only
archive — a refresh can revise or remove previously produced events, so
retain the source history the computation needs.

``kindling.lakeflow.temporal_strata_materialization: view`` (batch mode only)
declares those same strata as pipeline-scoped temporary views instead, so no
``__g0..gK`` tables exist at all. Each generation reads every lower one, so a
view is re-expanded once per reference: the plan behind ``events`` grows
exponentially in the generation ceiling, and the source is rescanned for each
expansion. Sound for a small ceiling, and a way to stop persisting
intermediate events; a foot-gun at the default ceiling of 10.
"""

import logging
from functools import reduce
from typing import Any, Dict, List, Optional

from kindling.injection import GlobalInjector
from kindling_ext_temporal.translation import TemporalPipeTranslator

STRATUM_SUFFIX = "__g"
DETERMINATIONS_SUFFIX = "__determinations"
SNAPSHOT_SUFFIX = "__episode_snapshot"


def _union(frames):
    return reduce(lambda left, right: left.unionByName(right), frames)


def _logger():
    # Stdlib logging rather than the injected provider: this runs at
    # declaration time inside a Lakeflow pipeline, and a diagnostic must
    # never be the thing that fails a pipeline's declaration pass.
    return logging.getLogger("TemporalSdpLowering")


def _execution_mode(mode) -> str:
    """Normalize and validate the temporal-chain execution mode.

    ``streaming`` keeps the streaming-table + append-flow lowering;
    ``batch`` lowers the event strata as materialized views. Values ignore
    surrounding whitespace and case, mirroring the other declarative
    execution settings.
    """
    normalized = str(mode).strip().lower()
    if normalized not in ("streaming", "batch"):
        raise ValueError(
            "Invalid kindling.lakeflow.temporal_mode value "
            f"{mode!r}; expected 'streaming' or 'batch'."
        )
    return normalized


def _strata_materialization(value, mode: str) -> str:
    """Normalize and validate how the numbered event strata are persisted.

    ``table`` (default) keeps one materialized view per generation;
    ``view`` declares them as pipeline-scoped temporary views, so no
    ``__g0..gK`` tables are created and each generation is recomputed inline
    wherever it is referenced.

    Only meaningful in batch mode: a streaming stratum is an append-flow
    target, and a temporary view cannot be one. Combining them is a config
    error rather than a silent downgrade to batch reads.
    """
    normalized = str(value).strip().lower()
    if normalized not in ("table", "view"):
        raise ValueError(
            "Invalid kindling.lakeflow.temporal_strata_materialization value "
            f"{value!r}; expected 'table' or 'view'."
        )
    if normalized == "view" and mode != "batch":
        raise ValueError(
            "kindling.lakeflow.temporal_strata_materialization='view' requires "
            f"kindling.lakeflow.temporal_mode='batch' (got {mode!r}): streaming "
            "strata are append-flow targets, which temporary views cannot be."
        )
    return normalized


def _spark():
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("No active Spark session during pipeline evaluation.")
    return session


def _temporal_registries():
    from kindling_ext_temporal.registry import (
        TemporalEpisodeRegistry,
        TemporalEventRegistry,
    )

    event_registry = GlobalInjector.get(TemporalEventRegistry)
    episode_registry = GlobalInjector.get(TemporalEpisodeRegistry)
    base_defs = [
        event_registry.get_base_event_definition(eventid)
        for eventid in event_registry.get_base_event_ids()
    ]
    episode_defs = [
        episode_registry.get_episode_definition(episodeid)
        for episodeid in episode_registry.get_episode_ids()
    ]
    return base_defs, episode_defs


def _physical_table_name(entity) -> str:
    """Resolve an EXTERNAL entity's physical catalog name.

    External temporal inputs (driving telemetry, the conditions table) are
    written by runner-engine jobs, so the pipeline must read them at the
    same physical names Kindling's EntityNameMapper produces from the
    bridged storage config.
    """
    from kindling.data_entities import EntityNameMapper

    return GlobalInjector.get(EntityNameMapper).get_table_name(entity)


_MISSING_TABLE_MARKERS = (
    "TABLE_OR_VIEW_NOT_FOUND",
    "NoSuchTableException",
    "SCHEMA_NOT_FOUND",
    "NoSuchNamespaceException",
    "NoSuchDatabaseException",
    "DATABASE_NOT_FOUND",
    "PATH_NOT_FOUND",
)


def _is_missing_table_error(error: Exception) -> bool:
    """Whether a conditions read failed because nothing has been ingested yet.

    Only a genuinely absent table/namespace is the benign first-run case.
    Every other failure (permissions, a malformed table, a bad catalog
    binding) used to be swallowed into an empty rule set, which silently
    lowered a pipeline that emits no boundary events at all — see the
    rule-count logging in ``_resolve_rules`` for the other half of that fix.
    """
    text = f"{type(error).__name__}: {error}"
    return any(marker.lower() in text.lower() for marker in _MISSING_TABLE_MARKERS)


def _condition_engine_sources():
    """Which condition sources this app's declared engines actually use.

    Mirrors ``declare_temporal_chain``: an engine declares exactly one
    source, and a chain with no declared engines keeps the pre-existing
    behavior of reading the conditions table unconditionally.
    """
    from kindling_ext_temporal.registry import TemporalEventRegistry

    event_registry = GlobalInjector.get(TemporalEventRegistry)
    engine_defs = [
        event_registry.get_condition_engine_definition(engineid)
        for engineid in event_registry.get_condition_engine_ids()
    ]
    has_table_engine = any(metadata.condition_source == "table" for metadata in engine_defs)
    has_registry_engine = any(metadata.condition_source == "registry" for metadata in engine_defs)
    read_conditions_table = not engine_defs or has_table_engine
    return has_table_engine, has_registry_engine, read_conditions_table


def _read_table_rules(spark, conditions_entity) -> List[Any]:
    """Read + validate the current table-sourced rule set.

    The conditions table is external (ingested outside the pipeline), so
    this read is legal during evaluation. The 'current view' is a Kindling
    provider construct, not a physical table — read the base SCD2 table
    and filter to current rows.
    """
    from kindling.data_entities import scd_config_from_tags
    from kindling_ext_temporal.validation import (
        ActiveSparkSqlExpressionParser,
        TemporalConditionValidator,
    )

    try:
        table_name = _physical_table_name(conditions_entity)
        df = spark.read.table(table_name)
        scd = scd_config_from_tags(conditions_entity)
        if scd.enabled and scd.is_current_column in df.columns:
            df = df.filter(df[scd.is_current_column])
        rows = df.collect()
    except Exception as error:
        if _is_missing_table_error(error):  # first run: conditions not ingested yet
            _logger().info(
                "Temporal SDP lowering: conditions table not found yet; "
                "no table-sourced rules for this update."
            )
            return []
        raise

    validator = TemporalConditionValidator(expression_parser=ActiveSparkSqlExpressionParser(spark))
    return validator.validate_or_raise(rows).valid_rules


def _resolve_rules(spark, conditions_entity):
    """Resolve every rule this chain runs, from both condition sources.

    Registry-declared rules (``DataConditions.register``) are as much a
    part of the chain as ingested rows, and the chain lowering has always
    run both. Omitting them here lowered a pipeline whose boundary events
    were never emitted at all: episodes opened by a base event and closed
    by a registry-backed condition simply ran to their synthetic expiry.

    Generation layering is computed over the *combined* set, not per
    source: the stratum count is fixed at declaration time, so a rule
    layered against a partial graph lands in the wrong stratum.

    Returns ``(rules_by_generation, max_rule_generation)``.
    """
    from kindling_ext_temporal.registry import TemporalConditionRegistry
    from kindling_ext_temporal.validation import (
        combine_condition_rules,
        layer_rules_by_generation,
    )

    has_table_engine, has_registry_engine, read_conditions_table = _condition_engine_sources()

    table_rules = _read_table_rules(spark, conditions_entity) if read_conditions_table else []
    registry_rules = (
        GlobalInjector.get(TemporalConditionRegistry).get_all_conditions()
        if has_registry_engine
        else []
    )
    combined_rules = combine_condition_rules(
        table_rules,
        registry_rules,
        has_table_engine=has_table_engine,
        has_registry_engine=has_registry_engine,
    )

    by_generation, max_generation = layer_rules_by_generation(combined_rules)
    _logger().info(
        "Temporal SDP lowering: %s condition rule(s) resolved "
        "(%s table-sourced, %s registry-declared) across generations %s.",
        len(combined_rules),
        len(table_rules),
        len(registry_rules),
        sorted(by_generation) or "(none)",
    )
    return by_generation, max_generation


def _determination_types(episode_defs) -> set:
    types = set()
    for episode in episode_defs:
        types.add(episode.determination_event or f"{episode.episodeid}.closed")
        types.add(episode.expiration_event or f"{episode.episodeid}.expired")
        types.add(episode.invalidation_event or f"{episode.episodeid}.invalidated")
    return types


def _split_rules(by_generation, episode_defs):
    """Split rules into pre-determination strata and post-determination
    (higher-order) strata by whether they transitively consume any
    determination event type."""
    determination_types = _determination_types(episode_defs)
    higher_ids: set = set()
    changed = True
    all_rules = [rule for rules in by_generation.values() for rule in rules]
    produced_by_higher: set = set()
    while changed:
        changed = False
        for rule in all_rules:
            if rule.condition_id in higher_ids:
                continue
            consumed = set(rule.consumes_event_type)
            if consumed & determination_types or consumed & produced_by_higher:
                higher_ids.add(rule.condition_id)
                produced_by_higher.update(rule.produced_event_types)
                changed = True

    pre: Dict[int, List[Any]] = {}
    post: List[Any] = []
    for generation, rules in by_generation.items():
        for rule in rules:
            if rule.condition_id in higher_ids:
                post.append(rule)
            else:
                pre.setdefault(generation, []).append(rule)
    return pre, post, higher_ids


def _fail_on_higher_order_episodes(episode_defs, higher_ids):
    """Episodes over higher-order boundaries are out of scope phase 1.

    Only fails when the boundary's producing condition is PRESENT in the
    rule set and classified higher-order; an absent condition (not yet
    ingested) simply yields no boundaries yet.
    """
    for episode in episode_defs:
        for boundary in (episode.start_event, episode.end_event):
            if not boundary or "." not in boundary:
                continue
            condition_id = boundary.rsplit(".", 1)[0]
            if condition_id in higher_ids:
                raise ValueError(
                    f"Temporal SDP lowering: episode '{episode.episodeid}' pairs "
                    f"boundary '{boundary}' produced by higher-order "
                    f"(determination-consuming) condition '{condition_id}'. "
                    "Episodes over higher-order conditions are not supported "
                    "in the stratified lowering yet."
                )


def declare_stratified_temporal(
    dp,
    events_name: str,
    episodes_name: Optional[str],
    max_generations: int,
    mode: str = "streaming",
    strata_materialization: str = "table",
):
    """Emit the stratified dataset graph for one temporal chain.

    ``events_name`` / ``episodes_name`` are the already-normalized
    single-part dataset names of the chain pipes' outputs (episodes may be
    None when no episodes are declared).

    ``mode`` selects how the pre-determination strata ``__g0..gK`` are
    lowered — ``streaming`` (default: streaming tables + append flows) or
    ``batch`` (one materialized view per stratum, batch reads) — so a
    base-event transform may use ordered analytic windows that Structured
    Streaming rejects. Everything from the episode snapshot downstream is
    identical in both modes. See
    ``docs/proposals/temporal_lakeflow_execution_mode.md``.

    ``strata_materialization`` (batch only) chooses whether those numbered
    strata are materialized views (``table``, default) or pipeline-scoped
    temporary views (``view``). Only ``__g0..gK`` are affected — the
    determination view, higher stratum, episodes target and the canonical
    ``events`` surface are declared identically either way.
    """
    from kindling_ext_temporal.engine import ConditionEngineRunner, EpisodeRunner
    from kindling_ext_temporal.entities import TemporalEntityResolver
    from pyspark.sql import functions as F

    mode = _execution_mode(mode)
    strata_materialization = _strata_materialization(strata_materialization, mode)
    spark = _spark()
    base_defs, episode_defs = _temporal_registries()
    if not base_defs:
        raise ValueError("Temporal SDP lowering: no base events registered.")

    resolver = GlobalInjector.get(TemporalEntityResolver)
    conditions_entity = resolver.get_conditions_entity()

    rules_by_generation, max_rule_generation = _resolve_rules(spark, conditions_entity)
    pre_rules, post_rules, higher_ids = _split_rules(rules_by_generation, episode_defs)
    _fail_on_higher_order_episodes(episode_defs, higher_ids)
    if max_rule_generation > max_generations:
        raise ValueError(
            f"Temporal SDP lowering: the current rule set reaches generation "
            f"{max_rule_generation}, beyond kindling.temporal.max_generations="
            f"{max_generations}. Raise the ceiling or re-ingest the rules."
        )

    engine = ConditionEngineRunner()
    episode_runner = EpisodeRunner()

    # --- stratum 0: base events, one append flow per declaration ----------
    from kindling.data_entities import DataEntityRegistry

    entity_registry = GlobalInjector.get(DataEntityRegistry)
    stratum_names = [f"{events_name}{STRATUM_SUFFIX}0"]

    def _base_source_table(metadata):
        """External physical name for one base-event declaration's input.

        Unchanged in both modes: a producer selected in the same pipeline
        still resolves externally and establishes no local dependency edge
        (see the SDP extension README). Making internal producers resolve to
        their pipeline-local dataset names is a separate change.
        """
        source_entity = entity_registry.get_entity_definition(metadata.input_entity_id)
        if source_entity is None:
            return metadata.input_entity_id
        return _physical_table_name(source_entity)

    if mode == "batch":
        # One MV per stratum: every read is a batch read, so base transforms
        # are ordinary batch Spark queries. Multi-source fan-in happens
        # inside the single query function — each input keeps its own
        # transform, and only the resulting envelopes are unioned.
        base_sources = [(metadata, _base_source_table(metadata)) for metadata in base_defs]
        # Temporary views are recomputed at every reference; materialized
        # views are persisted once per update. See _strata_materialization.
        stratum_dataset = (
            dp.temporary_view if strata_materialization == "view" else dp.materialized_view
        )

        def base_stratum(base_sources=base_sources):
            frames = []
            for metadata, source_table in base_sources:
                df = spark.table(source_table)
                transformed = metadata.transform(df) if metadata.transform else df
                frames.append(TemporalPipeTranslator.select_event_envelope(transformed, metadata))
            return _union(frames)

        base_stratum.__name__ = f"{stratum_names[0]}_view".replace(".", "_")
        stratum_dataset(name=stratum_names[0])(base_stratum)

        for generation in range(1, max_generations + 1):
            stratum_name = f"{events_name}{STRATUM_SUFFIX}{generation}"
            rules = pre_rules.get(generation, [])
            lower = list(stratum_names)

            def stratum_view(rules=rules, lower=lower):
                if not rules:
                    # Fixed-K topology: an empty generation still declares a
                    # dataset, preserving the envelope schema and the
                    # dependency edge on the base stratum.
                    return spark.table(lower[0]).where(F.lit(False))
                inputs = _union([spark.table(name) for name in lower])
                return engine.execute_rules(inputs, rules)

            stratum_view.__name__ = f"{stratum_name}_view".replace(".", "_")
            stratum_dataset(name=stratum_name)(stratum_view)
            stratum_names.append(stratum_name)
    else:
        dp.create_streaming_table(name=stratum_names[0])
        for metadata in base_defs:
            source_table = _base_source_table(metadata)

            def base_flow(metadata=metadata, source_table=source_table):
                df = spark.readStream.table(source_table)
                transformed = metadata.transform(df) if metadata.transform else df
                return TemporalPipeTranslator.select_event_envelope(transformed, metadata)

            base_flow.__name__ = f"{stratum_names[0]}_{metadata.eventid}".replace(".", "_")
            dp.append_flow(target=stratum_names[0], name=base_flow.__name__)(base_flow)

        # --- pre-determination boundary strata: fixed physical topology ---
        for generation in range(1, max_generations + 1):
            stratum_name = f"{events_name}{STRATUM_SUFFIX}{generation}"
            rules = pre_rules.get(generation, [])
            lower = list(stratum_names)
            dp.create_streaming_table(name=stratum_name)

            def stratum_flow(rules=rules, lower=lower):
                if not rules:
                    # Append flows require a streaming source even when a
                    # stratum has no rules (fixed-K topology): stream the base
                    # stratum, keep nothing.
                    from pyspark.sql import functions as F

                    return spark.readStream.table(lower[0]).where(F.lit(False))
                inputs = _union([spark.readStream.table(name) for name in lower])
                return engine.execute_rules(inputs, rules)

            stratum_flow.__name__ = f"{stratum_name}_flow".replace(".", "_")
            dp.append_flow(target=stratum_name, name=stratum_flow.__name__)(stratum_flow)
            stratum_names.append(stratum_name)

    union_members = list(stratum_names)

    # --- episodes: AUTO CDC snapshot, SCD2 preserves revision history -----
    if episodes_name is not None and episode_defs:
        snapshot_name = f"{episodes_name}{SNAPSHOT_SUFFIX}"

        @dp.temporary_view(name=snapshot_name)
        def episode_snapshot():
            boundaries = _union([spark.table(name) for name in stratum_names])
            evaluation_time = F.current_timestamp()
            frames = [
                episode_runner.execute(boundaries, episode, evaluation_time=evaluation_time)
                for episode in episode_defs
            ]
            return _union(frames).drop("created_at", "updated_at")

        dp.create_streaming_table(name=episodes_name)
        dp.create_auto_cdc_from_snapshot_flow(
            target=episodes_name,
            source=snapshot_name,
            keys=["episode_id"],
            stored_as_scd_type=2,
        )

        # --- determination events from ALL episode versions ---------------
        determinations_name = f"{events_name}{DETERMINATIONS_SUFFIX}"

        @dp.materialized_view(name=determinations_name)
        def determinations():
            episodes_df = spark.table(episodes_name)
            return _project_determination_events(episodes_df, episode_defs)

        union_members.append(determinations_name)

        # --- higher-order (determination-consuming) strata -----------------
        if post_rules:
            higher_name = f"{events_name}{STRATUM_SUFFIX}hi"

            @dp.materialized_view(name=higher_name)
            def higher_boundaries():
                source = spark.table(determinations_name)
                return engine.execute_rules(source, post_rules)

            union_members.append(higher_name)

    # --- the canonical events surface --------------------------------------
    @dp.materialized_view(name=events_name)
    def events_union():
        return _union([spark.table(name) for name in union_members]).dropDuplicates(["event_id"])


def _project_determination_events(episodes_df, episode_defs):
    """Determination events projected from episode version rows.

    Mirrors EpisodeRunner.execute_determination_events' envelope so event
    ids match the runner lowering exactly; every SCD2 version row projects
    its own determination, so historical and corrective events coexist.
    """
    from pyspark.sql import functions as F

    frames = []
    for episode in episode_defs:
        determination_event = episode.determination_event or f"{episode.episodeid}.closed"
        expiration_event = episode.expiration_event or f"{episode.episodeid}.expired"
        invalidation_event = episode.invalidation_event or f"{episode.episodeid}.invalidated"
        scoped = episodes_df.filter(
            (F.col("episode_type") == F.lit(episode.episodeid))
            & F.col("status").isin("closed", "expired", "invalidated")
        )
        emitted_event_type = (
            F.when(F.col("status") == F.lit("expired"), F.lit(expiration_event))
            .when(F.col("status") == F.lit("invalidated"), F.lit(invalidation_event))
            .otherwise(F.lit(determination_event))
        )
        event_id = F.sha2(
            F.concat_ws("||", emitted_event_type, F.col("episode_id").cast("string")), 256
        )
        payload = F.create_map(
            F.lit("episode_id"),
            F.col("episode_id").cast("string"),
            F.lit("episode_type"),
            F.lit(episode.episodeid),
            F.lit("condition_id"),
            F.lit(episode.condition_id).cast("string"),
            F.lit("status"),
            F.col("status").cast("string"),
            F.lit("close_reason"),
            F.col("close_reason").cast("string"),
            F.lit("start_event_id"),
            F.col("start_event_id").cast("string"),
            F.lit("end_event_id"),
            F.col("end_event_id").cast("string"),
            F.lit("start_time"),
            F.col("start_time").cast("string"),
            F.lit("end_time"),
            F.col("end_time").cast("string"),
            F.lit("duration_ms"),
            F.col("duration_ms").cast("string"),
        )
        attributes = F.create_map(
            F.lit("start_event_type"),
            F.lit(episode.start_event),
            F.lit("end_event_type"),
            F.lit(episode.end_event),
            F.lit("start_generation"),
            F.col("start_generation").cast("string"),
            F.lit("end_event_synthetic"),
            F.col("end_event_synthetic").cast("string"),
        )
        frames.append(
            scoped.select(
                event_id.alias("event_id"),
                emitted_event_type.alias("event_type"),
                (F.coalesce(F.col("start_generation"), F.lit(1)) + F.lit(1))
                .cast("int")
                .alias("generation"),
                F.lit("episode").alias("event_class"),
                F.col("subject_type"),
                F.col("subject_id"),
                F.col("end_time").alias("event_ts"),
                F.lit("kindling-temporal").alias("source_system"),
                F.col("episode_id").alias("correlation_id"),
                payload.alias("payload"),
                attributes.alias("attributes"),
                F.current_timestamp().alias("ingested_at"),
            )
        )
    return _union(frames)
