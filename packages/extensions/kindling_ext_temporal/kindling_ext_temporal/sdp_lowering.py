"""Stratified Lakeflow/SDP lowering of the temporal chain pipes.

Lakeflow gives a dataset no access to its own prior state — evaluation-time
reads of pipeline datasets are rejected (REFERENCE_DLT_DATASET_OUTSIDE_
QUERY_DEFINITION) and in-view self-target reads fail at runtime (both
probed on a real workspace, 2026-07-22). The chain pipes therefore lower to
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

The rule set is read from the conditions current view at source-evaluation
time (an EXTERNAL table — ingestion happens outside the pipeline by the
write-guard's design), so the wiring is exact per update while rules remain
data. Physical topology is fixed by ``kindling.temporal.max_generations``
so rule changes alter contents, never shape.
"""

from functools import reduce
from typing import Any, Dict, List, Optional

from kindling.injection import GlobalInjector

from .translation import TemporalPipeTranslator

STRATUM_SUFFIX = "__g"
DETERMINATIONS_SUFFIX = "__determinations"
SNAPSHOT_SUFFIX = "__episode_snapshot"


def _union(frames):
    return reduce(lambda left, right: left.unionByName(right), frames)


def _spark():
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError("No active Spark session during pipeline evaluation.")
    return session


def _temporal_registries():
    from .registry import TemporalEpisodeRegistry, TemporalEventRegistry

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


def _read_rules(spark, conditions_entity):
    """Read + validate the current rule set at source-evaluation time.

    The conditions table is external (ingested outside the pipeline), so
    this read is legal during evaluation. The 'current view' is a Kindling
    provider construct, not a physical table — read the base SCD2 table
    and filter to current rows. Returns (rules_by_generation,
    max_rule_generation).
    """
    from kindling.data_entities import scd_config_from_tags

    from .validation import ActiveSparkSqlExpressionParser, TemporalConditionValidator

    try:
        table_name = _physical_table_name(conditions_entity)
        df = spark.read.table(table_name)
        scd = scd_config_from_tags(conditions_entity)
        if scd.enabled and scd.is_current_column in df.columns:
            df = df.filter(df[scd.is_current_column])
        rows = df.collect()
    except Exception:  # first run: conditions not ingested yet
        return {}, 0

    validator = TemporalConditionValidator(expression_parser=ActiveSparkSqlExpressionParser(spark))
    report = validator.validate_or_raise(rows)
    layer_by_type = {
        event_type: layer_index
        for layer_index, layer in enumerate(report.generations)
        for event_type in layer
    }
    by_generation: Dict[int, List[Any]] = {}
    for rule in report.valid_rules:
        generation = max(
            (layer_by_type.get(produced, 1) for produced in rule.produced_event_types),
            default=1,
        )
        by_generation.setdefault(generation, []).append(rule)
    max_generation = max(by_generation, default=0)
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
    dp, events_name: str, episodes_name: Optional[str], max_generations: int
):
    """Emit the stratified dataset graph for one temporal chain.

    ``events_name`` / ``episodes_name`` are the already-normalized
    single-part dataset names of the chain pipes' outputs (episodes may be
    None when no episodes are declared).
    """
    from pyspark.sql import functions as F

    from .engine import ConditionEngineRunner, EpisodeRunner
    from .entities import TemporalEntityResolver

    spark = _spark()
    base_defs, episode_defs = _temporal_registries()
    if not base_defs:
        raise ValueError("Temporal SDP lowering: no base events registered.")

    resolver = GlobalInjector.get(TemporalEntityResolver)
    conditions_entity = resolver.get_conditions_entity()

    rules_by_generation, max_rule_generation = _read_rules(spark, conditions_entity)
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
    dp.create_streaming_table(name=stratum_names[0])
    for metadata in base_defs:
        source_entity = entity_registry.get_entity_definition(metadata.input_entity_id)
        source_table = (
            _physical_table_name(source_entity)
            if source_entity is not None
            else metadata.input_entity_id
        )

        def base_flow(metadata=metadata, source_table=source_table):
            df = spark.readStream.table(source_table)
            transformed = metadata.transform(df) if metadata.transform else df
            return TemporalPipeTranslator.select_event_envelope(transformed, metadata)

        base_flow.__name__ = f"{stratum_names[0]}_{metadata.eventid}".replace(".", "_")
        dp.append_flow(target=stratum_names[0], name=base_flow.__name__)(base_flow)

    # --- pre-determination boundary strata: fixed physical topology -------
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
