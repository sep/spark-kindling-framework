"""Validated ingestion for `silver.conditions` (proposal MVP items 1 and 2).

Conditions are rules-as-data: adding, modifying, or removing one is an
ordinary SCD2 upsert through whichever path feeds the table (file drop, CLI,
notebook, another pipeline). What this module adds is the required
validation pass in front of that upsert — a bad `enter_when` must not pass
silently into the table where a bad Python expression would never survive
code review.

`ingest_conditions` validates per row and quarantines rejects: well-formed
rows (including disabled ones — `enabled` is row state the engine filters at
read time) merge into the conditions entity, malformed rows are returned and,
when `kindling.temporal.conditions.quarantine_entity_id` is configured,
appended to the quarantine entity. Event-type graph cycles are set-level
inconsistencies — no subset ingest is well-defined — so they raise instead.

`validated_conditions_transform` is the file-drop hook: attach it as the
transform of a `FileIngestion` entry targeting the conditions entity and a
file containing any invalid row is rejected whole (the file path has no
per-row quarantine channel; use `ingest_conditions` for that).
"""

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, List, Optional

from kindling.sentinels import UNSET

from .entities import condition_quarantine_schema
from .validation import (
    ConditionValidationError,
    InvalidCondition,
    TemporalConditionValidator,
)

QUARANTINE_ENTITY_CONFIG_KEY = "kindling.temporal.conditions.quarantine_entity_id"

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConditionsIngestionResult:
    ingested_count: int
    quarantined: List[InvalidCondition] = field(default_factory=list)
    quarantine_entity_id: Optional[str] = None

    @property
    def is_clean(self) -> bool:
        return not self.quarantined


def _resolve_quarantine_entity_id() -> Optional[str]:
    try:
        from kindling.injection import GlobalInjector
        from kindling.spark_config import ConfigService

        value = GlobalInjector.get(ConfigService).get(QUARANTINE_ENTITY_CONFIG_KEY, None)
        return str(value).strip() or None if value is not None else None
    except Exception:
        return None


def _conditions_entity(resolver=None):
    if resolver is None:
        from kindling.injection import GlobalInjector

        from .entities import TemporalEntityResolver

        resolver = GlobalInjector.get(TemporalEntityResolver)
    return resolver.get_conditions_entity()


def _provider_for(entity):
    from kindling.entity_provider_registry import EntityProviderRegistry
    from kindling.injection import GlobalInjector

    return GlobalInjector.get(EntityProviderRegistry).get_provider_for_entity(entity)


def _quarantine_entity(quarantine_entity_id):
    """Resolve the registered quarantine entity; fall back to ad-hoc metadata.

    An app that registered the entity owns its storage configuration
    (provider tags, paths); fabricating metadata here would silently write
    to a different location than the registered entity reads from.
    """
    try:
        from kindling.data_entities import DataEntityRegistry
        from kindling.injection import GlobalInjector

        registered = GlobalInjector.get(DataEntityRegistry).get_entity_definition(
            quarantine_entity_id
        )
        if registered is not None:
            return registered
    except Exception:  # noqa: BLE001 - registry unavailable in bare tests
        pass

    from kindling.data_entities import EntityMetadata

    return EntityMetadata(
        entityid=quarantine_entity_id,
        name=quarantine_entity_id.split(".")[-1],
        merge_columns=[],
        tags={"provider_type": "delta", "temporal.kind": "conditions_quarantine"},
        schema=condition_quarantine_schema(),
        partition_columns=[],
    )


def _rule_generations(report) -> dict:
    """Map condition_id -> its layer in the batch's event-type graph.

    A rule's produced boundary types all land in one layer; the layer index
    is the rule's generation relative to this batch's graph.
    """
    layer_by_type = {
        event_type: layer_index
        for layer_index, layer in enumerate(report.generations)
        for event_type in layer
    }
    return {
        rule.condition_id: max(
            (layer_by_type.get(produced, 1) for produced in rule.produced_event_types),
            default=1,
        )
        for rule in report.valid_rules
    }


def _resolve_max_generations_ceiling() -> int:
    try:
        from kindling.injection import GlobalInjector
        from kindling.spark_config import ConfigService

        from .chain import DEFAULT_MAX_GENERATIONS, MAX_GENERATIONS_CONFIG_KEY

        value = GlobalInjector.get(ConfigService).get(MAX_GENERATIONS_CONFIG_KEY, None)
        if value is not None:
            return int(value)
        return DEFAULT_MAX_GENERATIONS
    except Exception:  # noqa: BLE001 - config service unavailable in bare tests
        from .chain import DEFAULT_MAX_GENERATIONS

        return DEFAULT_MAX_GENERATIONS


def _write_quarantine(spark, invalids, quarantine_entity_id, provider_factory):
    from pyspark.sql import functions as F

    entity = _quarantine_entity(quarantine_entity_id)
    rows = [
        (
            invalid.condition_id or None,
            list(invalid.errors),
            (
                None
                if invalid.row is None
                else json.dumps(invalid.row.asDict(recursive=True), default=str)
            ),
        )
        for invalid in invalids
    ]
    df = spark.createDataFrame(rows, condition_quarantine_schema()).withColumn(
        "quarantined_at", F.current_timestamp()
    )
    provider_factory(entity).append_to_entity(df, entity)


def ingest_conditions(
    conditions_df,
    *,
    validator: Optional[TemporalConditionValidator] = None,
    resolver=None,
    provider_factory=None,
    quarantine_entity_id: Any = UNSET,
) -> ConditionsIngestionResult:
    """Validate condition rows per row, quarantine rejects, upsert the rest.

    The conditions set is small by design (tens to low hundreds — the same
    assumption the condition engine's driver-side loop makes), so rows are
    collected and validated on the driver. Graph cycles raise
    ConditionValidationError: a cyclic set has no ingestible subset. Rows
    sharing a condition_id within one batch are all quarantined — the entity
    declares no scd.sequence_by, so which duplicate should win is undefined
    and the SCD2 merge would reject the batch far less legibly.

    Keyword arguments are JIT overrides for tests and manual runs; defaults
    resolve through the injector (TemporalEntityResolver, the provider
    registry) and the `kindling.temporal.conditions.quarantine_entity_id`
    config key. Pass ``quarantine_entity_id=None`` to disable the quarantine
    write even when config names an entity.
    """
    from .validation import ActiveSparkSqlExpressionParser

    rows = conditions_df.collect()

    id_counts = Counter(getattr(row, "condition_id", None) for row in rows)
    duplicates = [
        InvalidCondition(
            condition_id=row.condition_id,
            errors=[
                f"condition_id '{row.condition_id}' appears "
                f"{id_counts[row.condition_id]} times in this batch — which "
                "version wins is undefined (the entity has no scd.sequence_by)"
            ],
            row=row,
        )
        for row in rows
        if getattr(row, "condition_id", None) is not None and id_counts[row.condition_id] > 1
    ]
    duplicate_row_ids = {id(invalid.row) for invalid in duplicates}
    candidate_rows = [row for row in rows if id(row) not in duplicate_row_ids]

    validator = validator or TemporalConditionValidator(
        expression_parser=ActiveSparkSqlExpressionParser(conditions_df.sparkSession)
    )
    report = validator.validate(candidate_rows)

    set_level = [invalid for invalid in report.invalid_conditions if invalid.row is None]
    if set_level:
        raise ConditionValidationError(
            "Conditions set is not ingestible:\n"
            + "\n".join("; ".join(invalid.errors) for invalid in set_level)
        )

    invalid_row_ids = {id(invalid.row) for invalid in report.invalid_conditions}
    valid_rows = [row for row in candidate_rows if id(row) not in invalid_row_ids]

    # Stamp each rule's event-type-graph layer (advisory routing metadata;
    # the stratified lowering recomputes authoritative wiring from
    # declarations + the full current rule set at pipeline evaluation), and
    # reject rules deeper than the declared stratum ceiling up front.
    generation_by_rule = _rule_generations(report)
    max_generations = _resolve_max_generations_ceiling()
    too_deep = [
        InvalidCondition(
            condition_id=row.condition_id,
            errors=[
                f"condition '{row.condition_id}' sits at generation "
                f"{generation_by_rule[row.condition_id]}, beyond the declared "
                f"stratum ceiling kindling.temporal.max_generations={max_generations}"
            ],
            row=row,
        )
        for row in valid_rows
        if generation_by_rule.get(row.condition_id, 1) > max_generations
    ]
    too_deep_ids = {invalid.condition_id for invalid in too_deep}
    valid_rows = [row for row in valid_rows if row.condition_id not in too_deep_ids]

    provider_factory = provider_factory or _provider_for
    if valid_rows:
        from .entities import conditions_entity_schema

        entity = _conditions_entity(resolver)
        stamped = [
            {**row.asDict(recursive=False), "generation": generation_by_rule.get(row.condition_id)}
            for row in valid_rows
        ]
        valid_df = conditions_df.sparkSession.createDataFrame(stamped, conditions_entity_schema())
        provider_factory(entity).merge_to_entity(valid_df, entity)

    quarantined = duplicates + list(report.invalid_conditions) + too_deep
    if quarantine_entity_id is UNSET:
        quarantine_entity_id = _resolve_quarantine_entity_id()
    if quarantined and quarantine_entity_id:
        try:
            _write_quarantine(
                conditions_df.sparkSession, quarantined, quarantine_entity_id, provider_factory
            )
        except Exception:
            _logger.warning(
                "Quarantine write to '%s' failed after a successful conditions "
                "merge; rejected rows are only in the returned result",
                quarantine_entity_id,
                exc_info=True,
            )
            quarantine_entity_id = None

    return ConditionsIngestionResult(
        ingested_count=len(valid_rows),
        quarantined=quarantined,
        quarantine_entity_id=quarantine_entity_id if quarantined else None,
    )


def validated_conditions_transform(df: Any) -> Any:
    """File-ingestion transform: reject the whole file on any invalid row.

    Attach to a FileIngestion entry whose dest entity is the conditions
    entity; the standard file path then persists only files that validate
    clean. Per-row quarantine needs `ingest_conditions` instead.
    """
    from .validation import ActiveSparkSqlExpressionParser

    validator = TemporalConditionValidator(
        expression_parser=ActiveSparkSqlExpressionParser(df.sparkSession)
    )
    validator.validate_or_raise(df.collect())
    return df
