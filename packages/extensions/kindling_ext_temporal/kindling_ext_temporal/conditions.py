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

from dataclasses import dataclass, field
from typing import Any, List, Optional

from .entities import condition_quarantine_schema
from .validation import (
    ConditionValidationError,
    InvalidCondition,
    TemporalConditionValidator,
)

QUARANTINE_ENTITY_CONFIG_KEY = "kindling.temporal.conditions.quarantine_entity_id"


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


def _write_quarantine(spark, invalids, quarantine_entity_id, provider_factory):
    from kindling.data_entities import EntityMetadata
    from pyspark.sql import functions as F

    entity = EntityMetadata(
        entityid=quarantine_entity_id,
        name=quarantine_entity_id.split(".")[-1],
        merge_columns=[],
        tags={"provider_type": "delta", "temporal.kind": "conditions_quarantine"},
        schema=condition_quarantine_schema(),
        partition_columns=[],
    )
    rows = [
        (
            invalid.condition_id or None,
            list(invalid.errors),
            None if invalid.row is None else repr(invalid.row.asDict(recursive=True)),
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
    quarantine_entity_id: Optional[str] = None,
) -> ConditionsIngestionResult:
    """Validate condition rows per row, quarantine rejects, upsert the rest.

    The conditions set is small by design (tens to low hundreds — the same
    assumption the condition engine's driver-side loop makes), so rows are
    collected and validated on the driver. Graph cycles raise
    ConditionValidationError: a cyclic set has no ingestible subset.

    Keyword arguments are JIT overrides for tests and manual runs; defaults
    resolve through the injector (TemporalEntityResolver, the provider
    registry) and the `kindling.temporal.conditions.quarantine_entity_id`
    config key.
    """
    from .validation import ActiveSparkSqlExpressionParser

    rows = conditions_df.collect()
    validator = validator or TemporalConditionValidator(
        expression_parser=ActiveSparkSqlExpressionParser(conditions_df.sparkSession)
    )
    report = validator.validate(rows)

    set_level = [invalid for invalid in report.invalid_conditions if invalid.row is None]
    if set_level:
        raise ConditionValidationError(
            "Conditions set is not ingestible:\n"
            + "\n".join("; ".join(invalid.errors) for invalid in set_level)
        )

    invalid_row_ids = {id(invalid.row) for invalid in report.invalid_conditions}
    valid_rows = [row for row in rows if id(row) not in invalid_row_ids]

    provider_factory = provider_factory or _provider_for
    if valid_rows:
        entity = _conditions_entity(resolver)
        valid_df = conditions_df.sparkSession.createDataFrame(valid_rows, conditions_df.schema)
        provider_factory(entity).merge_to_entity(valid_df, entity)

    quarantined = list(report.invalid_conditions)
    quarantine_entity_id = quarantine_entity_id or _resolve_quarantine_entity_id()
    if quarantined and quarantine_entity_id:
        _write_quarantine(
            conditions_df.sparkSession, quarantined, quarantine_entity_id, provider_factory
        )

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
