"""Provenance stamping.

An entity with ``provenance.enabled`` (a tag, or the ``kindling.provenance.*``
settings default baked into its tags; parsed and validated in
``kindling.data_entities``) carries one nullable struct column, by default
``__provenance``, naming the pipe, run and inputs that wrote each row. See
docs/proposals/provenance_stamping.md.

This module builds the column and is called from every write path:

- the batch persist step, after ``apply_surrogate_keys`` (``stamp_provenance``);
- the streaming start, after the surrogate key step, so ``processed_at`` is
  per micro-batch and the execution id is the one that started the stream;
- the SDP dataset builder, from declaration-time literals.

The rule is "overwrite overwrites": the column is written with every row
write. It is never a change: the merges leave it out of every change test
(tracked, and the untracked comparison) and keep it in every write set.

The stamp is data, not telemetry: nothing here emits spans or metrics.
"""

import fnmatch
import threading
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Iterable, Optional, Tuple

from injector import inject
from kindling.data_entities import (
    EntityMetadata,
    provenance_column,
    provenance_config_from_tags,
    provenance_kept_inputs,
    quote_sql_identifier,
)
from kindling.injection import GlobalInjector
from kindling.signaling import SignalProvider
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

#: Version of the stamp's struct layout. Fields are only ever added.
PROVENANCE_SCHEMA_VERSION = 1

MODE_BATCH = "batch"
MODE_STREAMING = "streaming"
MODE_SDP = "sdp"

_STRING_MAP = MapType(StringType(), StringType(), True)

_STRING_FIELDS = (
    "pipe_id",
    "pipe_name",
    "run_id",
    "execution_id",
    "persist_id",
)
_IDENTITY_FIELDS = ("mode", "app_name", "environment", "platform", "kindling_version")


def provenance_struct_type() -> StructType:
    """The provenance column's type; every field is nullable."""
    return StructType(
        [StructField(name, StringType(), True) for name in _STRING_FIELDS]
        + [StructField("processed_at", TimestampType(), True)]
        + [StructField(name, StringType(), True) for name in _IDENTITY_FIELDS]
        + [
            StructField("input_entity_ids", ArrayType(StringType(), True), True),
            # Phase 2: entity id -> version read. Null until then.
            StructField("input_versions", _STRING_MAP, True),
            StructField("tags", _STRING_MAP, True),
            StructField("schema_version", IntegerType(), True),
        ]
    )


@dataclass(frozen=True)
class ProvenanceStamp:
    """The values of one write's stamp. Missing context is None (a null
    field), never an error."""

    pipe_id: Optional[str] = None
    pipe_name: Optional[str] = None
    run_id: Optional[str] = None
    execution_id: Optional[str] = None
    persist_id: Optional[str] = None
    #: None: ``current_timestamp()`` at write (per micro-batch on a stream).
    processed_at: Optional[datetime] = None
    mode: Optional[str] = None
    app_name: Optional[str] = None
    environment: Optional[str] = None
    platform: Optional[str] = None
    kindling_version: Optional[str] = None
    input_entity_ids: Tuple[str, ...] = ()
    tags: Tuple[Tuple[str, str], ...] = ()


def _string_lit(value: Optional[str]) -> Column:
    return F.lit(value).cast(StringType())


def provenance_column_expr(stamp: ProvenanceStamp) -> Column:
    """The stamp as a struct column of ``provenance_struct_type()``, built
    from literals, ``create_map`` and (without ``processed_at``)
    ``current_timestamp()``."""
    processed_at = (
        F.current_timestamp()
        if stamp.processed_at is None
        else F.lit(stamp.processed_at).cast(TimestampType())
    )
    input_ids = (
        F.array(*[F.lit(entity_id) for entity_id in stamp.input_entity_ids])
        if stamp.input_entity_ids
        else F.array()
    ).cast(ArrayType(StringType(), True))
    tag_args = [F.lit(part) for pair in stamp.tags for part in pair]
    tags = (F.create_map(*tag_args) if tag_args else F.create_map()).cast(_STRING_MAP)
    fields = [_string_lit(getattr(stamp, name)).alias(name) for name in _STRING_FIELDS]
    fields.append(processed_at.alias("processed_at"))
    fields.extend(_string_lit(getattr(stamp, name)).alias(name) for name in _IDENTITY_FIELDS)
    fields.extend(
        [
            input_ids.alias("input_entity_ids"),
            F.lit(None).cast(_STRING_MAP).alias("input_versions"),
            tags.alias("tags"),
            F.lit(PROVENANCE_SCHEMA_VERSION).cast(IntegerType()).alias("schema_version"),
        ]
    )
    return F.struct(*fields).cast(provenance_struct_type())


def _find_column(columns: Iterable[str], name: str) -> Optional[str]:
    lowered = name.lower()
    return next((column for column in columns if column.lower() == lowered), None)


def stamp_provenance(df: DataFrame, entity: EntityMetadata, stamp_factory) -> DataFrame:
    """Add the entity's provenance column to ``df`` when stamping is enabled.

    ``stamp_factory`` is a zero-argument callable returning the
    ``ProvenanceStamp``; it is only called for a stamped entity. Raises when
    ``df`` already has a column of that name (an upstream input kept its
    stamp, or the pipe built one): the stamp would silently replace it.

    A batch stamp without ``processed_at`` gets the driver's current time as
    a literal: a provider that stores a lazy plan (the memory provider's
    temp view) re-runs it on every read, and ``current_timestamp()`` would
    then change with each read. A stream keeps ``current_timestamp()``, so
    each micro-batch carries its own time.
    """
    column = provenance_column(entity)
    if column is None:
        return df
    existing = _find_column(df.columns, column)
    if existing is not None:
        raise ValueError(
            f"Entity '{entity.entityid}': the pipe output already has a column "
            f"'{existing}', which provenance stamping writes. Drop it in the pipe, "
            "or rename the stamp with provenance.column"
        )
    stamp = stamp_factory()
    if stamp.processed_at is None and df.isStreaming is not True:
        stamp = replace(stamp, processed_at=datetime.now(timezone.utc))
    return df.withColumn(column, provenance_column_expr(stamp))


def augment_schema_for_provenance(schema, entity: EntityMetadata):
    """Add the provenance column to a declared schema when stamping is enabled.

    Like ``augment_schema_for_scd2``: new tables get the column at creation,
    and ``schema.drift: fail`` and declarative schemas accept it. A schema of
    None (inferred from the first write) is returned unchanged; a DDL string
    (an SDP declaration's schema) gets the column appended in DDL.
    """
    column = provenance_column(entity)
    if schema is None or column is None:
        return schema
    if isinstance(schema, str):
        if not schema.strip():
            return schema
        ddl_type = provenance_struct_type().simpleString()
        return f"{schema.rstrip().rstrip(',')}, {quote_sql_identifier(column)} {ddl_type}"
    schema_struct = schema if isinstance(schema, StructType) else StructType(schema)
    if _find_column(schema_struct.fieldNames(), column) is not None:
        return schema_struct
    return StructType(schema_struct.fields + [StructField(column, provenance_struct_type(), True)])


def drop_provenance(df: DataFrame, entity: EntityMetadata, pipe=None) -> DataFrame:
    """Drop an input entity's provenance column from a downstream read.

    Kept when the reading pipe lists the entity in ``provenance.keep_inputs``.
    Otherwise a downstream table could inherit a stamp naming the wrong
    pipe, and the stamp's maps break ``distinct`` and ``groupBy``.

    Lazy: ``DataFrame.drop`` by name is a no-op for a missing column, so no
    schema is resolved here (an SDP dataset function must not analyze its
    inputs eagerly).
    """
    if df is None:
        return df
    column = provenance_column(entity)
    if column is None:
        return df
    if pipe is not None and getattr(entity, "entityid", None) in provenance_kept_inputs(pipe):
        return df
    return df.drop(column)


# ── Stamp values ─────────────────────────────────────────────────────────────


@lru_cache(maxsize=1)
def _kindling_version() -> Optional[str]:
    try:
        from importlib.metadata import version

        return version("spark-kindling")
    except Exception:
        return None


def _text(value: Any) -> Optional[str]:
    """A config value as text; anything but a non-empty str is None (this
    also ignores test doubles)."""
    return value.strip() if isinstance(value, str) and value.strip() else None


def _config_service(config_service):
    if config_service is not None:
        return config_service
    try:
        from kindling.spark_config import ConfigService

        return GlobalInjector.get(ConfigService)
    except Exception:
        return None


def _runtime_identity(config_service) -> Dict[str, Optional[str]]:
    """App, environment, platform and Kindling version, best effort."""
    config_service = _config_service(config_service)
    platform = None
    initial: Dict[str, Any] = {}
    if config_service is not None:
        try:
            platform = _text(config_service.get("kindling.platform.name")) or _text(
                config_service.get("kindling.platform.environment")
            )
        except Exception:
            platform = None
        candidate = getattr(config_service, "initial_config", None)
        initial = candidate if isinstance(candidate, dict) else {}
    return {
        "app_name": _text(initial.get("app_name")),
        "environment": _text(initial.get("environment")),
        "platform": platform,
        "kindling_version": _kindling_version(),
    }


def selected_pipe_tags(pipe, entity: EntityMetadata) -> Tuple[Tuple[str, str], ...]:
    """The pipe tags the entity's ``provenance.pipe_tags`` allow-list names,
    sorted by name. Tags that held a resolved secret are always left out."""
    from kindling.config_loaders import is_resolved_secret_value

    patterns = provenance_config_from_tags(entity).pipe_tags
    tags = getattr(pipe, "tags", None)
    if not patterns or not isinstance(tags, Mapping) or not tags:
        return ()
    selected = []
    for name in sorted(tags, key=str):
        value = tags[name]
        if value is None or is_resolved_secret_value(value):
            continue
        if any(fnmatch.fnmatchcase(str(name), pattern) for pattern in patterns):
            selected.append((str(name), str(value)))
    return tuple(selected)


def build_provenance_stamp(
    pipe,
    entity: EntityMetadata,
    *,
    mode: str,
    persist_id: Optional[str] = None,
    execution_id: Optional[str] = None,
    processed_at: Optional[datetime] = None,
    config_service=None,
) -> ProvenanceStamp:
    """The stamp for one write of ``pipe`` into ``entity``.

    ``run_id`` (the runtime manifest's per-initialization id) is null until
    that lands; ``execution_id`` is the executor's run id.
    """
    input_ids = getattr(pipe, "input_entity_ids", None)
    return ProvenanceStamp(
        pipe_id=_text(getattr(pipe, "pipeid", None)),
        pipe_name=_text(getattr(pipe, "name", None)),
        run_id=None,
        execution_id=_text(execution_id),
        persist_id=_text(persist_id),
        processed_at=processed_at,
        mode=mode,
        input_entity_ids=(
            tuple(str(entity_id) for entity_id in input_ids)
            if isinstance(input_ids, (list, tuple))
            else ()
        ),
        tags=selected_pipe_tags(pipe, entity),
        **_runtime_identity(config_service),
    )


# ── Declarative engines ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class DeclaredProvenance:
    """Provenance lowering for one declared dataset (SDP/Lakeflow).

    Built when the dataset is declared, so every value is a literal,
    ``processed_at`` included (incremental refresh never sees
    ``current_timestamp()``). Both operations are lazy: an SDP dataset
    function must not analyze its inputs eagerly, so there is no
    existing-column check on the output (``withColumn`` replaces).
    """

    column: Optional[str] = None
    stamp: Optional[ProvenanceStamp] = None
    #: (input entity id, provenance column dropped from that input's read)
    dropped_inputs: Tuple[Tuple[str, str], ...] = ()

    def drop_from_input(self, entity_id: str, df: DataFrame) -> DataFrame:
        for dropped_id, column in self.dropped_inputs:
            if dropped_id == entity_id:
                return df.drop(column)
        return df

    def stamp_output(self, df: DataFrame) -> DataFrame:
        if self.column is None or self.stamp is None:
            return df
        return df.withColumn(self.column, provenance_column_expr(self.stamp))


def declared_provenance(
    pipe,
    entity,
    input_entities: Mapping[str, Any],
    *,
    processed_at: Optional[datetime] = None,
    config_service=None,
) -> DeclaredProvenance:
    """The provenance lowering of ``pipe`` writing ``entity`` in a
    declarative engine: the output stamp (``mode='sdp'``, no run, execution
    or persist id) and the stamp columns to drop from each input read
    (``input_entities`` maps input entity id to its metadata)."""
    dropped = []
    kept = provenance_kept_inputs(pipe)
    for entity_id, input_entity in (input_entities or {}).items():
        column = provenance_column(input_entity) if input_entity is not None else None
        if column is not None and entity_id not in kept:
            dropped.append((entity_id, column))
    column = provenance_column(entity) if entity is not None else None
    stamp = None
    if column is not None:
        stamp = build_provenance_stamp(
            pipe,
            entity,
            mode=MODE_SDP,
            processed_at=processed_at or datetime.now(timezone.utc),
            config_service=config_service,
        )
    return DeclaredProvenance(column=column, stamp=stamp, dropped_inputs=tuple(dropped))


# ── Execution ids ────────────────────────────────────────────────────────────


@GlobalInjector.singleton_autobind()
class ProvenanceRunCapture:
    """Execution id of each running pipe, captured from lifecycle signals.

    Neither executor passes its run id to the persist step or the stream
    starter, so this listens to the signals that carry it, keyed by pipe id
    (the watermark aspect keys its read-time capture the same way, and
    shares its contract: one execution of a pipe at a time per process):

    - ``orchestrator.pipe_started`` and ``datapipes.before_pipe`` (batch,
      and streams the generation executor starts);
    - ``streaming.pipe_starting`` (streams the StreamingOrchestrator starts).

    A pipe's entry is cleared when it completes, fails or is skipped, so a
    persist outside an executor gets a null execution id, not a stale one.
    """

    _SET_SIGNALS = (
        "orchestrator.pipe_started",
        "datapipes.before_pipe",
        "streaming.pipe_starting",
    )
    _CLEAR_SIGNALS = (
        "orchestrator.pipe_completed",
        "orchestrator.pipe_failed",
        "orchestrator.pipe_skipped",
        "datapipes.after_pipe",
        "datapipes.pipe_failed",
        "datapipes.pipe_skipped",
    )

    @inject
    def __init__(self, signal_provider: SignalProvider):
        self._signals = signal_provider
        self._lock = threading.Lock()
        self._execution_ids: Dict[str, str] = {}
        self._registered = False

    def register(self) -> None:
        """Connect the handlers. Idempotent."""
        if self._registered or self._signals is None:
            return
        for names, handler in (
            (self._SET_SIGNALS, self._on_started),
            (self._CLEAR_SIGNALS, self._on_finished),
        ):
            for name in names:
                signal = self._signals.get_signal(name) or self._signals.create_signal(name)
                signal.connect(handler, weak=False)
        self._registered = True

    def _on_started(self, sender, *, pipe_id=None, run_id=None, **kwargs):
        if pipe_id is None or run_id is None:
            return None
        with self._lock:
            self._execution_ids[str(pipe_id)] = str(run_id)
        return None

    def _on_finished(self, sender, *, pipe_id=None, **kwargs):
        if pipe_id is None:
            return None
        with self._lock:
            self._execution_ids.pop(str(pipe_id), None)
        return None

    def execution_id(self, pipe_id: str) -> Optional[str]:
        """The execution id of the running pipe, or None."""
        with self._lock:
            return self._execution_ids.get(str(pipe_id))


def current_execution_id(pipe_id: str) -> Optional[str]:
    """The captured execution id of ``pipe_id``, or None when nothing
    captured one (no executor, or the capture is not registered)."""
    try:
        return GlobalInjector.get(ProvenanceRunCapture).execution_id(pipe_id)
    except Exception:
        return None
