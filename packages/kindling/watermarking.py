import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from kindling.common_transforms import *
from kindling.entity_provider import IncrementalReadableEntityProvider
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_log_provider import *
from kindling.spark_session import *
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, lit
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .data_entities import *


class WatermarkEntityFinder(ABC):
    @abstractmethod
    def get_watermark_entity_for_entity(self, context: str) -> Any:
        pass

    @abstractmethod
    def get_watermark_entity_for_layer(self, layer: str) -> Any:
        pass


@GlobalInjector.singleton_autobind()
class SimpleWatermarkEntityFinder(WatermarkEntityFinder):
    """Default implementation — stores all watermarks in a single system.watermarks Delta table."""

    def __init__(self):
        from types import SimpleNamespace

        schema = StructType(
            [
                StructField("watermark_id", StringType(), False),
                StructField("source_entity_id", StringType(), False),
                StructField("reader_id", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                # Legacy column: only meaningful for integer (Delta version)
                # cursors; -1 for providers with non-integer cursors. Kept
                # NOT NULL for compatibility with already-deployed tables.
                StructField("last_version_processed", IntegerType(), False),
                # The general watermark: an opaque cursor string defined and
                # interpreted by the source entity's provider (a Delta
                # version, a REST updated_at timestamp, queue offsets, ...).
                StructField("cursor", StringType(), True),
                StructField("last_execution_id", StringType(), False),
            ]
        )
        self._entity = SimpleNamespace(
            entityid="system.watermarks",
            name="watermarks",
            schema=schema,
            partition_columns=[],
            merge_columns=["watermark_id"],
            tags={"provider_type": "delta"},
        )

    def get_watermark_entity_for_entity(self, context: str) -> Any:
        return self._entity

    def get_watermark_entity_for_layer(self, layer: str) -> Any:
        return self._entity


class WatermarkService(ABC):
    """Abstract base for watermark management.

    Implementations MUST emit these signals:
        - watermark.before_get: Before fetching a watermark
        - watermark.watermark_found: When a watermark exists
        - watermark.watermark_missing: When no watermark exists (first run)
        - watermark.before_save: Before saving a watermark
        - watermark.after_save: After saving a watermark
        - watermark.save_failed: When watermark save fails
        - watermark.before_read_changes: Before reading entity changes
        - watermark.after_read_changes: After reading entity changes
        - watermark.no_new_data: When no new data is available
    """

    EMITS = [
        "watermark.before_get",
        "watermark.watermark_found",
        "watermark.watermark_missing",
        "watermark.before_save",
        "watermark.after_save",
        "watermark.save_failed",
        "watermark.before_read_changes",
        "watermark.after_read_changes",
        "watermark.no_new_data",
    ]

    @abstractmethod
    def get_cursor(self, source_entity_id: str, reader_id: str) -> Optional[str]:
        """Return the stored cursor for (source, reader), or None if this
        reader has never processed the source. The cursor is an opaque
        string defined and interpreted only by the source entity's provider
        (see ``IncrementalReadableEntityProvider``) — a Delta table version,
        a REST created/updated timestamp, queue offsets, etc."""
        pass

    @abstractmethod
    def save_cursor(
        self,
        source_entity_id: str,
        reader_id: str,
        cursor: str,
        last_execution_id: str,
    ) -> DataFrame:
        pass

    @abstractmethod
    def read_changes(self, entity, pipe) -> Tuple[Optional[DataFrame], Optional[str]]:
        """Read unprocessed changes AND the cursor the read covers.

        Returns ``(DataFrame, new_cursor)``; ``(None, None)`` means no new
        data. The returned cursor covers exactly the returned data — the
        value the caller must record as the watermark once (and only once)
        the processed output is durably persisted. Capturing it at read
        time is what prevents source changes landing between read and
        persist from being marked processed without ever being read.
        """
        pass

    # -- Legacy version-typed wrappers -----------------------------------
    # Delta cursors are stringified table versions; these wrappers keep the
    # historical integer-based API working for callers that know they are
    # talking to a version-cursor source.

    def get_watermark(self, source_entity_id: str, reader_id: str) -> Optional[int]:
        cursor = self.get_cursor(source_entity_id, reader_id)
        if cursor is None:
            return None
        try:
            return int(cursor)
        except ValueError:
            return None

    def save_watermark(
        self,
        source_entity_id: str,
        reader_id: str,
        last_version_processed: int,
        last_execution_id: str,
    ) -> DataFrame:
        return self.save_cursor(
            source_entity_id, reader_id, str(last_version_processed), last_execution_id
        )

    def read_current_entity_changes(self, entity, pipe):
        df, _ = self.read_changes(entity, pipe)
        return df


@GlobalInjector.singleton_autobind()
class WatermarkManager(WatermarkService, SignalEmitter):
    """Manages watermarks for incremental data processing with signal emissions."""

    @inject
    def __init__(
        self,
        ep: EntityProvider,
        wef: WatermarkEntityFinder,
        lp: PythonLoggerProvider,
        signal_provider: Optional[SignalProvider] = None,
        provider_registry: Optional[EntityProviderRegistry] = None,
    ):
        self.wef = wef
        self.ep = ep
        self.provider_registry = provider_registry
        self.logger = lp.get_logger("watermark")
        self.spark = get_or_create_spark_session()
        self._init_signal_emitter(signal_provider)

    def get_cursor(self, source_entity_id: str, reader_id: str) -> Optional[str]:
        self.logger.debug(f"Getting watermark cursor for {source_entity_id}-{reader_id}")

        self.emit("watermark.before_get", source_entity_id=source_entity_id, reader_id=reader_id)

        try:
            df = (
                self.ep.read_entity(self.wef.get_watermark_entity_for_entity(source_entity_id))
                .filter(
                    (col("source_entity_id") == source_entity_id) & (col("reader_id") == reader_id)
                )
                .limit(1)
            )
            row = None if df.isEmpty() else df.first()
        except AnalysisException as e:
            missing_markers = (
                "DELTA_MISSING_DELTA_TABLE",
                # Catalog-managed watermark tables surface as missing
                # tables/views rather than missing Delta paths.
                "TABLE_OR_VIEW_NOT_FOUND",
                "DELTA_TABLE_NOT_FOUND",
            )
            if any(marker in str(e) for marker in missing_markers):
                self.logger.debug("Watermarks table does not exist yet")
                self.emit(
                    "watermark.watermark_missing",
                    source_entity_id=source_entity_id,
                    reader_id=reader_id,
                )
                return None
            raise

        if row is None:
            self.logger.debug("No watermark")
            self.emit(
                "watermark.watermark_missing",
                source_entity_id=source_entity_id,
                reader_id=reader_id,
            )
            return None

        # Prefer the general cursor column; fall back to the legacy integer
        # column for rows written before the cursor column existed.
        cursor = row["cursor"] if "cursor" in df.columns else None
        if cursor is None:
            legacy = row["last_version_processed"]
            cursor = str(legacy) if legacy is not None and legacy >= 0 else None

        self.logger.debug(f"Watermark cursor = {cursor}")
        self.emit(
            "watermark.watermark_found",
            source_entity_id=source_entity_id,
            reader_id=reader_id,
            cursor=cursor,
        )
        return cursor

    def save_cursor(
        self,
        source_entity_id: str,
        reader_id: str,
        cursor: str,
        last_execution_id: str,
    ) -> DataFrame:
        self.emit(
            "watermark.before_save",
            source_entity_id=source_entity_id,
            reader_id=reader_id,
            cursor=cursor,
            last_execution_id=last_execution_id,
        )

        try:
            timestamp = datetime.fromtimestamp(time.time())

            # Legacy integer column: populated when the cursor is a version
            # number (Delta), -1 sentinel otherwise (column is NOT NULL on
            # already-deployed tables).
            try:
                legacy_version = int(cursor)
            except (TypeError, ValueError):
                legacy_version = -1

            data = [
                (
                    f"{source_entity_id}_{reader_id}",
                    source_entity_id,
                    reader_id,
                    timestamp,
                    legacy_version,
                    cursor,
                    last_execution_id,
                )
            ]

            df = self.spark.createDataFrame(
                data, self.wef.get_watermark_entity_for_entity(source_entity_id).schema
            )

            result = self.ep.merge_to_entity(
                df, self.wef.get_watermark_entity_for_entity(source_entity_id)
            )

            self.emit(
                "watermark.after_save",
                source_entity_id=source_entity_id,
                reader_id=reader_id,
                cursor=cursor,
                last_execution_id=last_execution_id,
            )

            return result

        except Exception as e:
            self.emit(
                "watermark.save_failed",
                source_entity_id=source_entity_id,
                reader_id=reader_id,
                cursor=cursor,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise

    def read_changes(self, entity, pipe) -> Tuple[Optional[DataFrame], Optional[str]]:
        self.logger.debug(f"read_changes - {entity.entityid} for {pipe.name}")

        self.emit(
            "watermark.before_read_changes",
            entity_id=entity.entityid,
            pipe_id=pipe.pipeid,
            pipe_name=pipe.name,
        )

        cursor = self.get_cursor(entity.entityid, pipe.pipeid)
        provider = self._provider_for(entity)

        if isinstance(provider, IncrementalReadableEntityProvider):
            df, new_cursor = provider.read_entity_changes(entity, cursor)
        else:
            df, new_cursor = self._legacy_version_read(provider, entity, cursor)

        if df is None:
            self.logger.debug(f"read_changes - {entity.entityid} for {pipe.name}: No new data")
            self.emit(
                "watermark.no_new_data",
                entity_id=entity.entityid,
                pipe_id=pipe.pipeid,
                pipe_name=pipe.name,
                cursor=cursor,
            )
            return None, None

        self.emit(
            "watermark.after_read_changes",
            entity_id=entity.entityid,
            pipe_id=pipe.pipeid,
            pipe_name=pipe.name,
            cursor=cursor,
            new_cursor=new_cursor,
            has_data=True,
            is_initial_load=cursor is None,
        )
        return df, new_cursor

    def _provider_for(self, entity):
        """Resolve the entity's own provider; fall back to the legacy
        single provider when no registry is available."""
        if self.provider_registry is not None:
            try:
                return self.provider_registry.get_provider_for_entity(entity)
            except Exception:  # noqa: BLE001
                self.logger.debug(
                    f"Provider registry lookup failed for {entity.entityid}; "
                    f"falling back to legacy provider"
                )
        return self.ep

    def _legacy_version_read(
        self, provider, entity, cursor: Optional[str]
    ) -> Tuple[Optional[DataFrame], Optional[str]]:
        """Version-based incremental read for providers that implement the
        legacy EntityProvider interface but not
        IncrementalReadableEntityProvider."""
        watermark_version = int(cursor) if cursor is not None else None

        if watermark_version is None and not provider.check_entity_exists(entity):
            # Existence is checked explicitly: version 0 is also a real,
            # readable table whose creation commit may already contain data.
            return None, None

        current_version = provider.get_entity_version(entity)

        if watermark_version is None:
            df = remove_duplicates(
                provider.read_entity(entity)
                .withColumn("SourceVersion", lit(current_version).cast(IntegerType()))
                .transform(drop_if_exists, "SourceTimestamp")
                .withColumn(
                    "SourceTimestamp",
                    lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")).cast(
                        TimestampType()
                    ),
                ),
                entity.merge_columns,
            )
            return df, str(current_version)

        if current_version > watermark_version:
            df = provider.read_entity_since_version(
                entity, watermark_version + 1, end_version=current_version
            )
            return df, str(current_version)

        return None, None


@dataclass
class ResolvedRead:
    """Marker returned by a ``read.resolve_read`` handler that has taken
    ownership of the read. ``df`` may be None, meaning "resolved: no new
    data" (the pipe should skip) — distinct from no handler resolving at
    all, which falls through to an ordinary full provider read."""

    df: Optional[DataFrame]


@GlobalInjector.singleton_autobind()
class WatermarkAspect(SignalEmitter):
    """Incremental processing as a bolt-on signal aspect.

    Watermark behavior attaches to pipe execution through signals instead of
    being woven into the read/persist strategy:

    - ``read.resolve_read`` (sync, interdicting): when the read is for a
      pipe's driving source with ``use_watermark`` enabled, this aspect
      performs the incremental changes read and records the **cursor** that
      read covers, keyed by pipe. The cursor is an opaque, provider-defined
      incremental position — a Delta table version, a REST provider's
      created/updated timestamp, queue offsets — which this aspect stores
      and returns verbatim, never interprets (see
      ``IncrementalReadableEntityProvider``).
    - ``persist.after_persist``: advances the watermark to the recorded
      cursor — the cursor that was READ, never re-derived at persist
      time, so source changes landing between read and persist are never
      marked processed without being processed.
    - ``persist.persist_failed``: discards the recorded version; the
      watermark stays put and the next run re-reads the same slice
      (at-least-once, made safe by idempotent merge writes).
    - ``datapipes.pipe_failed`` / ``orchestrator.pipe_failed``: a pipe can
      fail BEFORE its persist runs (a reference-input read fails, the
      transform raises) — these discard the recorded cursor too, so a
      capture can never outlive the execution that made it. As a second
      guard, a non-watermarked read of a pipe's driving source (a
      full-refresh run) also clears any stale capture for that pipe.
    - ``datapipes.pipe_skipped`` / ``orchestrator.pipe_skipped``: a pipe
      whose driving read produced data can still be skipped when another
      input is unavailable — nothing persists, so the capture is discarded
      for the same reason.

    Concurrency contract: **concurrent executions of the same pipe within
    one process are unsupported.** This is inherent to the watermark
    model — there is exactly one cursor per (source, reader), so two
    concurrent incremental executions of one pipe would race on the cursor
    and merge overlapping slices regardless of this aspect's bookkeeping.
    Pending captures are therefore keyed by pipe id alone; both executers
    run a pipe at most once per run. If a watermarked capture replaces an
    existing one (a crashed run that emitted no lifecycle signals, or a
    genuinely concurrent overlapping run), the aspect logs a warning and
    last-capture-wins. If concurrent same-pipe execution ever becomes a
    requirement, the right shape is an execution-scoped token minted at
    read time and carried through the persist/failure signals — a
    deliberate future design, not something to approximate here.

    Driving-source convention: a pipe operates on a single source of
    truth — its FIRST input entity — and every other input is reference
    data, read in full. Only the driving source is watermarked. A table fed
    by multiple sources is built by multiple pipes each contributing its
    own driving source, not by one pipe with several watermarked inputs.
    (``DataPipesExecuter._populate_source_dict`` implements the read side
    of this convention by passing ``use_watermark`` only for input 0.)

    The aspect is registered at bootstrap for non-standalone platforms.
    Local/standalone execution deliberately runs without it: reads fall
    through to fixture/provider full reads, matching prior behavior where
    watermarking was skipped locally. An execution engine that owns its own
    incrementality (e.g. a declarative-pipelines backend) simply never
    registers the aspect.
    """

    EMITS = [
        "persist.watermark_saved",
    ]

    @inject
    def __init__(
        self,
        wms: WatermarkService,
        lp: PythonLoggerProvider,
        signal_provider: SignalProvider,
    ):
        self.wms = wms
        self.logger = lp.get_logger("WatermarkAspect")
        self._signals = signal_provider
        self._init_signal_emitter(signal_provider)
        # pipe_id -> (source_entity_id, cursor) captured at read time
        self._pending: Dict[str, Tuple[str, str]] = {}
        self._registered = False

    def register(self) -> None:
        """Connect the aspect's handlers. Idempotent."""
        if self._registered:
            return
        for signal_name, handler in (
            ("read.resolve_read", self._on_resolve_read),
            ("persist.after_persist", self._on_after_persist),
            ("persist.persist_failed", self._on_persist_failed),
            # A pipe can fail before its persist ever runs (a reference
            # input read fails, the transform raises). Both executers emit
            # a pipe-level failure signal; clear the captured version so it
            # cannot outlive the execution that captured it.
            ("datapipes.pipe_failed", self._on_pipe_failed),
            ("orchestrator.pipe_failed", self._on_pipe_failed),
            # A skipped pipe (driving read had data, another input didn't)
            # never persists — its capture dies with the execution too.
            ("datapipes.pipe_skipped", self._on_pipe_failed),
            ("orchestrator.pipe_skipped", self._on_pipe_failed),
        ):
            signal = self._signals.get_signal(signal_name) or self._signals.create_signal(
                signal_name
            )
            signal.connect(handler, weak=False)
        self._registered = True
        self.logger.debug("WatermarkAspect registered")

    def _on_resolve_read(self, sender, *, entity=None, pipe=None, use_watermark=False, **kwargs):
        if entity is None or pipe is None:
            return None
        if not use_watermark:
            # A non-watermarked read of the pipe's DRIVING source (a
            # full-refresh run, or a pipe with watermarking disabled)
            # supersedes any version captured by an earlier failed
            # watermarked execution. Clear it so the persist that follows
            # this read cannot save a stale watermark. Reference-input
            # reads (non-driving) must not clear the driving capture.
            input_ids = getattr(pipe, "input_entity_ids", None) or []
            if input_ids and entity.entityid == input_ids[0]:
                self._pending.pop(pipe.pipeid, None)
            return None
        df, cursor = self.wms.read_changes(entity, pipe)
        if cursor is not None:
            if pipe.pipeid in self._pending:
                # Every normal lifecycle path (persist, persist-failure,
                # pipe-failure, skip, full-refresh) clears the capture, so
                # finding one here means a prior run crashed without
                # emitting signals — or two executions of this pipe are
                # overlapping in this process, which the watermark model
                # does not support (one cursor per source/reader). See the
                # class docstring's concurrency contract.
                self.logger.warning(
                    f"Replacing existing pending watermark capture for pipe "
                    f"'{pipe.pipeid}' — prior execution ended without a "
                    f"lifecycle signal, or concurrent same-pipe executions "
                    f"are overlapping (unsupported). Last capture wins."
                )
            self._pending[pipe.pipeid] = (entity.entityid, cursor)
        else:
            self._pending.pop(pipe.pipeid, None)
        return ResolvedRead(df=df)

    def _on_after_persist(self, sender, *, pipe_id=None, persist_id=None, **kwargs):
        pending = self._pending.pop(pipe_id, None)
        if pending is None:
            return None
        source_entity_id, cursor = pending
        self.wms.save_cursor(source_entity_id, pipe_id, cursor, str(uuid.uuid4()))
        try:
            legacy_version = int(cursor)
        except (TypeError, ValueError):
            legacy_version = None
        self.emit(
            "persist.watermark_saved",
            pipe_id=pipe_id,
            source_entity_id=source_entity_id,
            cursor=cursor,
            version=legacy_version,
            persist_id=persist_id,
        )
        return None

    def _on_persist_failed(self, sender, *, pipe_id=None, **kwargs):
        self._pending.pop(pipe_id, None)
        return None

    def _on_pipe_failed(self, sender, *, pipe_id=None, **kwargs):
        self._pending.pop(pipe_id, None)
        return None
