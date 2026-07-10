import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from kindling.common_transforms import *
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
                StructField("last_version_processed", IntegerType(), False),
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
    def get_watermark(self, source_entity_id: str, reader_id: str) -> Optional[int]:
        pass

    @abstractmethod
    def save_watermark(
        self,
        source_entity_id: str,
        reader_id: str,
        last_version_processed: int,
        last_execution_id: str,
    ) -> DataFrame:
        pass

    @abstractmethod
    def read_current_entity_changes(self, entity, pipe):
        pass

    def read_changes_with_version(self, entity, pipe) -> Tuple[Optional[DataFrame], Optional[int]]:
        """Read unprocessed changes AND the source version the read covers.

        Returns ``(DataFrame, version)``; ``(None, None)`` means no new data.
        The returned version is the version the read is bounded to — the
        value the caller must record as the watermark once (and only once)
        the processed output is durably persisted. Capturing it at read
        time is what prevents a commit landing between read and persist
        from being marked processed without ever being read.
        """
        raise NotImplementedError


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
    ):
        self.wef = wef
        self.ep = ep
        self.logger = lp.get_logger("watermark")
        self.spark = get_or_create_spark_session()
        self._init_signal_emitter(signal_provider)

    def get_watermark(self, source_entity_id: str, reader_id: str) -> Optional[int]:
        self.logger.debug(f"Getting watermark for {source_entity_id}-{reader_id}")

        self.emit("watermark.before_get", source_entity_id=source_entity_id, reader_id=reader_id)

        try:
            df = (
                self.ep.read_entity(self.wef.get_watermark_entity_for_entity(source_entity_id))
                .filter(
                    (col("source_entity_id") == source_entity_id) & (col("reader_id") == reader_id)
                )
                .select("last_version_processed")
                .limit(1)
            )
        except AnalysisException as e:
            if "DELTA_MISSING_DELTA_TABLE" in str(e):
                self.logger.debug("Watermarks table does not exist yet")
                self.emit(
                    "watermark.watermark_missing",
                    source_entity_id=source_entity_id,
                    reader_id=reader_id,
                )
                return None
            raise

        if df.isEmpty():
            self.logger.debug("No watermark")
            self.emit(
                "watermark.watermark_missing",
                source_entity_id=source_entity_id,
                reader_id=reader_id,
            )
            return None

        version = df.first()["last_version_processed"]
        self.logger.debug(f"Watermark = {version}")
        self.emit(
            "watermark.watermark_found",
            source_entity_id=source_entity_id,
            reader_id=reader_id,
            version=version,
        )
        return version

    def save_watermark(
        self,
        source_entity_id: str,
        reader_id: str,
        last_version_processed: int,
        last_execution_id: str,
    ) -> DataFrame:
        self.emit(
            "watermark.before_save",
            source_entity_id=source_entity_id,
            reader_id=reader_id,
            last_version_processed=last_version_processed,
            last_execution_id=last_execution_id,
        )

        try:
            timestamp = datetime.fromtimestamp(time.time())

            data = [
                (
                    f"{source_entity_id}_{reader_id}",
                    source_entity_id,
                    reader_id,
                    timestamp,
                    last_version_processed,
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
                last_version_processed=last_version_processed,
                last_execution_id=last_execution_id,
            )

            return result

        except Exception as e:
            self.emit(
                "watermark.save_failed",
                source_entity_id=source_entity_id,
                reader_id=reader_id,
                last_version_processed=last_version_processed,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise

    def read_current_entity_changes(self, entity, pipe):
        df, _ = self.read_changes_with_version(entity, pipe)
        return df

    def read_changes_with_version(self, entity, pipe) -> Tuple[Optional[DataFrame], Optional[int]]:
        key_columns = entity.merge_columns
        self.logger.debug(
            f"read_current_changes - {entity.entityid} for {pipe.name}: {str(key_columns)}"
        )

        self.emit(
            "watermark.before_read_changes",
            entity_id=entity.entityid,
            pipe_id=pipe.pipeid,
            pipe_name=pipe.name,
        )

        watermark_version = self.get_watermark(entity.entityid, pipe.pipeid)
        currentVersion = self.ep.get_entity_version(entity)

        if watermark_version is None and currentVersion == 0:
            self.logger.debug(
                f"read_current_changes - {entity.entityid} for {pipe.name}: Source entity does not exist yet"
            )
            self.emit(
                "watermark.no_new_data",
                entity_id=entity.entityid,
                pipe_id=pipe.pipeid,
                pipe_name=pipe.name,
                current_version=currentVersion,
                watermark_version=watermark_version,
            )
            return None, None

        if watermark_version is None:
            self.logger.debug(
                f"read_current_changes - {entity.entityid} for {pipe.name}: No watermark"
            )
            result = remove_duplicates(
                self.ep.read_entity(entity)
                .withColumn("SourceVersion", lit(currentVersion).cast(IntegerType()))
                .transform(drop_if_exists, "SourceTimestamp")
                .withColumn(
                    "SourceTimestamp",
                    lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")).cast(
                        TimestampType()
                    ),
                ),
                key_columns,
            )
            self.emit(
                "watermark.after_read_changes",
                entity_id=entity.entityid,
                pipe_id=pipe.pipeid,
                pipe_name=pipe.name,
                current_version=currentVersion,
                watermark_version=None,
                has_data=True,
                is_initial_load=True,
            )
            return result, currentVersion
        elif currentVersion > watermark_version:
            self.logger.debug(
                f"read_current_changes - {entity.entityid} for {pipe.name}: "
                f"Reading changes for versions {watermark_version + 1}..{currentVersion}"
            )
            # Read from the version AFTER the watermark (startingVersion is
            # inclusive), bounded to currentVersion so the slice matches the
            # version recorded once the output persists. Reading from
            # currentVersion instead would silently skip every commit
            # between the watermark and the latest one.
            result = self.ep.read_entity_since_version(
                entity, watermark_version + 1, end_version=currentVersion
            )
            self.emit(
                "watermark.after_read_changes",
                entity_id=entity.entityid,
                pipe_id=pipe.pipeid,
                pipe_name=pipe.name,
                current_version=currentVersion,
                watermark_version=watermark_version,
                has_data=True,
                is_initial_load=False,
            )
            return result, currentVersion
        else:
            self.logger.debug(
                f"read_current_changes - {entity.entityid} for {pipe.name}: No new data"
            )
            self.emit(
                "watermark.no_new_data",
                entity_id=entity.entityid,
                pipe_id=pipe.pipeid,
                pipe_name=pipe.name,
                current_version=currentVersion,
                watermark_version=watermark_version,
            )
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
      performs the incremental changes read and records the source version
      that read covers, keyed by pipe.
    - ``persist.after_persist``: advances the watermark to the recorded
      version — the version that was READ, never re-fetched at persist
      time, so a commit landing between read and persist is never marked
      processed without being processed.
    - ``persist.persist_failed``: discards the recorded version; the
      watermark stays put and the next run re-reads the same slice
      (at-least-once, made safe by idempotent merge writes).

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
        self._pending: Dict[str, Tuple[str, int]] = {}
        self._registered = False

    def register(self) -> None:
        """Connect the aspect's handlers. Idempotent."""
        if self._registered:
            return
        for signal_name, handler in (
            ("read.resolve_read", self._on_resolve_read),
            ("persist.after_persist", self._on_after_persist),
            ("persist.persist_failed", self._on_persist_failed),
        ):
            signal = self._signals.get_signal(signal_name) or self._signals.create_signal(
                signal_name
            )
            signal.connect(handler, weak=False)
        self._registered = True
        self.logger.debug("WatermarkAspect registered")

    def _on_resolve_read(self, sender, *, entity=None, pipe=None, use_watermark=False, **kwargs):
        if not use_watermark or entity is None or pipe is None:
            return None
        df, version = self.wms.read_changes_with_version(entity, pipe)
        if version is not None:
            self._pending[pipe.pipeid] = (entity.entityid, version)
        else:
            self._pending.pop(pipe.pipeid, None)
        return ResolvedRead(df=df)

    def _on_after_persist(self, sender, *, pipe_id=None, persist_id=None, **kwargs):
        pending = self._pending.pop(pipe_id, None)
        if pending is None:
            return None
        source_entity_id, version = pending
        self.wms.save_watermark(source_entity_id, pipe_id, version, str(uuid.uuid4()))
        self.emit(
            "persist.watermark_saved",
            pipe_id=pipe_id,
            source_entity_id=source_entity_id,
            version=version,
            persist_id=persist_id,
        )
        return None

    def _on_persist_failed(self, sender, *, pipe_id=None, **kwargs):
        self._pending.pop(pipe_id, None)
        return None
