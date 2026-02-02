import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from kindling.common_transforms import *
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_log_provider import *
from kindling.spark_session import *
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
    def get_watermark_entity_for_entity(self, context: str):
        pass

    @abstractmethod
    def get_watermark_entity_for_layer(self, layer: str):
        pass


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

        df = (
            self.ep.read_entity(self.wef.get_watermark_entity_for_entity(source_entity_id))
            .filter((col("source_entity_id") == source_entity_id) & (col("reader_id") == reader_id))
            .select("last_version_processed")
            .limit(1)
        )

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
            return result
        elif currentVersion > watermark_version:
            self.logger.debug(
                f"read_current_changes - {entity.entityid} for {pipe.name}: Version: {currentVersion} -- Reading and transforming feed"
            )
            result = self.ep.read_entity_since_version(entity, currentVersion)
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
            return result
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
            return None
