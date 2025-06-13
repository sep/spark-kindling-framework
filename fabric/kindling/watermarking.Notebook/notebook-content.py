# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, date_format 
from typing import Dict, Optional, List, Callable, Tuple
from datetime import datetime
import time
from pyspark.sql.types import (
    TimestampType,
    StringType,
    IntegerType,
    DateType,
    StructType,
    StructField
)
  
notebook_import(".spark_log_provider")
notebook_import(".data_entities")
notebook_import(".injection")
notebook_import(".spark_session")
notebook_import(".common_transforms")
notebook_import(".spark_config")

spark = get_or_create_spark_session()

from abc import ABC, abstractmethod
from injector import Injector, inject, singleton, Binder

class WatermarkEntityFinder(ABC):
    @abstractmethod
    def get_watermark_entity_for_entity(self, context:str):
        pass 

    @abstractmethod
    def get_watermark_entity_for_layer(self, layer:str):
        pass 


class WatermarkService(ABC):
    @abstractmethod
    def get_watermark(self, source_entity_id: str, reader_id: str) -> Optional[int]:
        pass

    @abstractmethod
    def save_watermark(self, source_entity_id: str, reader_id: str, 
                      last_version_processed: int, last_execution_id: str) -> DataFrame:
        pass
        
    @abstractmethod
    def read_current_entity_changes(self, entity, pipe ):
        pass

@GlobalInjector.singleton_autobind()
class WatermarkManager(BaseServiceProvider, WatermarkService):
    @inject
    def __init__(self, ep: EntityProvider, wef: WatermarkEntityFinder, lp: PythonLoggerProvider ):
        self.wef = wef
        self.ep = ep
        self.logger = lp.get_logger("watermark")

    def get_watermark(self, source_entity_id: str, reader_id: str) -> Optional[int]:
        self.logger.debug(f"Getting watermark for {source_entity_id}-{reader_id}")

        df = (self.ep.read_entity(self.wef.get_watermark_entity_for_entity(source_entity_id))
                .filter((col("source_entity_id") == source_entity_id) & 
                        (col("reader_id") == reader_id))
                .select("last_version_processed")
                .limit(1))
        
        if df.isEmpty():
            self.logger.debug("No watermark")
            return None

        self.logger.debug(f"Watermark = {df.first()['last_version_processed']}")
        return df.first()["last_version_processed"]

    def save_watermark(self, source_entity_id: str, reader_id: str, 
                      last_version_processed: int, last_execution_id: str) -> DataFrame:
        timestamp = datetime.fromtimestamp(time.time())

        data = [(
            f"{source_entity_id}_{reader_id}",
            source_entity_id,
            reader_id,
            timestamp,
            last_version_processed,
            last_execution_id
        )]

        df = (spark.createDataFrame(data, self.wef.get_watermark_entity_for_entity(source_entity_id).schema))

        return self.ep.merge_to_entity(df,self.wef.get_watermark_entity_for_entity(source_entity_id))

    def read_current_entity_changes(self, entity, pipe):
        key_columns = entity.merge_columns
        self.logger.debug(f"read_current_changes - {entity.entityid} for {pipe.name}: {str(key_columns)}")   
        watermark_version = self.get_watermark(entity.entityid, pipe.pipeid) 
        currentVersion = self.ep.get_entity_version(entity)

        if (watermark_version is None):
            self.logger.debug(f"read_current_changes - {entity.entityid} for {pipe.name}: No watermark")   
            return (remove_duplicates(self.ep.read_entity(entity).withColumn("SourceVersion",lit(currentVersion).cast(IntegerType())).transform(drop_if_exists,"SourceTimestamp").withColumn("SourceTimestamp",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss")).cast(TimestampType())),key_columns))
        elif (currentVersion > watermark_version):
            self.logger.debug(f"read_current_changes - {entity.entityid} for {pipe.name}: Version: {currentVersion} -- Reading and transforming feed")
            return self.ep.read_entity_since_version(entity, currentVersion)   
        else: 
            self.logger.debug(f"read_current_changes - {entity.entityid} for {pipe.name}: No new data")   
            return None
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
