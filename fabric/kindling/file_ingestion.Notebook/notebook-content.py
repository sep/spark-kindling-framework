# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from dataclasses import dataclass, fields
from typing import Callable, List, Dict
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
 
import time
import logging
from typing import Callable
from typing import Any
 
from abc import ABC, abstractmethod
from injector import Injector, inject, singleton, Binder

notebook_import(".injection")
notebook_import(".spark_session")
notebook_import(".spark_config")
notebook_import(".spark_log_provider")
notebook_import(".data_entities")
notebook_import(".spark_trace")

@dataclass
class FileIngestionMetadata:
    entry_id: str
    name: str
    patterns: List[str]
    dest_entity_id: str
    tags: Dict[str, str]
    infer_schema: bool = True
 
class FileIngestionEntries:
    deregistry = None
    
    @classmethod
    def entry(cls, **decorator_params):
        if(cls.deregistry is None):
            cls.deregistry = GlobalInjector.get(FileIngestionRegistry)
        # Check all required fields are provided
        required_fields = {field.name for field in fields(FileIngestionMetadata)}

        decorator_params['infer_schema'] = decorator_params['infer_schema'] if ('infer_schema' in decorator_params.keys()) else True

        missing_fields = required_fields - decorator_params.keys() 

        if missing_fields:
            raise ValueError(f"Missing required fields in file ingestion decorator: {missing_fields}")
        
        destEntityId = decorator_params['entry_id']

        del decorator_params['entry_id']

        cls.deregistry.register_entry(destEntityId, **decorator_params)

        return None

class FileIngestionRegistry(ABC):
    @abstractmethod
    def register_entry(self, entryId, **decorator_params):
        pass

    @abstractmethod
    def get_entry_ids(self):
        pass

    @abstractmethod
    def get_entry_definition(self, entryId):
        pass

@GlobalInjector.singleton_autobind()
class FileIngestionManager(FileIngestionRegistry):
    @inject
    def __init__(self, lp: PythonLoggerProvider):
        self.logger = lp.get_logger("FileIngestionManager")
        self.logger.debug("File ingestion manager initialized ...")
        self.registry = {}

    def register_entry(self, entryId, **decorator_params):
        self.registry[entryId] = FileIngestionMetadata(entryId, **decorator_params)

    def get_entry_ids(self):
        return self.registry.keys()

    def get_entry_definition(self, entryId):
        return self.registry.get(entryId)

class FileIngestionProcessor(ABC):
    @abstractmethod
    def process_path(self, path: str ):
        pass

import notebookutils

@GlobalInjector.singleton_autobind()
class SimpleFileIngestionProcessor(FileIngestionProcessor):
    @inject
    def __init__(self, config: ConfigService, fir: FileIngestionRegistry, ep: EntityProvider, der: DataEntityRegistry, tp: SparkTraceProvider, lp: PythonLoggerProvider ):
        self.config = config
        self.fir = fir
        self.ep = ep
        self.der = der
        self.tp = tp
        self.server = self.config.get("SYNAPSE_STORAGE_SERVER")
        self.logger = lp.get_logger("SimpleFileIngestionProcessor")
        self.spark = get_or_create_spark_session()

    def process_path(self, path: str ):
        with self.tp.span(component="SimpleFileIngestionProcessor", operation="process_path",details={},reraise=True ):        
            file_list = notebookutils.fs.ls(path)

            filenames = [file.name for file in file_list if file.isFile]

            import re

            for fn in filenames:
                fis = self.fir.get_entry_ids()
                for fi in fis:
                    fe = self.fir.get_entry_definition(fi)
                    pattern = re.compile(fe.patterns[0])
                    match = re.match(pattern, fn)
                    matched = not match is None

                    if matched:
                        with self.tp.span(operation="ingest_on_match"):   
                            named_groups = match.groupdict()
                            dest_entity_id = fe.dest_entity_id.format(**named_groups)
                            self.logger.debug(f"Filename: {fn} Pattern: {fe.patterns[0]} Matched: {matched} Matches: {named_groups} DestEntityId: {dest_entity_id}")

                            filetype = named_groups['filetype']

                            de = self.der.get_entity_definition(dest_entity_id)

                            df = self.spark.read.format(filetype) \
                                .option("header", "true") \
                                .option("inferSchema", "true" if fe.infer_schema else "false" ) \
                                .load(f"{path}/{fn}")
                            with self.tp.span(operation="merge_to_entity"):   
                                self.ep.merge_to_entity( df, de )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
