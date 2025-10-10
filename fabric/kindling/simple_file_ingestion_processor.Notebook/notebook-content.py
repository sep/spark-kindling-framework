# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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
notebook_import(".platform_provider")
notebook_import(".file_ingestion")

@GlobalInjector.singleton_autobind()
class SimpleFileIngestionProcessor(FileIngestionProcessor):
    @inject
    def __init__(self, config: ConfigService, fir: FileIngestionRegistry, ep: EntityProvider, der: DataEntityRegistry, tp: SparkTraceProvider, lp: PythonLoggerProvider, pep: PlatformServiceProvider ):
        self.config = config
        self.fir = fir
        self.ep = ep
        self.der = der
        self.tp = tp
        self.logger = lp.get_logger("SimpleFileIngestionProcessor")
        self.spark = get_or_create_spark_session()
        self.env = pep.get_service()

    def process_path(self, path: str ):
        with self.tp.span(component="SimpleFileIngestionProcessor", operation="process_path",details={},reraise=True ): 
            filenames = self.env.list(path)

            import re

            for fn in filenames:
                fis = self.fir.get_entry_ids()
                for fi in fis:
                    fe = self.fir.get_entry_definition(fi)
                    pattern = re.compile(fe.patterns[0])
                    match = re.match(pattern, fn)
                    matched = not match is None

                    if matched:
                        self.logger.debug("Match, processing ")
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
                    else:
                        self.logger.debug("No matches")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
