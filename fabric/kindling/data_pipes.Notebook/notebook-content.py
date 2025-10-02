# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

import time
import logging
from dataclasses import dataclass, fields
from typing import Callable, List, Dict, Any
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from abc import ABC, abstractmethod
from injector import Injector, inject, singleton, Binder

notebook_import(".injection")
notebook_import(".spark_log_provider")
notebook_import(".spark_trace")
notebook_import(".data_entities")

@dataclass
class PipeMetadata:
    pipeid: str
    name: str        
    execute: Callable
    tags: Dict[str,str]
    input_entity_ids: List[str]
    output_entity_id: str
    output_type: str

class EntityReadPersistStrategy(ABC):
    @abstractmethod
    def create_pipe_entity_reader(self, pipe: str):
        pass

    @abstractmethod
    def create_pipe_persist_activator(self, pipe: PipeMetadata):
        pass

class DataPipes:
    dpregistry = None

    @classmethod
    def pipe(cls, **decorator_params):
        def decorator(func):
            if(cls.dpregistry is None):
                cls.dpregistry = GlobalInjector.get(DataPipesRegistry)
            decorator_params['execute'] = func
            required_fields = {field.name for field in fields(PipeMetadata)}
            missing_fields = required_fields - decorator_params.keys()
            
            if missing_fields:
                raise ValueError(f"Missing required fields in pipe decorator: {missing_fields}")

            pipeid = decorator_params['pipeid']
            del decorator_params['pipeid']
            cls.dpregistry.register_pipe(pipeid, **decorator_params)
            return func
        return decorator

class DataPipesRegistry(ABC):
    @abstractmethod
    def register_pipe(self, pipeid, **decorator_params):
        passabstractmethod

    @abstractmethod
    def get_pipe_ids(self):
        pass

    @abstractmethod
    def get_pipe_definition(self, name):
        pass

class DataPipesExecution(ABC):
    @abstractmethod
    def run_datapipes(self, pipes):
        pass

@GlobalInjector.singleton_autobind()
class DataPipesManager(DataPipesRegistry):
    @inject
    def __init__(self, lp: PythonLoggerProvider):
        self.registry = {}
        self.data_pipes_logger = lp.get_logger("data_pipes_manager") 
        self.data_pipes_logger.debug("Data pipes manager initialized ...")

    def register_pipe(self, pipeid, **decorator_params):
        self.registry[pipeid] = PipeMetadata(pipeid, **decorator_params)
        self.data_pipes_logger.debug(f"Pipe registered: {pipeid}")

    def get_pipe_ids(self):
        return self.registry.keys()

    def get_pipe_definition(self, name):
        return self.registry.get(name)

@GlobalInjector.singleton_autobind()
class DataPipesExecuter(DataPipesExecution):
    @inject
    def __init__(self, lp: PythonLoggerProvider, dpe: DataEntityRegistry, dpr: DataPipesRegistry, erps: EntityReadPersistStrategy, tp: SparkTraceProvider):
        self.erps = erps
        self.dpr = dpr
        self.dpe = dpe
        self.data_pipes_logger = lp.get_logger("data_pipes_executer") 
        self.tp = tp

    def run_datapipes(self, pipes):
        pipe_entity_reader = self.erps.create_pipe_entity_reader
        pipe_activator = self.erps.create_pipe_persist_activator

        with self.tp.span(component="data_pipes_executer", operation="execute_all_datapipes"):
            for pipeid in pipes:
                with self.tp.span(operation="execute_datapipe", component=f"pipe-{pipeid}"):
                    pipe = self.dpr.get_pipe_definition(pipeid)
                    self._execute_datapipe(pipe_entity_reader(pipe), pipe_activator(pipe), pipe )

    def _execute_datapipe(self, entity_reader: Callable[[str],DataFrame],activator: Callable[[DataFrame], None], pipe: PipeMetadata) -> DataFrame:
        input_entities = self._populate_source_dict(entity_reader, pipe)
        first_source = list(input_entities.values())[0]
        self.data_pipes_logger.debug(f"Prepping data pipe: {pipe.pipeid}")
        if(first_source is not None):
            self.data_pipes_logger.debug(f"Running data pipe: {pipe.pipeid}")
            processedDf = pipe.execute( **input_entities )
            activator(processedDf)
        else:
            self.data_pipes_logger.debug(f"Skipping data pipe: {pipe.pipeid}")

    def _populate_source_dict(self, entity_reader: Callable[[str], DataFrame], pipe) -> dict[str, DataFrame]:
        result = {}
        for i, entity_id in enumerate(pipe.input_entity_ids):
            is_first = (i == 0)  # True for the first entity, False for others
            key = entity_id.replace(".", "_")
            result[key] = entity_reader(self.dpe.get_entity_definition(entity_id), is_first)
        return result

class StageProcessingService(ABC):
    @abstractmethod
    def execute( self, stage: str, stage_description:str, stage_details: Dict, layer: str, preprocessor: Callable ):
        pass



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

