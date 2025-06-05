# Fabric notebook source


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
notebook_import(".spark_config")
notebook_import(".spark_log_provider")

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

        print(decorator_params)

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
class FileIngestionManager(BaseServiceProvider, FileIngestionRegistry):
    @inject
    def __init__(self):
        print("File ingestion manager initialized ...")
        self.registry = {}

    def register_entry(self, entryId, **decorator_params):
        self.registry[entryId] = FileIngestionMetadata(entryId, **decorator_params)

    def get_entry_ids(self):
        return self.registry.keys()

    def get_entry_definition(self, entryId):
        return self.registry.get(entryId)

