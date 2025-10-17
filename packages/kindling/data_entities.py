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

from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_log_provider import *


class EntityPathLocator(ABC):
    @abstractmethod
    def get_table_path(self, entity):
        pass


class EntityNameMapper(ABC):
    @abstractmethod
    def get_table_name(self, entity):
        pass


class EntityProvider(ABC):
    @abstractmethod
    def ensure_entity_table(self, entity):
        pass

    @abstractmethod
    def check_entity_exists(self, entity):
        pass

    @abstractmethod
    def merge_to_entity(self, df, entity):
        pass

    @abstractmethod
    def append_to_entity(self, df, entity):
        pass

    @abstractmethod
    def read_entity(self, entity):
        pass

    @abstractmethod
    def read_entity_as_stream(self, entity):
        pass

    @abstractmethod
    def read_entity_since_version(self, entity, since_version):
        pass

    @abstractmethod
    def write_to_entity(self, df, entity):
        pass

    @abstractmethod
    def get_entity_version(self, entity):
        pass

    @abstractmethod
    def append_as_stream(self, entity, df, checkpointLocation, format=None, options=None):
        pass


@dataclass
class EntityMetadata:
    entityid: str
    name: str
    partition_columns: List[str]
    merge_columns: List[str]
    tags: Dict[str, str]
    schema: Any


class DataEntities:

    deregistry = None

    @classmethod
    def entity(cls, **decorator_params):
        if cls.deregistry is None:
            cls.deregistry = GlobalInjector.get(DataEntityRegistry)
        # Check all required fields are provided
        required_fields = {field.name for field in fields(EntityMetadata)}
        missing_fields = required_fields - decorator_params.keys()

        if missing_fields:
            raise ValueError(f"Missing required fields in entity decorator: {missing_fields}")

        entityid = decorator_params["entityid"]

        del decorator_params["entityid"]

        cls.deregistry.register_entity(entityid, **decorator_params)

        return None


class DataEntityRegistry(ABC):
    @abstractmethod
    def register_entity(self, entityid, **decorator_params):
        pass

    @abstractmethod
    def get_entity_ids(self):
        pass

    @abstractmethod
    def get_entity_definition(self, name):
        pass


@GlobalInjector.singleton_autobind()
class DataEntityManager(DataEntityRegistry):
    @inject
    def __init__(self):
        self.registry = {}

    def register_entity(self, entityid, **decorator_params):
        self.registry[entityid] = EntityMetadata(entityid, **decorator_params)

    def get_entity_ids(self):
        return self.registry.keys()

    def get_entity_definition(self, name):
        return self.registry.get(name)
