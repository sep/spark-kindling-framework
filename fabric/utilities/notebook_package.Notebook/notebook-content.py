# Fabric notebook source


# CELL ********************

from dataclasses import dataclass, fields
from typing import Callable, List, Dict

@dataclass
class NotebookPackage:
    name: str
    dependencies: List[str]
    tags: Dict[str, str]

class NotebookPackages:

    registry = {}
    
    @classmethod
    def register(cls, **decorator_params):
        # Check all required fields are provided
        required_fields = {field.name for field in fields(NotebookPackage)}
        missing_fields = required_fields - decorator_params.keys()
        
        if missing_fields:
            raise ValueError(f"Missing required fields in package decorator: {missing_fields}")
        
        package_name = decorator_params['name']

        del decorator_params['name']

        cls.register_package(package_name, **decorator_params)

        return None

    @classmethod
    def register_package(cls, name, **decorator_params):
        cls.registry[name] = NotebookPackage(name, **decorator_params)

    @classmethod
    def get_package_names(cls):
        return cls.registry.keys()
    
    @classmethod
    def get_package_definition(cls, name):
        return cls.registry.get(name)

