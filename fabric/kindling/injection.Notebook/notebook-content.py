# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from abc import ABC
from injector import Injector, singleton
import inspect 

if 'GI_IMPORT_GUARD' not in globals():
    # Variable doesn't exist yet, so this is the first run
    GI_IMPORT_GUARD = True

    class GlobalInjector:
        _instance = None
        
        @classmethod
        def get_instance_id(cls):
            return id(cls.get_injector())

        @classmethod
        def get_injector(cls):
            if cls._instance is None:
                #print("Creating a new global injector instance ...")
                cls._instance = Injector(auto_bind=True)
            return cls._instance
        
        @classmethod
        def clear(cls):
            _instance = None

        @classmethod
        def get(cls, interface):
            #print(f"Injector ID: {id(cls.get_injector())}")
            return cls.get_injector().get(interface)
 
        @classmethod
        def bind(cls, interface, implementation):
            #print("Calling bind on GI ...")
            cls.get_injector().binder.bind(interface, to=implementation)
        
        @classmethod
        def autobind(cls, *interfaces):
            def decorator(cls_to_bind):
                # If no explicit interfaces provided, find parent interfaces
                if not interfaces:
                    # Get all direct parent classes
                    bases = cls_to_bind.__bases__
                    # Filter for those that are abstract
                    for base in bases:
                        if inspect.isabstract(base):
                            cls.bind(base, cls_to_bind)
                else:
                    # Bind to explicitly provided interfaces
                    for interface in interfaces:
                        cls.bind(interface, cls_to_bind)
                
                return cls_to_bind
            return decorator

        @classmethod
        def singleton_autobind(cls, *interfaces):
            #print("singleton_autobind invoked ...")
            def decorator(cls_to_bind):
                # First apply singleton to create a singleton-wrapped class
                singleton_cls = singleton(cls_to_bind)
                
                # Then bind the singleton-wrapped class to each interface with explicit scope
                for interface in interfaces or [base for base in cls_to_bind.__bases__ 
                                            if inspect.isabstract(base)]:
                    cls.get_injector().binder.bind(
                        interface, 
                        to=singleton_cls,
                        scope=singleton  # Explicitly setting singleton scope
                    )
                
                # Also bind the class to itself with explicit scope
                cls.get_injector().binder.bind(
                    cls_to_bind, 
                    to=singleton_cls,
                    scope=singleton
                )
                
                return singleton_cls
            return decorator

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
