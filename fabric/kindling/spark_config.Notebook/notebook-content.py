# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from typing import Any, Dict, List, Optional, Type, Union
from abc import ABC, abstractmethod
from dynaconf import Dynaconf
from pyspark.sql import SparkSession
  
notebook_import('.injection')
notebook_import(".spark_session")

class ConfigService(ABC):
    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        pass
    
    @abstractmethod
    def get_all(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def using_env(self, env: str):
        pass

@GlobalInjector.singleton_autobind()
class DynaconfConfig(ConfigService):
    def __init__(
        self,
        spark_session: Optional[SparkSession] = None,
        initial_config: Optional[Dict[str, Any]] = None,
        config_files: Optional[List[str]] = None,
        env: str = "development",
        adls_enabled: bool = False,
        adls_account: Optional[str] = None,
        adls_container: Optional[str] = None,
        adls_config_path: Optional[str] = None,
        **dynaconf_kwargs
    ):
        self.spark = spark_session or get_or_create_spark_session()
        self.initial_config = initial_config or {}
        
        loaders = []

        #logger.debug(f"Synapse storage server: {server}")
        #logger.debug(f"Synapse storage account: {account}")

        if adls_enabled and adls_account and adls_container:
            try:
                import adls_loader
                loaders.append("adls_loader")
                
                dynaconf_kwargs["ADLS_FOR_DYNACONF_ENABLED"] = True
                dynaconf_kwargs["ADLS_ACCOUNT_FOR_DYNACONF"] = adls_account
                dynaconf_kwargs["ADLS_CONTAINER_FOR_DYNACONF"] = adls_container
                if adls_config_path:
                    dynaconf_kwargs["ADLS_CONFIG_PATH_FOR_DYNACONF"] = adls_config_path
            except ImportError:
                pass
                #logger.warn("Warning: ADLS loader not found. ADLS config loading disabled.")
        
        loaders.append("dynaconf.loaders.env_loader")
        
        settings_files = config_files or ["settings.toml", ".secrets.toml"]
        
        self.dynaconf = Dynaconf(
            settings_files=settings_files,
            environments=True,
            env=env,
            preload=[self.initial_config],
            merge_enabled=True,
            LOADERS_FOR_DYNACONF=loaders,
            **dynaconf_kwargs
        )
    
    def get(self, key: str, default: Any = None) -> Any:
        if self.spark:
            try:
                spark_value = self.spark.conf.get(key)
                if spark_value is not None:
                    return spark_value
            except:
                pass
        
        return self.dynaconf.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        if self.spark:
            try:
                self.spark.conf.set(key, value)
            except:
                pass
        
        self.dynaconf.set(key, value)
    
    def get_all(self) -> Dict[str, Any]:
        all_config = {}
        
        for key in self.dynaconf.to_dict().keys():
            all_config[key] = self.dynaconf.get(key)
        
        if self.spark:
            try:
                for key, value in self.spark.conf.getAll():
                    all_config[key] = value
            except:
                pass
        
        return all_config
    
    def using_env(self, env: str):
        return self.dynaconf.using_env(env)
    
    def __getattr__(self, name: str) -> Any:
        value = self.get(name)
        if value is None:
            raise AttributeError(f"No configuration found for '{name}'")
        return value
    
    def reload(self) -> None:
        self.dynaconf.reload()
    
    def get_fresh(self, key: str, default: Any = None) -> Any:
        if self.spark:
            try:
                spark_value = self.spark.conf.get(key)
                if spark_value is not None:
                    return spark_value
            except:
                pass
        
        return self.dynaconf.get_fresh(key, default=default)

from injector import inject, Injector, Module, singleton, provider
from typing import Any, Type

class ConfigModule(Module):
    @singleton
    @provider
    def provide_config(self) -> ConfigService:
        return DynaconfConfig(
            env="development",
            initial_config={
                "default_timeout": 30,
                "log_level": "INFO"
            },
            spark_session=get_or_create_spark_session()          
        )

def configure_injector():
    injector = Injector([ConfigModule()])
    return injector

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
