# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from typing import Dict, List, Optional, Any, Union

from .base import (
    NotebookBackend, BackendConfig, CredentialProvider, StorageProvider,
    ArtifactProvider, SparkProvider, EnvironmentProvider, BackendFactory
)
from ..utils.exceptions import BackendError, AuthenticationError


class DatabricksCredentialProvider(CredentialProvider):
    def get_token(self, audience: str) -> str:
        try:
            import __main__
            dbutils = getattr(__main__, 'dbutils', None)
            if dbutils:
                return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
            raise Exception("dbutils not available")
        except Exception as e:
            raise AuthenticationError(f"Failed to get Databricks token: {str(e)}") from e


class DatabricksStorageProvider(StorageProvider):
    def __init__(self):
        import __main__
        self.dbutils = getattr(__main__, 'dbutils', None)
        if not self.dbutils:
            raise BackendError("dbutils not available")
    
    def exists(self, path: str) -> bool:
        try:
            self.dbutils.fs.ls(path)
            return True
        except Exception:
            return False
    
    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        self.dbutils.fs.cp(source, destination, overwrite)
    
    def read(self, path: str, encoding: str = 'utf-8') -> Union[str, bytes]:
        try:
            with open(path, 'r' if encoding else 'rb') as f:
                return f.read()
        except Exception as e:
            raise BackendError(f"Failed to read file: {str(e)}") from e
    
    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        try:
            mode = 'w' if isinstance(content, str) else 'wb'
            with open(path, mode) as f:
                f.write(content)
        except Exception as e:
            raise BackendError(f"Failed to write file: {str(e)}") from e
    
    def list(self, path: str) -> List[str]:
        try:
            files = self.dbutils.fs.ls(path)
            return [f.name for f in files]
        except Exception as e:
            raise BackendError(f"Failed to list directory: {str(e)}") from e


class DatabricksArtifactProvider(ArtifactProvider):
    def __init__(self, credential_provider: CredentialProvider, config: BackendConfig):
        self.credential_provider = credential_provider
        self.config = config
    
    def get_notebooks(self):
        pass
    
    def get_notebook(self, name: str):
        pass
    
    def create_notebook(self, name: str, notebook):
        pass
    
    def update_notebook(self, name: str, notebook):
        pass
    
    def delete_notebook(self, name: str):
        pass


class DatabricksSparkProvider(SparkProvider):
    def __init__(self):
        import __main__
        self.spark = getattr(__main__, 'spark', None)
        if not self.spark:
            raise BackendError("Spark session not available")
    
    def get_spark_session(self):
        return self.spark
    
    def get_config(self, key: str, default: Any = None) -> Any:
        try:
            return self.spark.conf.get(key, default)
        except Exception:
            try:
                all_configs = dict(self.spark.sparkContext.getConf().getAll())
                return all_configs.get(key, default)
            except Exception:
                return default
    
    def set_config(self, key: str, value: Any) -> None:
        try:
            self.spark.conf.set(key, value)
        except Exception as e:
            raise BackendError(f"Failed to set Spark config: {str(e)}") from e
    
    def get_log_level(self) -> str:
        try:
            return str(self.spark.sparkContext._jvm.org.apache.log4j.LogManager.getRootLogger().getLevel())
        except Exception:
            return "INFO"


class DatabricksEnvironmentProvider(EnvironmentProvider):
    def __init__(self):
        import __main__
        self.spark = getattr(__main__, 'spark', None)
        self.dbutils = getattr(__main__, 'dbutils', None)
    
    def is_interactive_session(self) -> bool:
        try:
            if self.dbutils:
                context = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                return context.notebookPath().isDefined()
            return False
        except Exception:
            return False
    
    def get_workspace_info(self) -> Dict[str, Any]:
        info = {'platform': 'databricks'}
        
        try:
            if self.dbutils:
                context = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                info.update({
                    'workspace_url': context.browserHostName().get(),
                    'workspace_id': context.workspaceId().get(),
                    'notebook_path': context.notebookPath().get() if context.notebookPath().isDefined() else None,
                })
        except Exception:
            pass
        
        return info
    
    def get_cluster_info(self) -> Dict[str, Any]:
        info = {'platform': 'databricks'}
        
        try:
            if self.spark:
                info.update({
                    'application_id': self.spark.sparkContext.applicationId,
                    'spark_version': self.spark.version,
                })
            
            if self.dbutils:
                context = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                info.update({
                    'cluster_id': context.clusterId().get(),
                    'cluster_name': context.clusterName().get() if context.clusterName().isDefined() else None,
                })
        except Exception:
            pass
        
        return info


class DatabricksBackend(NotebookBackend):
    def __init__(self, config: BackendConfig):
        super().__init__(config)
    
    @property
    def platform_name(self) -> str:
        return "databricks"
    
    @property
    def credential_provider(self):
        if self._credential_provider is None:
            self._credential_provider = DatabricksCredentialProvider()
        return self._credential_provider
    
    @property
    def storage_provider(self):
        if self._storage_provider is None:
            self._storage_provider = DatabricksStorageProvider()
        return self._storage_provider
    
    @property
    def artifact_provider(self):
        if self._artifact_provider is None:
            self._artifact_provider = DatabricksArtifactProvider(
                self.credential_provider, self.config
            )
        return self._artifact_provider
    
    @property
    def spark_provider(self):
        if self._spark_provider is None:
            self._spark_provider = DatabricksSparkProvider()
        return self._spark_provider
    
    @property
    def environment_provider(self):
        if self._environment_provider is None:
            self._environment_provider = DatabricksEnvironmentProvider()
        return self._environment_provider
    
    def initialize(self) -> None:
        pass
    
    def validate_environment(self) -> bool:
        try:
            import __main__
            
            if not hasattr(__main__, 'dbutils'):
                return False
            
            dbutils = getattr(__main__, 'dbutils')
            if dbutils is None:
                return False
            
            return True
        except Exception:
            return False


BackendFactory.register_backend("databricks", DatabricksBackend)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
