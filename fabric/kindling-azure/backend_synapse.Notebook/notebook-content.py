# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from .base import NotebookBackend, BackendConfig, BackendFactory
from .fabric import (
    FabricCredentialProvider, FabricStorageProvider, FabricSparkProvider
)
from ..utils.exceptions import BackendError


class SynapseEnvironmentProvider:
    def __init__(self):
        import __main__
        self.spark = getattr(__main__, 'spark', None)
        self.mssparkutils = getattr(__main__, 'mssparkutils', None)
    
    def is_interactive_session(self) -> bool:
        try:
            if 'get_ipython' not in globals():
                try:
                    from IPython import get_ipython
                except ImportError:
                    return False
            else:
                get_ipython = globals()['get_ipython']
                
            ipython = get_ipython()
            if ipython is None:
                return False
                
            connection_file = ipython.config.get('IPKernelApp', {}).get('connection_file')
            return connection_file is not None
        except Exception:
            return False
    
    def get_workspace_info(self):
        info = {'platform': 'synapse'}
        
        try:
            if self.spark:
                info.update({
                    'workspace_id': self._get_spark_config('spark.synapse.workspace.id'),
                    'workspace_name': self._get_spark_config('spark.synapse.workspace.name'),
                    'workspace_endpoint': self._get_spark_config('spark.synapse.workspace.endpoint'),
                })
        except Exception:
            pass
        
        return info
    
    def get_cluster_info(self):
        info = {'platform': 'synapse'}
        
        try:
            if self.spark:
                info.update({
                    'application_id': self.spark.sparkContext.applicationId,
                    'application_name': self.spark.sparkContext.appName,
                    'spark_version': self.spark.version,
                })
        except Exception:
            pass
        
        return info
    
    def _get_spark_config(self, key: str, default=None):
        if not self.spark:
            return default
        
        try:
            return self.spark.conf.get(key, default)
        except Exception:
            try:
                all_configs = dict(self.spark.sparkContext.getConf().getAll())
                return all_configs.get(key, default)
            except Exception:
                return default


class SynapseArtifactProvider:
    pass


class SynapseBackend(NotebookBackend):
    def __init__(self, config: BackendConfig):
        super().__init__(config)
    
    @property
    def platform_name(self) -> str:
        return "synapse"
    
    @property
    def credential_provider(self):
        if self._credential_provider is None:
            self._credential_provider = FabricCredentialProvider()
        return self._credential_provider
    
    @property
    def storage_provider(self):
        if self._storage_provider is None:
            self._storage_provider = FabricStorageProvider()
        return self._storage_provider
    
    @property
    def artifact_provider(self):
        if self._artifact_provider is None:
            self._artifact_provider = SynapseArtifactProvider()
        return self._artifact_provider
    
    @property
    def spark_provider(self):
        if self._spark_provider is None:
            self._spark_provider = FabricSparkProvider()
        return self._spark_provider
    
    @property
    def environment_provider(self):
        if self._environment_provider is None:
            self._environment_provider = SynapseEnvironmentProvider()
        return self._environment_provider
    
    def initialize(self) -> None:
        pass
    
    def validate_environment(self) -> bool:
        try:
            import __main__
            spark = getattr(__main__, 'spark', None)
            if spark:
                app_name = spark.sparkContext.appName
                return 'synapse' in app_name.lower()
            return False
        except Exception:
            return False


BackendFactory.register_backend("synapse", SynapseBackend)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
