# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from typing import Dict, List, Optional, Union, Any, Set
from dataclasses import dataclass, field
import enum
import types
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass

@dataclass
class BackendConfig:
    workspace_id: Optional[str] = None
    endpoint: Optional[str] = None
    spark_configs: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.spark_configs is None:
            self.spark_configs = {}


class CredentialProvider(ABC):
    @abstractmethod
    def get_token(self, audience: str) -> str:
        pass


class StorageProvider(ABC):
    @abstractmethod
    def exists(self, path: str) -> bool:
        pass
    
    @abstractmethod
    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        pass
    
    @abstractmethod
    def read(self, path: str, encoding: str = 'utf-8') -> Union[str, bytes]:
        pass
    
    @abstractmethod
    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        pass
    
    @abstractmethod
    def list(self, path: str) -> List[str]:
        pass


class ArtifactProvider(ABC):
    @abstractmethod
    def get_notebooks(self) -> List[NotebookResource]:
        pass
    
    @abstractmethod
    def get_notebook(self, name: str) -> NotebookResource:
        pass
    
    @abstractmethod
    def create_notebook(self, name: str, notebook: NotebookResource) -> NotebookResource:
        pass
    
    @abstractmethod
    def update_notebook(self, name: str, notebook: NotebookResource) -> NotebookResource:
        pass
    
    @abstractmethod
    def delete_notebook(self, name: str) -> None:
        pass


class SparkProvider(ABC):
    @abstractmethod
    def get_spark_session(self):
        pass
    
    @abstractmethod
    def get_config(self, key: str, default: Any = None) -> Any:
        pass
    
    @abstractmethod
    def set_config(self, key: str, value: Any) -> None:
        pass
    
    @abstractmethod
    def get_log_level(self) -> str:
        pass


class EnvironmentProvider(ABC):
    @abstractmethod
    def is_interactive_session(self) -> bool:
        pass
    
    @abstractmethod
    def get_workspace_info(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def get_cluster_info(self) -> Dict[str, Any]:
        pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
