from .injection import *
from .spark_log import *

from abc import ABC, abstractmethod
from injector import Injector, inject, singleton, Binder

class PythonLoggerProvider(ABC):
    @abstractmethod
    def get_logger(self, name: str, session = None):
        pass
 
@GlobalInjector.singleton_autobind()
class SparkLoggerProvider(PythonLoggerProvider):
    @inject
    def __init__(self, 
                 config: ConfigService):
        self.config = config

    def get_logger(self, name: str, session = None):
        return SparkLogger(name, config = self.config, session = session)