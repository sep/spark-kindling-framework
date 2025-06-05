# Fabric notebook source


# CELL ********************

from abc import ABC, abstractmethod
 
notebook_import('.spark_config')
notebook_import('.injection')
notebook_import('.spark_log')

class PythonLoggerProvider(ABC):
    @abstractmethod
    def get_logger(self, name: str):
        pass
 
@GlobalInjector.singleton_autobind()
class SparkLoggerProvider(BaseServiceProvider, PythonLoggerProvider):
    def get_logger(self, name: str):
        return SparkLogger(name)
