# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

notebook_import('.injection')
notebook_import('.spark_log')

class PythonLoggerProvider(ABC):
    @abstractmethod
    def get_logger(self, name: str):
        pass
 
@GlobalInjector.singleton_autobind()
class SparkLoggerProvider(PythonLoggerProvider):
    def get_logger(self, name: str):
        return SparkLogger(name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
