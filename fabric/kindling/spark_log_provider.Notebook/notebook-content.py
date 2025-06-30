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
    def get_logger(self, name: str, session = None):
        pass
 
@GlobalInjector.singleton_autobind()
class SparkLoggerProvider(PythonLoggerProvider):
    def get_logger(self, name: str, session = None):
        return SparkLogger(name, session = session)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
