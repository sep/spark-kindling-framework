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
notebook_import(".notebook_framework")

class PlatformEnvironmentProvider(ABC):
    @abstractmethod
    def get_service(self):
        pass
 
@GlobalInjector.singleton_autobind()
class SparkPlatformEnvironmentProvider(PlatformEnvironmentProvider):
    def get_service(self):
        return globals().get("platform_environment_service", None)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
