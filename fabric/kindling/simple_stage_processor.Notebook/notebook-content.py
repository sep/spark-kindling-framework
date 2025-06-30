# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp, lit, row_number, when
from pyspark.sql.window import Window
from delta.tables import *
from typing import Dict
  
notebook_import(".spark_config")
notebook_import(".spark_trace")
notebook_import(".simple_read_persist_strategy")
notebook_import(".data_pipes")
notebook_import(".data_entities")
notebook_import(".injection")

def execute_process_stage( stage: str, stage_description:str, stage_details: Dict, layer: str ):
    #print(f"GlobalInjector ID = {GlobalInjector.get_instance_id()}")
    GlobalInjector.get(StageProcessingService).execute(stage, stage_description, stage_details, layer)
    
class StageProcessingService(ABC):
    @abstractmethod
    def execute( self, stage: str, stage_description:str, stage_details: Dict, layer: str ):
        pass

@GlobalInjector.singleton_autobind()
class StageProcessor(StageProcessingService):
    @inject
    def __init__(self, dpr: DataPipesRegistry, ep: EntityProvider, dep: DataPipesExecution, wef: WatermarkEntityFinder, tp: SparkTraceProvider ):
        self.wef = wef
        self.ep = ep
        self.dpr = dpr        
        self.dep = dep
        self.tp = tp

    def execute( self, stage: str, stage_description:str, stage_details: Dict, layer: str ):
        with self.tp.span(component=stage_description,operation=stage_description,details=stage_details,reraise=True ):        
            self.ep.ensure_entity_table(self.wef.get_watermark_entity_for_layer(layer))
            pipe_ids = self.dpr.get_pipe_ids()
            stage_pipe_ids = [pipe_id for pipe_id in pipe_ids if pipe_id.startswith(stage)]
            self.dep.run_datapipes(stage_pipe_ids)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
