# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

notebook_import(".spark_log")
notebook_import(".data_pipes")
notebook_import(".injection")
notebook_import(".watermarking")
 
from pyspark.sql.functions import col
import uuid
from functools import reduce

@GlobalInjector.singleton_autobind()
class SimpleReadPersistStrategy(BaseServiceProvider, EntityReadPersistStrategy):

    @inject
    def __init__(self, wms: WatermarkService, ep: EntityProvider, der: DataEntityRegistry, tp: SparkTraceProvider, lp: PythonLoggerProvider ):
        self.wms = wms
        self.der = der
        self.ep = ep
        self.tp = tp
        self.logger = lp.get_logger("SimpleReadPersistStrategy")

    def create_pipe_entity_reader(self, pipe: str):
        return lambda entity, usewm: self.wms.read_current_entity_changes(entity, pipe) if usewm else self.ep.read_entity(entity)

    def create_pipe_persist_activator(self, pipe):

        ##TODO: More intelligent processing -- parallelization, caching, error handling, skipping if inputs fail, etc.

        def persist_lambda(df):

            if( pipe.input_entity_ids and len(pipe.input_entity_ids) > 0 ):
                
                src_input_entity = self.der.get_entity_definition(pipe.input_entity_ids[0])

                src_input_entity_id = src_input_entity.entityid

                src_read_version = self.ep.get_entity_version(src_input_entity)

                self.logger.debug(f"read_version - pipe = {pipe.pipeid} - ver = {src_read_version}") 

                self.logger.debug(f"persist_lambda - pipe = {pipe.pipeid}")    
                output_entity = self.der.get_entity_definition(pipe.output_entity_id)

                with self.tp.span(component="data_utils", operation="merge_and_watermark", reraise=False):
                    if(self.ep.check_entity_exists(output_entity)):
                        with self.tp.span(operation="merge_to_entity_table"):          
                            self.ep.merge_to_entity(df, output_entity)
                    else:
                        with self.tp.span(operation="write_to_entity_table", reraise=True):  
                            self.ep.write_to_entity(df, output_entity)

                    with self.tp.span(operation="save_watermarks"): 
                        with self.tp.span(operation="save_watermark", component=f"{src_input_entity_id}"):  
                            self.wms.save_watermark(src_input_entity_id, pipe.pipeid, src_read_version, str(uuid.uuid4()))

        return persist_lambda


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
