# Fabric notebook source


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
    def __init__(self, wms: WatermarkService, ep: EntityProvider, der: DataEntityRegistry ):
        self.wms = wms
        self.der = der
        self.ep = ep
        self.logger = SparkLogger("SimpleReadPersistStrategy")

    def create_pipe_entity_reader(self, pipe: str):
        return lambda entity, usewm: self.wms.read_current_entity_changes(entity, pipe) if usewm else self.ep.read_entity(entity)

    def create_pipe_persist_activator(self, pipe: PipeMetadata):

        ##TODO: More intelligent processing -- parallelization, caching, error handling, skipping if inputs fail, etc.

        def persist_lambda(df):
            read_versions = {
                entityid: self.ep.get_entity_version(self.der.get_entity_definition(entityid)) for entityid in pipe.input_entity_ids 
            }

            self.logger.debug(f"read_versions - pipe = {pipe.pipeid} - map = {json.dumps(read_versions)}") 

            self.logger.debug(f"persist_lambda - pipe = {pipe.pipeid}")    
            output_entity = self.der.get_entity_definition(pipe.output_entity_id)
            filter_condition = reduce(lambda a, b: a | b, (col(c).isNull() for c in output_entity.merge_columns))

            with SparkTrace.current().span(component="data_utils", operation="merge_and_watermark", reraise=False):
                if(self.ep.check_entity_exists(output_entity)):
                    with SparkTrace.current().span(operation="merge_to_entity_table"):          
                        self.ep.merge_to_entity(df, output_entity)
                else:
                    with SparkTrace.current().span(operation="write_to_entity_table", reraise=True):  
                        self.ep.write_to_entity(df, output_entity)

                with SparkTrace.current().span(operation="save_watermarks"): 
                    src = pipe.input_entity_ids[0]
                    with SparkTrace.current().span(operation="save_watermark", component=f"{src}"):  
                        self.wms.save_watermark(src, pipe.pipeid, read_versions[src], str(uuid.uuid4()))

        return persist_lambda

