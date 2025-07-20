# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from kindling.data_entities import DataEntities

from pyspark.sql.types import *
from types import SimpleNamespace
  
from kindling.data_entities import *
from kindling.data_pipes import *
from kindling.injection import GlobalInjector

from pyspark.sql.functions import *
import json
from datetime import timedelta

def start_pipe_as_stream_processor(pipeid, config = {}):
    dpr = GlobalInjector.get(DataPipesRegistry)
    ep = GlobalInjector.get(EntityProvider)
    der = GlobalInjector.get(DataEntityRegistry)
    epl = GlobalInjector.get(EntityPathLocator)
    pipe = dpr.get_pipe_definition(pipeid)
    streamInputEntity = der.get_entity_definition(pipe.input_entity_ids[0])
    srcfrmt = config.get("source_format", "delta")
    srccnfg = config.get("source_config", None) or {"path": epl.get_table_path(streamInputEntity)}
    stream = ep.read_entity_as_stream(streamInputEntity, srcfrmt, srccnfg)
    streamOutEntity = der.get_entity_definition(pipe.output_entity_id)
    transformed_stream = pipe.execute( stream )
    base_chkpt_path = config.get("base_checkpoint_path")
    query = ep.append_as_stream(streamOutEntity, transformed_stream, f"{base_chkpt_path}/{pipe.pipeid}").start(epl.get_table_path(streamOutEntity))
    return query


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
