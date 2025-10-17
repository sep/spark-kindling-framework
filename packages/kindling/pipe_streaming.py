from abc import ABC, abstractmethod

from kindling.injection import *
from kindling.data_entities import *
from kindling.spark_log_provider import *
from kindling.data_pipes import *
from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_trace import *

class PipeStreamOrchestrator(ABC):
    @abstractmethod
    def start_pipe_as_stream_processor(self, pipeid, options = {}) -> object:
        pass

@GlobalInjector.singleton_autobind()
class SimplePipeStreamOrchestrator(PipeStreamOrchestrator):
    @inject
    def __init__(self, cs: ConfigService, dpr: DataPipesRegistry, ep: EntityProvider, der: DataEntityRegistry, epl: EntityPathLocator, plp: PythonLoggerProvider ):
        self.dpr = dpr
        self.ep = ep
        self.der = der
        self.epl = epl
        self.logger = plp.get_logger("SimplePipeStreamOrchestrator")
        self.logger.debug(f"SynapseStreamProcessor initialized")
        self.cs = cs

    def start_pipe_as_stream_processor(self, pipeid, options = {}) -> object:
        pipe = self.dpr.get_pipe_definition(pipeid)
        streamInputEntity = self.der.get_entity_definition(pipe.input_entity_ids[0])
        srcfrmt = options.get("source_format", "delta")
        srccnfg = options.get("source_config", None) or {"path": self.epl.get_table_path(streamInputEntity)}
        self.logger.debug(f"Src format = {srcfrmt}, cnfg = {srccnfg}")
        stream = self.ep.read_entity_as_stream(streamInputEntity, srcfrmt, srccnfg)
        streamOutEntity = self.der.get_entity_definition(pipe.output_entity_id)
        transformed_stream = pipe.execute( stream )
        base_chkpt_path = options.get("base_checkpoint_path", None) or self.cs.get("base_checkpoint_path")
        return self.ep.append_as_stream(streamOutEntity, transformed_stream, f"{base_chkpt_path}/{pipe.pipeid}").start(self.epl.get_table_path(streamOutEntity))


