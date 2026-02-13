from abc import ABC, abstractmethod

from kindling.data_entities import *
from kindling.data_pipes import *
from kindling.entity_provider import (
    StreamableEntityProvider,
    is_stream_writable,
    is_streamable,
)
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_log_provider import *
from kindling.spark_trace import *


class PipeStreamOrchestrator(ABC):
    @abstractmethod
    def start_pipe_as_stream_processor(self, pipeid, options=None) -> object:
        pass


@GlobalInjector.singleton_autobind()
class SimplePipeStreamOrchestrator(PipeStreamOrchestrator):
    @inject
    def __init__(
        self,
        cs: ConfigService,
        dpr: DataPipesRegistry,
        provider_registry: EntityProviderRegistry,
        der: DataEntityRegistry,
        epl: EntityPathLocator,
        plp: PythonLoggerProvider,
    ):
        self.dpr = dpr
        self.provider_registry = provider_registry
        self.der = der
        self.epl = epl
        self.logger = plp.get_logger("SimplePipeStreamOrchestrator")
        self.logger.debug(f"SimplePipeStreamOrchestrator initialized")
        self.cs = cs

    def start_pipe_as_stream_processor(self, pipeid, options=None) -> object:
        options = options or {}
        pipe = self.dpr.get_pipe_definition(pipeid)
        if not pipe.input_entity_ids:
            raise ValueError(f"Streaming pipe '{pipeid}' has no input entities")

        # Convention:
        # - first input entity is streaming input
        # - remaining input entities are direct (batch/static) reads for joins/lookups
        input_entity = self.der.get_entity_definition(pipe.input_entity_ids[0])
        output_entity = self.der.get_entity_definition(pipe.output_entity_id)

        # Resolve providers via registry based on entity tags
        input_provider = self.provider_registry.get_provider_for_entity(input_entity)
        output_provider = self.provider_registry.get_provider_for_entity(output_entity)

        # Read input as stream (provider knows its own format)
        if not is_streamable(input_provider):
            raise TypeError(
                f"Input provider for entity '{input_entity.entityid}' "
                f"(type={input_entity.tags.get('provider_type')}) "
                f"does not support streaming reads"
            )
        stream = input_provider.read_entity_as_stream(input_entity)
        input_entity_frames = {pipe.input_entity_ids[0].replace(".", "_"): stream}

        for static_entity_id in pipe.input_entity_ids[1:]:
            static_entity = self.der.get_entity_definition(static_entity_id)
            static_provider = self.provider_registry.get_provider_for_entity(static_entity)
            input_entity_frames[static_entity_id.replace(".", "_")] = static_provider.read_entity(
                static_entity
            )

        # Transform
        if len(input_entity_frames) == 1:
            transformed_stream = pipe.execute(stream)
        else:
            transformed_stream = pipe.execute(**input_entity_frames)

        # Write output as stream
        base_chkpt_path = options.get("base_checkpoint_path", None) or self.cs.get(
            "base_checkpoint_path"
        )
        if not is_stream_writable(output_provider) and not hasattr(
            output_provider, "append_as_stream"
        ):
            raise TypeError(
                f"Output provider for entity '{output_entity.entityid}' "
                f"(type={output_entity.tags.get('provider_type')}) "
                f"does not support streaming writes"
            )
        stream_handle = output_provider.append_as_stream(
            transformed_stream, output_entity, f"{base_chkpt_path}/{pipe.pipeid}"
        )
        output_path = output_entity.tags.get("provider.path") or self.epl.get_table_path(
            output_entity
        )
        return (
            stream_handle.start(output_path) if hasattr(stream_handle, "start") else stream_handle
        )
