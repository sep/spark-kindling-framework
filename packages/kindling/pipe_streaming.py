from abc import ABC, abstractmethod

from kindling.data_entities import *
from kindling.data_pipes import *
from kindling.entity_provider import (
    can_ensure_destination,
    is_stream_mergeable,
    is_stream_writable,
    is_streamable,
)
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_log_provider import *
from kindling.spark_trace import *


class PipeStreamStarter(ABC):
    @abstractmethod
    def start_pipe_stream(self, pipeid, options=None) -> object:
        pass


@GlobalInjector.singleton_autobind()
class SimplePipeStreamStarter(PipeStreamStarter):
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
        self.logger = plp.get_logger("SimplePipeStreamStarter")
        self.logger.debug("SimplePipeStreamStarter initialized")
        self.cs = cs

    def start_pipe_stream(self, pipeid, options=None) -> object:
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
        # Prefer kwargs execution (consistent with batch pipe execution), but keep
        # backwards compatibility for single-input pipes declared as `def pipe(df): ...`.
        try:
            transformed_stream = pipe.execute(**input_entity_frames)
        except TypeError as kw_err:
            # Fall back to positional-only execution for legacy single-input pipes.
            if len(input_entity_frames) != 1:
                raise
            try:
                transformed_stream = pipe.execute(stream)
            except TypeError:
                # Preserve the more-informative kwargs failure.
                raise kw_err

        # Write output as stream
        base_chkpt_path = options.get("base_checkpoint_path") or self.cs.get(
            "kindling.storage.checkpoint_root"
        )
        if not base_chkpt_path:
            raise ValueError(
                "Missing streaming checkpoint root. "
                "Set kindling.storage.checkpoint_root or pass streaming_options['base_checkpoint_path']."
            )
        # Sink write mode: merge (per micro-batch upsert honoring the
        # entity's SCD1/SCD2 semantics) vs append. Mirrors the batch persist
        # strategy, which merges whenever the provider supports it: entities
        # that declare merge/business keys default to merge when the sink
        # provider can stream-merge. The `write.mode` entity tag (shared
        # with the batch persist path) forces either mode.
        write_mode = str(output_entity.tags.get("write.mode") or "").strip().lower()
        if write_mode not in ("", "append", "merge"):
            raise ValueError(
                f"Entity '{output_entity.entityid}': invalid write.mode "
                f"'{write_mode}' (expected 'append' or 'merge')"
            )
        if write_mode == "merge" and not is_stream_mergeable(output_provider):
            raise TypeError(
                f"Output provider for entity '{output_entity.entityid}' "
                f"(type={output_entity.tags.get('provider_type')}) "
                f"does not support streaming merges"
            )
        if not write_mode:
            wants_merge = is_stream_mergeable(output_provider) and getattr(
                output_entity, "merge_columns", None
            )
            write_mode = "merge" if wants_merge else "append"

        # Make the resolved mode visible at query start: whether a streaming
        # pipe merges or appends is derived (tag > merge_columns + provider
        # capability), and the answer decides how replayed micro-batches
        # land in the sink.
        self.logger.info(
            f"Streaming pipe '{pipeid}' -> entity '{output_entity.entityid}': "
            f"resolved sink write mode '{write_mode}'"
        )

        if write_mode == "append" and (
            not is_stream_writable(output_provider)
            and not hasattr(output_provider, "append_as_stream")
        ):
            raise TypeError(
                f"Output provider for entity '{output_entity.entityid}' "
                f"(type={output_entity.tags.get('provider_type')}) "
                f"does not support streaming writes"
            )
        mode = str(
            output_entity.tags.get("provider.access_mode")
            or self.cs.get("kindling.delta.access_mode")
            or "catalog"
        ).lower()

        # Ensure destination up front when the provider supports it.
        # This is important for streaming because some sinks require the destination
        # (table/path/topic/etc.) to exist before the query can start.
        if can_ensure_destination(output_provider):
            output_provider.ensure_destination(output_entity)
        else:
            # Backward compatibility: older providers expose `ensure_entity_table()`.
            ensure_output_table = getattr(output_provider, "ensure_entity_table", None)
            if callable(ensure_output_table):
                ensure_output_table(output_entity)

        if write_mode == "merge":
            # merge_as_stream starts the query itself (foreachBatch resolves
            # the target table internally), so no toTable()/start() step.
            # Recognized streaming options are forwarded so callers can set
            # the trigger and query name on merged sinks too.
            merge_options = {
                key: options[key] for key in ("trigger", "query_name") if key in options
            }
            return output_provider.merge_as_stream(
                transformed_stream,
                output_entity,
                f"{base_chkpt_path}/{pipe.pipeid}",
                options=merge_options,
            )

        stream_handle = output_provider.append_as_stream(
            transformed_stream, output_entity, f"{base_chkpt_path}/{pipe.pipeid}"
        )
        output_table = output_entity.tags.get("provider.table_name")
        output_path = output_entity.tags.get("provider.path")

        if mode == "catalog":
            if not output_table:
                enm = GlobalInjector.get(EntityNameMapper)
                output_table = enm.get_table_name(output_entity)
            if hasattr(stream_handle, "toTable"):
                return stream_handle.toTable(output_table)
            raise TypeError(
                f"Streaming sink for entity '{output_entity.entityid}' does not support table writes via toTable()"
            )

        if hasattr(stream_handle, "start"):
            # Non-file sinks (e.g., Kafka) commonly use `start()` with no path.
            # Table/path sinks should supply `provider.path` or be resolvable via EntityPathLocator.
            if not output_path:
                try:
                    output_path = self.epl.get_table_path(output_entity)
                except Exception:
                    output_path = None
            return stream_handle.start(output_path) if output_path else stream_handle.start()

        # Providers may choose to start the stream internally and return a StreamingQuery.
        return stream_handle
