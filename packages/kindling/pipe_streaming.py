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

        # Driving-source convention: a pipe declares its source-of-truth
        # inputs with ``driving_entity_ids``; this set is not inferred from
        # position. Omitting ``driving_entity_ids`` resolves to the first
        # declared input, preserving the single-driving default used by
        # existing streaming pipes. Driving inputs are read as
        # streams; every other input is reference data read in full
        # (stream-static joins). The pipe body receives one kwarg per input
        # in ``input_entity_ids`` order and composes them itself -- the
        # starter does not union. Streaming offsets live in Spark's
        # checkpoint, so selection here depends only on declared driving
        # inputs; see the batch aspect for the corresponding convention.
        #
        # Two operational consequences of several driving inputs in one
        # query, documented rather than solved here:
        # - The query advances at the pace of its slowest source: Spark's
        #   global event-time progress is the minimum across sources.
        # - Adding a driving input later changes the query's source list,
        #   which an existing checkpoint cannot absorb cleanly; the query
        #   needs a new checkpoint, and replay into the sink has to be
        #   reasoned about.
        # Where per-source independence matters more than a single query,
        # the `flows` shape (N contributor pipes, engine-native) is the
        # better lowering -- see docs/proposals/collector_driving_inputs.md.
        driving_entity_ids = set(resolve_driving_entity_ids(pipe))

        input_entities = {}
        input_providers = {}
        for entity_id in pipe.input_entity_ids:
            entity = self.der.get_entity_definition(entity_id)
            input_entities[entity_id] = entity
            input_providers[entity_id] = self.provider_registry.get_provider_for_entity(entity)

        output_entity = self.der.get_entity_definition(pipe.output_entity_id)
        output_provider = self.provider_registry.get_provider_for_entity(output_entity)

        # Check every driving provider before opening any stream: a pipe with
        # one undeliverable driving declaration must not leave a half-built
        # plan or an opened stream behind.
        for entity_id in pipe.input_entity_ids:
            if entity_id not in driving_entity_ids:
                continue
            entity = input_entities[entity_id]
            if not is_streamable(input_providers[entity_id]):
                raise TypeError(
                    f"Input provider for entity '{entity.entityid}' "
                    f"(type={entity.tags.get('provider_type')}) "
                    f"does not support streaming reads"
                )

        input_entity_frames = {}
        for entity_id in pipe.input_entity_ids:
            entity = input_entities[entity_id]
            provider = input_providers[entity_id]
            if entity_id in driving_entity_ids:
                frame = provider.read_entity_as_stream(entity)
            else:
                frame = provider.read_entity(entity)
            input_entity_frames[entity_id.replace(".", "_")] = frame

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
                transformed_stream = pipe.execute(next(iter(input_entity_frames.values())))
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
        # Derived datasets are recomputed wholesale from their inputs; a
        # continuous sink has no "recompute" moment, so a derived entity
        # cannot be a streaming target. Materialize it with a batch pipe
        # or a declarative engine (where it lowers to a materialized view).
        if derived_config_from_tags(output_entity).enabled:
            raise TypeError(
                f"Entity '{output_entity.entityid}' is a derived dataset "
                "(dataset.kind='derived') and cannot be a streaming sink"
            )

        # Sink write mode: merge (per micro-batch upsert honoring the
        # entity's SCD1/SCD2 semantics), insert (per micro-batch
        # insert-if-absent) vs append. Mirrors the batch persist
        # strategy, which merges whenever the provider supports it: entities
        # that declare merge/business keys default to merge when the sink
        # provider can stream-merge. The `write.mode` entity tag (shared
        # with the batch persist path) forces the mode.
        write_mode = str(output_entity.tags.get("write.mode") or "").strip().lower()
        if write_mode not in ("", "append", "merge", "insert"):
            raise ValueError(
                f"Entity '{output_entity.entityid}': invalid write.mode "
                f"'{write_mode}' (expected 'append', 'merge' or 'insert')"
            )
        if write_mode in ("merge", "insert") and not is_stream_mergeable(output_provider):
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

        if write_mode in ("merge", "insert"):
            # merge_as_stream starts the query itself (foreachBatch resolves
            # the target table internally), so no toTable()/start() step.
            # "insert" rides the same sink: each micro-batch runs the batch
            # merge, and the provider picks the insert-only strategy from
            # the entity's write.mode tag.
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
