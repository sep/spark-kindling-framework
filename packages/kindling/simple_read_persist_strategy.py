import time
import uuid
from functools import reduce
from typing import Optional

from kindling.data_entities import *
from kindling.data_pipes import *
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log import *
from kindling.watermarking import *
from pyspark.sql.functions import col


@GlobalInjector.singleton_autobind()
class SimpleReadPersistStrategy(EntityReadPersistStrategy, SignalEmitter):
    """Strategy for reading entities and persisting pipe results with signals."""

    @inject
    def __init__(
        self,
        wms: WatermarkService,
        ep: EntityProvider,
        der: DataEntityRegistry,
        tp: SparkTraceProvider,
        lp: PythonLoggerProvider,
        provider_registry: EntityProviderRegistry,
        signal_provider: Optional[SignalProvider] = None,
    ):
        self.wms = wms
        self.der = der
        self.ep = ep  # Legacy EntityProvider for backward compatibility
        self.provider_registry = provider_registry  # New multi-provider registry
        self.tp = tp
        self.logger = lp.get_logger("SimpleReadPersistStrategy")
        self._init_signal_emitter(signal_provider)

    def create_pipe_entity_reader(self, pipe: str):
        def entity_reader(entity, usewm):
            if usewm:
                # Watermarking uses the legacy path (Delta-specific)
                return self.wms.read_current_entity_changes(entity, pipe)
            else:
                # Get appropriate provider for this entity
                provider = self.provider_registry.get_provider_for_entity(entity)
                return provider.read_entity(entity)

        return entity_reader

    def create_pipe_persist_activator(self, pipe):
        # Capture self for use in closure
        strategy = self

        ##TODO: More intelligent processing -- parallelization, caching, error handling, skipping if inputs fail, etc.

        def persist_lambda(df):
            persist_id = str(uuid.uuid4())
            start_time = time.time()

            if pipe.input_entity_ids and len(pipe.input_entity_ids) > 0:

                src_input_entity = strategy.der.get_entity_definition(pipe.input_entity_ids[0])
                src_input_entity_id = src_input_entity.entityid

                # Get provider for source entity (version tracking may be Delta-specific)
                src_provider = strategy.provider_registry.get_provider_for_entity(src_input_entity)
                src_read_version = (
                    src_provider.get_entity_version(src_input_entity)
                    if hasattr(src_provider, "get_entity_version")
                    else 0
                )

                output_entity = strategy.der.get_entity_definition(pipe.output_entity_id)
                output_provider = strategy.provider_registry.get_provider_for_entity(output_entity)

                strategy.logger.debug(
                    f"read_version - pipe = {pipe.pipeid} - ver = {src_read_version}"
                )
                strategy.logger.debug(f"persist_lambda - pipe = {pipe.pipeid}")

                strategy.emit(
                    "persist.before_persist",
                    pipe_id=pipe.pipeid,
                    source_entity_id=src_input_entity_id,
                    output_entity_id=pipe.output_entity_id,
                    source_version=src_read_version,
                    persist_id=persist_id,
                )

                try:
                    with strategy.tp.span(
                        component="data_utils", operation="merge_and_watermark", reraise=False
                    ):
                        # Check if entity exists using provider
                        if output_provider.check_entity_exists(output_entity):
                            # Merge operation (Delta-specific, fallback to write for other providers)
                            with strategy.tp.span(operation="merge_to_entity_table"):
                                if hasattr(output_provider, "merge_to_entity"):
                                    output_provider.merge_to_entity(df, output_entity)
                                else:
                                    # Provider doesn't support merge, use append instead
                                    strategy.logger.info(
                                        f"Provider '{output_entity.provider_type or 'delta'}' does not support merge, using append"
                                    )
                                    output_provider.append_to_entity(df, output_entity)
                        else:
                            with strategy.tp.span(operation="write_to_entity_table", reraise=True):
                                output_provider.write_to_entity(df, output_entity)

                        with strategy.tp.span(operation="save_watermarks"):
                            with strategy.tp.span(
                                operation="save_watermark", component=f"{src_input_entity_id}"
                            ):
                                strategy.wms.save_watermark(
                                    src_input_entity_id,
                                    pipe.pipeid,
                                    src_read_version,
                                    str(uuid.uuid4()),
                                )

                                strategy.emit(
                                    "persist.watermark_saved",
                                    pipe_id=pipe.pipeid,
                                    source_entity_id=src_input_entity_id,
                                    version=src_read_version,
                                    persist_id=persist_id,
                                )

                    duration = time.time() - start_time
                    strategy.emit(
                        "persist.after_persist",
                        pipe_id=pipe.pipeid,
                        source_entity_id=src_input_entity_id,
                        output_entity_id=pipe.output_entity_id,
                        duration_seconds=duration,
                        persist_id=persist_id,
                    )

                except Exception as e:
                    duration = time.time() - start_time
                    strategy.emit(
                        "persist.persist_failed",
                        pipe_id=pipe.pipeid,
                        source_entity_id=src_input_entity_id,
                        output_entity_id=pipe.output_entity_id,
                        error=str(e),
                        error_type=type(e).__name__,
                        duration_seconds=duration,
                        persist_id=persist_id,
                    )
                    raise

        return persist_lambda
