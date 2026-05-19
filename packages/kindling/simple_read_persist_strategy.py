import os
import time
import uuid
from functools import reduce
from pathlib import Path
from typing import Optional

from pyspark.sql.functions import col

from kindling.data_entities import *
from kindling.data_pipes import *
from kindling.entity_provider_csv import (
    FixtureCSVEntityProvider,
    resolve_fixture_csv_path,
)
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log import *
from kindling.watermarking import *


def _is_local_execution() -> bool:
    """
    Return True when running in local/standalone mode.

    Checks the active platform service; falls back to True when no platform
    service is registered (e.g. during unit tests with no bootstrap).
    """
    try:
        from kindling.platform_provider import PlatformServiceProvider  # noqa: PLC0415

        provider = GlobalInjector.get(PlatformServiceProvider)
        svc = provider.get_service() if provider is not None else None
        if svc is None:
            # No platform bootstrapped — treat as local.
            return True
        platform_name = getattr(svc, "get_platform_name", lambda: "")()
        return str(platform_name).strip().lower() == "standalone"
    except Exception:  # noqa: BLE001
        # Best-effort: if we cannot determine the platform, assume non-local
        # to avoid silently skipping registered providers on real platforms.
        return False


def _apply_df_transforms(results, df):
    """Return the last non-None DataFrame returned by any signal subscriber."""
    if results:
        for _, retval in results:
            if retval is not None and hasattr(retval, "schema"):
                df = retval
    return df


@GlobalInjector.singleton_autobind()
class SimpleReadPersistStrategy(EntityReadPersistStrategy, SignalEmitter):
    """Strategy for reading entities and persisting pipe results with signals.

    Subscribers can intercept and transform DataFrames at two points:

    ``read.after_read`` — fired after reading a source entity. Receives
    ``df``, ``entity_id``, ``pipe_id``, ``used_watermark``. Return a new
    DataFrame to replace the one passed to the pipe function, or ``None``
    to leave it unchanged.

    ``persist.before_persist`` — fired before writing the pipe output.
    Receives ``df``, ``pipe_id``, ``source_entity_id``, ``output_entity_id``,
    ``source_version``, ``persist_id``. Return a new DataFrame to replace
    the one written to storage, or ``None`` to leave it unchanged.
    """

    EMITS = [
        "read.after_read",
        "persist.before_persist",
        "persist.after_persist",
        "persist.watermark_saved",
        "persist.persist_failed",
    ]

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
        strategy = self

        def entity_reader(entity, usewm):
            if usewm and not _is_local_execution():
                # Watermarking uses the legacy path (Delta-specific)
                df = strategy.wms.read_current_entity_changes(entity, pipe)
            elif _is_local_execution():
                # [implementer] tests/entities/ auto-discovery convention — ki-bsi
                # Priority: explicit --source > kindling.yaml env mapping > fixture CSV > registry
                # Fixture discovery applies only when running locally (standalone platform).
                cwd = Path(os.getcwd())
                fixture_path = resolve_fixture_csv_path(entity.entityid, cwd)
                if fixture_path is not None:
                    strategy.logger.info(
                        f"Using fixture CSV for entity '{entity.entityid}': {fixture_path}"
                    )
                    df = FixtureCSVEntityProvider(fixture_path).read_entity(entity)
                else:
                    provider = strategy.provider_registry.get_provider_for_entity(entity)
                    df = provider.read_entity(entity)
            else:
                provider = strategy.provider_registry.get_provider_for_entity(entity)
                df = provider.read_entity(entity)

            if df is not None:
                results = strategy.emit(
                    "read.after_read",
                    df=df,
                    entity_id=entity.entityid,
                    pipe_id=pipe.pipeid,
                    used_watermark=usewm,
                )
                df = _apply_df_transforms(results, df)

            return df

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
                # Version tracking: Delta providers return actual version numbers, other providers
                # default to 0 (no versioning). This is acceptable for watermarking as the
                # watermark system will still track processing state via timestamps.
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

                results = strategy.emit(
                    "persist.before_persist",
                    df=df,
                    pipe_id=pipe.pipeid,
                    source_entity_id=src_input_entity_id,
                    output_entity_id=pipe.output_entity_id,
                    source_version=src_read_version,
                    persist_id=persist_id,
                )
                df = _apply_df_transforms(results, df)

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
                                    # Provider doesn't support merge, use append if writable
                                    from kindling.entity_provider import (
                                        WritableEntityProvider,
                                    )

                                    if isinstance(output_provider, WritableEntityProvider):
                                        strategy.logger.info(
                                            f"Provider '{output_entity.provider_type or 'delta'}' does not support merge, using append"
                                        )
                                        output_provider.append_to_entity(df, output_entity)
                                    else:
                                        raise ValueError(
                                            f"Provider '{output_entity.provider_type or 'unknown'}' does not support write operations"
                                        )
                        else:
                            with strategy.tp.span(operation="write_to_entity_table", reraise=True):
                                from kindling.entity_provider import (
                                    WritableEntityProvider,
                                )

                                if isinstance(output_provider, WritableEntityProvider):
                                    output_provider.write_to_entity(df, output_entity)
                                else:
                                    raise ValueError(
                                        f"Provider '{output_entity.provider_type or 'unknown'}' does not support write operations"
                                    )

                        if pipe.use_watermark:
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
                        df=df,
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
