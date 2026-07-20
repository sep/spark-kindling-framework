import os
import time
import uuid
from functools import reduce
from pathlib import Path
from typing import Optional

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
from pyspark.sql.functions import col


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

    Subscribers can intercept at three points:

    ``read.resolve_read`` — fired before any read happens. Receives
    ``entity``, ``entity_id``, ``pipe``, ``pipe_id``, ``use_watermark``.
    A handler that takes ownership of the read returns a ``ResolvedRead``
    (see ``kindling.watermarking``) whose ``df`` becomes the read result —
    possibly ``None``, meaning "no new data, skip the pipe." When no
    handler resolves, the strategy falls through to an ordinary full read
    from the entity's provider. This is how incremental/watermark reads
    attach (``WatermarkAspect``) without the strategy knowing about them.

    ``read.after_read`` — fired after reading a source entity. Receives
    ``df``, ``entity_id``, ``pipe_id``, ``used_watermark``. Return a new
    DataFrame to replace the one passed to the pipe function, or ``None``
    to leave it unchanged.

    ``persist.before_persist`` — fired before writing the pipe output.
    Receives ``df``, ``pipe_id``, ``source_entity_id``, ``output_entity_id``,
    ``persist_id``. Return a new DataFrame to replace the one written to
    storage, or ``None`` to leave it unchanged.

    Watermark bookkeeping is deliberately NOT handled here: the
    ``WatermarkAspect`` (``kindling.watermarking``) listens to
    ``persist.after_persist`` / ``persist.persist_failed`` and advances the
    watermark to the cursor it captured at read time.
    """

    EMITS = [
        "read.resolve_read",
        "read.after_read",
        "persist.before_persist",
        "persist.after_persist",
        "persist.persist_failed",
    ]

    @inject
    def __init__(
        self,
        ep: EntityProvider,
        der: DataEntityRegistry,
        tp: SparkTraceProvider,
        lp: PythonLoggerProvider,
        provider_registry: EntityProviderRegistry,
        signal_provider: Optional[SignalProvider] = None,
    ):
        self.der = der
        self.ep = ep  # Legacy EntityProvider for backward compatibility
        self.provider_registry = provider_registry  # New multi-provider registry
        self.tp = tp
        self.logger = lp.get_logger("SimpleReadPersistStrategy")
        self._init_signal_emitter(signal_provider)

    def create_pipe_entity_reader(self, pipe):
        strategy = self

        def entity_reader(entity, usewm):
            # Interdiction point: a handler (e.g. WatermarkAspect) may take
            # ownership of the read entirely — including deciding there is
            # no new data (df=None). Distinct from read.after_read, which
            # can only transform a DataFrame that was already read.
            resolved = None
            for _, retval in strategy.emit(
                "read.resolve_read",
                entity=entity,
                entity_id=entity.entityid,
                pipe=pipe,
                pipe_id=pipe.pipeid,
                use_watermark=usewm,
            ):
                if retval is not None and hasattr(retval, "df"):
                    resolved = retval

            if resolved is not None:
                df = resolved.df
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

                # Driving-source convention: input 0 is the pipe's single
                # source of truth; other inputs are reference data. See
                # WatermarkAspect (kindling.watermarking) for the full
                # statement of the convention.
                src_input_entity = strategy.der.get_entity_definition(pipe.input_entity_ids[0])
                src_input_entity_id = src_input_entity.entityid

                output_entity = strategy.der.get_entity_definition(pipe.output_entity_id)
                output_provider = strategy.provider_registry.get_provider_for_entity(output_entity)

                strategy.logger.debug(f"persist_lambda - pipe = {pipe.pipeid}")

                results = strategy.emit(
                    "persist.before_persist",
                    df=df,
                    pipe_id=pipe.pipeid,
                    source_entity_id=src_input_entity_id,
                    output_entity_id=pipe.output_entity_id,
                    persist_id=persist_id,
                )
                df = _apply_df_transforms(results, df)

                try:
                    # Sink write mode override, shared with the streaming path
                    # (SimplePipeStreamStarter): "append" skips the merge even
                    # when the provider supports it (e.g. append-only fact
                    # tables); "merge" makes the merge requirement explicit —
                    # no silent append fallback. Unset keeps the default:
                    # merge when the provider can, append otherwise. The
                    # static tag value is validated at entity-registration
                    # time (data_entities._validate_write_mode_tag); checking
                    # inside the try keeps any raise paired with
                    # persist.persist_failed after persist.before_persist.
                    write_mode = (
                        str((output_entity.tags or {}).get("write.mode") or "").strip().lower()
                    )
                    if write_mode not in ("", "append", "merge"):
                        raise ValueError(
                            f"Entity '{output_entity.entityid}': invalid write.mode "
                            f"'{write_mode}' (expected 'append' or 'merge')"
                        )
                    if write_mode == "merge" and not hasattr(output_provider, "merge_to_entity"):
                        provider_type = (output_entity.tags or {}).get("provider_type", "unknown")
                        raise ValueError(
                            f"Entity '{output_entity.entityid}': write.mode is 'merge' but "
                            f"provider '{provider_type}' does not support merge operations"
                        )

                    with strategy.tp.span(
                        component="data_utils", operation="merge_and_watermark", reraise=False
                    ):
                        # Check if entity exists using provider
                        if output_provider.check_entity_exists(output_entity):
                            # Merge operation (Delta-specific, fallback to write for other providers)
                            with strategy.tp.span(operation="merge_to_entity_table"):
                                if write_mode != "append" and hasattr(
                                    output_provider, "merge_to_entity"
                                ):
                                    output_provider.merge_to_entity(df, output_entity)
                                else:
                                    # Append mode, or provider doesn't support merge
                                    from kindling.entity_provider import (
                                        WritableEntityProvider,
                                    )

                                    if isinstance(output_provider, WritableEntityProvider):
                                        if not write_mode:
                                            provider_type = (output_entity.tags or {}).get(
                                                "provider_type", "delta"
                                            )
                                            strategy.logger.info(
                                                f"Provider '{provider_type}' does not support merge, using append"
                                            )
                                        output_provider.append_to_entity(df, output_entity)
                                    else:
                                        provider_type = (output_entity.tags or {}).get(
                                            "provider_type", "unknown"
                                        )
                                        raise ValueError(
                                            f"Provider '{provider_type}' does not support write operations"
                                        )
                        else:
                            with strategy.tp.span(operation="write_to_entity_table", reraise=True):
                                from kindling.entity_provider import (
                                    WritableEntityProvider,
                                )

                                if isinstance(output_provider, WritableEntityProvider):
                                    output_provider.write_to_entity(df, output_entity)
                                else:
                                    provider_type = (output_entity.tags or {}).get(
                                        "provider_type", "unknown"
                                    )
                                    raise ValueError(
                                        f"Provider '{provider_type}' does not support write operations"
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
