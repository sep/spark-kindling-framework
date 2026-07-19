import dataclasses
import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from typing import Any, Callable, Dict, List, Optional

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from kindling.injection import *
from kindling.sentinels import UNSET
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log_provider import *
from kindling.spark_trace import *
from pyspark.sql import DataFrame

from .data_entities import *
from .data_entities import _raise_if_not_initialized


@dataclass
class PipeMetadata:
    pipeid: str
    name: str
    execute: Callable
    tags: Dict[str, str]
    input_entity_ids: List[str]
    output_entity_id: str
    output_type: str
    use_watermark: bool = False


class EntityReadPersistStrategy(ABC):
    """Abstract base for entity read/persist strategies.

    Implementations MUST emit these signals:
        - persist.before_persist: Before persisting pipe output
        - persist.after_persist: After successful persist
        - persist.persist_failed: When persist fails

    (persist.watermark_saved is emitted by WatermarkAspect, which listens
    to persist.after_persist — see kindling.watermarking.)
    """

    EMITS = [
        "persist.before_persist",
        "persist.after_persist",
        "persist.persist_failed",
    ]

    @abstractmethod
    def create_pipe_entity_reader(self, pipe: "PipeMetadata"):
        pass

    @abstractmethod
    def create_pipe_persist_activator(self, pipe: PipeMetadata):
        pass


class _PipeIds:
    """Auto-populated namespace of pipe ID string constants.

    Each registered pipe gets an attribute whose name is the pipeid
    with dots and hyphens replaced by underscores, and whose value is
    the pipeid string.

    Example::

        @DataPipes.pipe(pipeid="bronze_to_silver_orders", ...)
        def transform(df): ...

        # DataPipes.ids.bronze_to_silver_orders == "bronze_to_silver_orders"
    """


class DataPipes:
    dpregistry = None
    ids = _PipeIds()

    # [implementer] expose public test reset API — TASK-20260430-001
    @classmethod
    def reset(cls) -> None:
        """Reset the pipe registry. Use between tests to prevent state pollution."""
        cls.dpregistry = None
        cls.ids = _PipeIds()

    @classmethod
    def view(
        cls,
        *,
        pipeid: str,
        name: str = None,
        input_entity_ids: List[str],
        output_entity_id: str,
        sql: str = None,
        sql_file: str = None,
        tags: Dict[str, str] = None,
    ):
        """Register a SQL-driven view pipe backed by the memory entity provider.

        Pure SQL form (no decorator needed):
            DataPipes.view(
                pipeid="view.enriched",
                input_entity_ids=["bronze.orders", "bronze.customers"],
                output_entity_id="view.enriched",
                sql_file="views/enriched.sql",
            )

        With post-processing (decorator form):
            @DataPipes.view(
                pipeid="view.enriched",
                input_entity_ids=["bronze.orders"],
                output_entity_id="view.enriched",
                sql_file="views/enriched.sql",
            )
            def enriched(df):
                return df.filter(df.amount > 0)

        Input entities are registered as Spark temp views named by replacing dots
        with underscores (e.g. ``bronze.orders`` → ``bronze_orders``). The SQL
        (from ``sql_file`` or ``sql=``) references those view names. ``sql_file``
        is resolved relative to the calling module's directory.
        """
        import inspect
        from pathlib import Path as _Path

        if sql is None and sql_file is None:
            raise ValueError("DataPipes.view() requires either 'sql' or 'sql_file'")

        sql_file_abs = None
        if sql_file is not None:
            caller_file = inspect.stack()[1][1]
            sql_file_abs = str(_Path(caller_file).parent / sql_file)

        resolved_name = name or pipeid
        resolved_tags = {"pipe_type": "view", "provider_type": "memory", **(tags or {})}

        def _build_execute(post_fn=None):
            _sql = sql
            _sf = sql_file_abs
            _ids = list(input_entity_ids)

            def execute(**entity_dfs):
                from kindling.spark_config import get_or_create_spark_session

                spark = get_or_create_spark_session()
                for eid, df in zip(_ids, entity_dfs.values()):
                    df.createOrReplaceTempView(eid.replace(".", "_"))
                query = _Path(_sf).read_text() if _sf else _sql
                result = spark.sql(query)
                return post_fn(result) if post_fn is not None else result

            return execute

        def _do_register(execute_fn):
            if cls.dpregistry is None:
                try:
                    _raise_if_not_initialized("DataPipes.view", "view")
                    cls.dpregistry = GlobalInjector.get(DataPipesRegistry)
                except Exception as exc:
                    if isinstance(exc, KindlingNotInitializedError):
                        raise
                    raise KindlingNotInitializedError(
                        "A DataPipes.view() call fired before initialize() was called. "
                        "Call initialize() before importing view modules. "
                        "See your app.py register_all() for the correct order."
                    ) from exc
            cls.dpregistry.register_pipe(
                pipeid,
                name=resolved_name,
                input_entity_ids=input_entity_ids,
                output_entity_id=output_entity_id,
                output_type="memory",
                tags=resolved_tags,
                execute=execute_fn,
            )
            setattr(cls.ids, pipeid.replace(".", "_").replace("-", "_"), pipeid)

        _do_register(_build_execute())

        def decorator(func):
            _do_register(_build_execute(post_fn=func))
            return func

        return decorator

    @classmethod
    def pipe(cls, **decorator_params):
        def decorator(func):
            if cls.dpregistry is None:
                try:
                    _raise_if_not_initialized("DataPipes.pipe", "pipe")
                    cls.dpregistry = GlobalInjector.get(DataPipesRegistry)
                except Exception as exc:
                    if isinstance(exc, KindlingNotInitializedError):
                        raise
                    raise KindlingNotInitializedError(
                        "A @DataPipes.pipe decorator fired before initialize() was called. "
                        "Call initialize() before importing pipe modules. "
                        "See your app.py register_all() for the correct order."
                    ) from exc
            decorator_params["execute"] = func
            required_fields = {
                field.name
                for field in fields(PipeMetadata)
                if field.default is dataclasses.MISSING
                and field.default_factory is dataclasses.MISSING
            }
            missing_fields = required_fields - decorator_params.keys()

            if missing_fields:
                raise ValueError(f"Missing required fields in pipe decorator: {missing_fields}")

            pipeid = decorator_params["pipeid"]
            del decorator_params["pipeid"]
            cls.dpregistry.register_pipe(pipeid, **decorator_params)
            setattr(cls.ids, pipeid.replace(".", "_").replace("-", "_"), pipeid)
            return func

        return decorator


class DataPipesRegistry(ABC):
    @abstractmethod
    def register_pipe(self, pipeid, **decorator_params):
        pass

    @abstractmethod
    def get_pipe_ids(self):
        pass

    @abstractmethod
    def get_pipe_definition(self, name):
        pass


class DataPipesExecution(ABC):
    """Abstract base for data pipe execution.

    Implementations MUST emit these signals:
        - datapipes.before_run: Before run_datapipes() starts
        - datapipes.after_run: After successful completion
        - datapipes.run_failed: When run_datapipes() fails
        - datapipes.before_pipe: Before each individual pipe
        - datapipes.after_pipe: After each pipe completes
        - datapipes.pipe_failed: When a pipe fails
        - datapipes.pipe_skipped: When a pipe is skipped (no data)
    """

    EMITS = [
        "datapipes.before_run",
        "datapipes.after_run",
        "datapipes.run_failed",
        "datapipes.before_pipe",
        "datapipes.after_pipe",
        "datapipes.pipe_failed",
        "datapipes.pipe_skipped",
    ]

    @abstractmethod
    def run_datapipes(self, pipes):
        pass

    @abstractmethod
    def run_datapipes_dag(self, pipes, strategy=None, **kwargs):
        pass


@GlobalInjector.singleton_autobind()
class DataPipesManager(DataPipesRegistry):
    @inject
    def __init__(self, lp: PythonLoggerProvider):
        self.registry = {}
        self.logger = lp.get_logger("data_pipes_manager")
        self.logger.debug("Data pipes manager initialized ...")

    def register_pipe(self, pipeid, **decorator_params):
        self.registry[pipeid] = PipeMetadata(pipeid, **decorator_params)
        self.logger.debug(f"Pipe registered: {pipeid}")

    def get_pipe_ids(self):
        return self.registry.keys()

    def get_pipe_definition(self, name):
        return self.registry.get(name)


@GlobalInjector.singleton_autobind()
class DataPipesExecuter(DataPipesExecution, SignalEmitter):
    """Executes registered data pipes with signal emissions."""

    @inject
    def __init__(
        self,
        lp: PythonLoggerProvider,
        dpe: DataEntityRegistry,
        dpr: DataPipesRegistry,
        erps: EntityReadPersistStrategy,
        tp: SparkTraceProvider,
        signal_provider: SignalProvider = None,
    ):
        self._init_signal_emitter(signal_provider)
        self.erps = erps
        self.dpr = dpr
        self.dpe = dpe
        self.logger = lp.get_logger("data_pipes_executer")
        self.tp = tp

    def run_datapipes(
        self,
        pipes: List[str],
        use_dag: bool = False,
        dag_strategy: Optional[Any] = None,
        no_watermark: bool = False,
        **dag_kwargs,
    ):
        """Execute a list of pipes with signal emissions.

        Args:
            pipes: List of pipe IDs to execute
            use_dag: If True, delegate execution to DAG orchestrator mode
            dag_strategy: Optional DAG execution strategy override
            no_watermark: If True, disable watermark reads/writes for this run
            **dag_kwargs: Additional DAG execution options
        """
        if use_dag:
            return self.run_datapipes_dag(
                pipes, strategy=dag_strategy, no_watermark=no_watermark, **dag_kwargs
            )

        run_id = str(uuid.uuid4())
        start_time = time.time()
        success_count = 0
        failed_pipes = []
        skipped_pipes = []

        # Emit before_run signal
        self.emit("datapipes.before_run", pipe_ids=pipes, pipe_count=len(pipes), run_id=run_id)

        pipe_entity_reader = self.erps.create_pipe_entity_reader
        pipe_activator = self.erps.create_pipe_persist_activator

        try:
            with self.tp.span(component="data_pipes_executer", operation="execute_datapipes"):
                for index, pipeid in enumerate(pipes):
                    pipe = self.dpr.get_pipe_definition(pipeid)
                    if no_watermark and pipe.use_watermark:
                        pipe = dataclasses.replace(pipe, use_watermark=False)
                    pipe_start = time.time()

                    # Emit before_pipe signal
                    self.emit(
                        "datapipes.before_pipe",
                        pipe_id=pipeid,
                        pipe_name=pipe.name,
                        pipe_index=index,
                        run_id=run_id,
                    )

                    try:
                        with self.tp.span(
                            operation="execute_datapipe",
                            component=f"pipe-{pipeid}",
                            details=pipe.tags,
                        ):
                            was_skipped = self._execute_datapipe(
                                pipe_entity_reader(pipe), pipe_activator(pipe), pipe
                            )

                        pipe_duration = time.time() - pipe_start

                        if was_skipped:
                            skipped_pipes.append(pipeid)
                            self.emit(
                                "datapipes.pipe_skipped",
                                pipe_id=pipeid,
                                pipe_name=pipe.name,
                                skip_reason="no_data",
                                run_id=run_id,
                            )
                        else:
                            success_count += 1
                            # Emit after_pipe signal
                            self.emit(
                                "datapipes.after_pipe",
                                pipe_id=pipeid,
                                pipe_name=pipe.name,
                                duration_seconds=pipe_duration,
                                run_id=run_id,
                            )

                    except Exception as e:
                        pipe_duration = time.time() - pipe_start
                        failed_pipes.append(pipeid)

                        # Emit pipe_failed signal
                        self.emit(
                            "datapipes.pipe_failed",
                            pipe_id=pipeid,
                            pipe_name=pipe.name,
                            error=str(e),
                            error_type=type(e).__name__,
                            duration_seconds=pipe_duration,
                            run_id=run_id,
                        )
                        raise

            total_duration = time.time() - start_time

            # Emit after_run signal
            self.emit(
                "datapipes.after_run",
                pipe_ids=pipes,
                success_count=success_count,
                skipped_count=len(skipped_pipes),
                failed_count=len(failed_pipes),
                duration_seconds=total_duration,
                run_id=run_id,
            )

        except Exception as e:
            total_duration = time.time() - start_time

            # Emit run_failed signal
            self.emit(
                "datapipes.run_failed",
                pipe_ids=pipes,
                failed_pipe=failed_pipes[-1] if failed_pipes else None,
                success_count=success_count,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=total_duration,
                run_id=run_id,
            )
            raise

    def run_datapipes_dag(
        self,
        pipes: List[str],
        strategy: Optional[Any] = None,
        parallel: Any = UNSET,  # bool | UNSET
        max_workers: Any = UNSET,  # int | UNSET
        error_strategy: Any = UNSET,  # ErrorStrategy | UNSET
        pipe_timeout: Any = UNSET,  # float | None (no timeout) | UNSET
        streaming_options: Optional[Dict[str, Any]] = None,
        auto_cache: Any = UNSET,  # bool | UNSET
        no_watermark: bool = False,
        retry_attempts: Any = UNSET,  # int | UNSET
        retry_interval_seconds: Any = UNSET,  # float | UNSET
    ):
        """Execute pipes via DAG planning/generation execution facade.

        Options left UNSET resolve from `kindling.execution.*` config;
        passed values override config just-in-time (an explicit
        ``pipe_timeout=None`` disables the timeout even when config sets one).
        """
        from kindling.execution_orchestrator import ExecutionOrchestrator

        orchestrator = GlobalInjector.get(ExecutionOrchestrator)
        return orchestrator.execute(
            pipe_ids=pipes,
            strategy=strategy,
            parallel=parallel,
            max_workers=max_workers,
            error_strategy=error_strategy,
            pipe_timeout=pipe_timeout,
            streaming_options=streaming_options,
            auto_cache=auto_cache,
            no_watermark=no_watermark,
            retry_attempts=retry_attempts,
            retry_interval_seconds=retry_interval_seconds,
        )

    def _execute_datapipe(
        self,
        entity_reader: Callable[[str], DataFrame],
        activator: Callable[[DataFrame], None],
        pipe: PipeMetadata,
    ) -> bool:
        """Execute a single data pipe.

        Args:
            entity_reader: Function to read source entities
            activator: Function to persist the result
            pipe: Pipe metadata

        Returns:
            True if pipe was skipped (no data), False otherwise
        """
        input_entities = self._populate_source_dict(entity_reader, pipe)
        first_source = list(input_entities.values())[0]
        self.logger.debug(f"Prepping data pipe: {pipe.pipeid}")
        if first_source is not None:
            self.logger.debug(f"Executing data pipe: {pipe.pipeid}")
            processedDf = pipe.execute(**input_entities)
            activator(processedDf)
            return False  # Not skipped
        else:
            self.logger.debug(f"Skipping data pipe: {pipe.pipeid}")
            return True  # Skipped

    def _populate_source_dict(
        self, entity_reader: Callable[[str], DataFrame], pipe
    ) -> dict[str, DataFrame]:
        result = {}
        for i, entity_id in enumerate(pipe.input_entity_ids):
            # Driving-source convention: a pipe operates on ONE source of
            # truth — its first input — and every other input is reference
            # data, read in full. Only the driving source is ever read
            # incrementally (watermarked). A multi-source output table is
            # built by multiple pipes, each with its own driving source,
            # never by one pipe with several watermarked inputs. See
            # WatermarkAspect (kindling.watermarking) for the write side.
            is_first = i == 0
            key = entity_id.replace(".", "_")
            result[key] = entity_reader(
                self.dpe.get_entity_definition(entity_id), pipe.use_watermark and is_first
            )
        return result


class StageProcessingService(ABC):
    @abstractmethod
    def execute(
        self,
        stage: str,
        stage_description: str,
        stage_details: Dict,
        layer: str,
        preprocessor: Callable,
    ):
        pass
