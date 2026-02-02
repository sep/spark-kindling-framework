import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, fields
from typing import Any, Callable, Dict, List, Optional

from delta.tables import DeltaTable
from injector import Binder, Injector, inject, singleton
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log_provider import *
from kindling.spark_trace import *
from pyspark.sql import DataFrame

from .data_entities import *


@dataclass
class PipeMetadata:
    pipeid: str
    name: str
    execute: Callable
    tags: Dict[str, str]
    input_entity_ids: List[str]
    output_entity_id: str
    output_type: str


class EntityReadPersistStrategy(ABC):
    """Abstract base for entity read/persist strategies.

    Implementations MUST emit these signals:
        - persist.before_persist: Before persisting pipe output
        - persist.after_persist: After successful persist
        - persist.persist_failed: When persist fails
        - persist.watermark_saved: After watermark is saved
    """

    EMITS = [
        "persist.before_persist",
        "persist.after_persist",
        "persist.persist_failed",
        "persist.watermark_saved",
    ]

    @abstractmethod
    def create_pipe_entity_reader(self, pipe: str):
        pass

    @abstractmethod
    def create_pipe_persist_activator(self, pipe: PipeMetadata):
        pass


class DataPipes:
    dpregistry = None

    @classmethod
    def pipe(cls, **decorator_params):
        def decorator(func):
            if cls.dpregistry is None:
                cls.dpregistry = GlobalInjector.get(DataPipesRegistry)
            decorator_params["execute"] = func
            required_fields = {field.name for field in fields(PipeMetadata)}
            missing_fields = required_fields - decorator_params.keys()

            if missing_fields:
                raise ValueError(f"Missing required fields in pipe decorator: {missing_fields}")

            pipeid = decorator_params["pipeid"]
            del decorator_params["pipeid"]
            cls.dpregistry.register_pipe(pipeid, **decorator_params)
            return func

        return decorator


class DataPipesRegistry(ABC):
    @abstractmethod
    def register_pipe(self, pipeid, **decorator_params):
        passabstractmethod

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

    def run_datapipes(self, pipes: List[str]):
        """Execute a list of pipes with signal emissions.

        Args:
            pipes: List of pipe IDs to execute
        """
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
            is_first = i == 0  # True for the first entity, False for others
            key = entity_id.replace(".", "_")
            result[key] = entity_reader(self.dpe.get_entity_definition(entity_id), is_first)
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
