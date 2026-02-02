import time
import uuid
from abc import ABC, abstractmethod
from typing import Dict, Optional

from delta.tables import *
from injector import Binder, Injector, inject, singleton
from kindling.data_entities import *
from kindling.data_pipes import *
from kindling.injection import *
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_config import *
from kindling.spark_trace import *
from pyspark.sql.functions import current_timestamp, lit, row_number, when
from pyspark.sql.window import Window

from .simple_read_persist_strategy import *


def execute_process_stage(stage: str, stage_description: str, stage_details: Dict, layer: str):
    # print(f"GlobalInjector ID = {GlobalInjector.get_instance_id()}")
    GlobalInjector.get(StageProcessingService).execute(
        stage, stage_description, stage_details, layer
    )


class StageProcessingService(ABC):
    """Abstract base for stage processing.

    Implementations MUST emit these signals:
        - stage.before_execute: Before stage execution starts
        - stage.after_execute: After stage completes successfully
        - stage.execute_failed: When stage execution fails
    """

    EMITS = [
        "stage.before_execute",
        "stage.after_execute",
        "stage.execute_failed",
    ]

    @abstractmethod
    def execute(self, stage: str, stage_description: str, stage_details: Dict, layer: str):
        pass


@GlobalInjector.singleton_autobind()
class StageProcessor(StageProcessingService, SignalEmitter):
    """Processes pipeline stages with signal emissions."""

    @inject
    def __init__(
        self,
        dpr: DataPipesRegistry,
        ep: EntityProvider,
        dep: DataPipesExecution,
        wef: WatermarkEntityFinder,
        tp: SparkTraceProvider,
        signal_provider: Optional[SignalProvider] = None,
    ):
        self.wef = wef
        self.ep = ep
        self.dpr = dpr
        self.dep = dep
        self.tp = tp
        self._init_signal_emitter(signal_provider)

    def execute(self, stage: str, stage_description: str, stage_details: Dict, layer: str):
        execution_id = str(uuid.uuid4())
        start_time = time.time()

        # Get pipes for this stage
        pipe_ids = self.dpr.get_pipe_ids()
        stage_pipe_ids = [pipe_id for pipe_id in pipe_ids if pipe_id.startswith(stage)]

        self.emit(
            "stage.before_execute",
            stage=stage,
            stage_description=stage_description,
            layer=layer,
            pipe_count=len(stage_pipe_ids),
            pipe_ids=stage_pipe_ids,
            execution_id=execution_id,
        )

        try:
            with self.tp.span(
                component=stage_description,
                operation=stage_description,
                details=stage_details,
                reraise=True,
            ):
                self.ep.ensure_entity_table(self.wef.get_watermark_entity_for_layer(layer))
                self.dep.run_datapipes(stage_pipe_ids)

            duration = time.time() - start_time
            self.emit(
                "stage.after_execute",
                stage=stage,
                stage_description=stage_description,
                layer=layer,
                pipe_count=len(stage_pipe_ids),
                duration_seconds=duration,
                execution_id=execution_id,
            )

        except Exception as e:
            duration = time.time() - start_time
            self.emit(
                "stage.execute_failed",
                stage=stage,
                stage_description=stage_description,
                layer=layer,
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=duration,
                execution_id=execution_id,
            )
            raise
