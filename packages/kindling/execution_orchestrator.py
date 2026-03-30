"""Execution orchestrator facade for DAG-based data pipe execution.

Provides a high-level entrypoint that coordinates plan generation and
generation-based execution while preserving existing executor options.
"""

from typing import Any, List, Optional

from injector import inject

from kindling.execution_strategy import ExecutionPlanGenerator, ExecutionStrategy
from kindling.generation_executor import (
    ErrorStrategy,
    ExecutionResult,
    GenerationExecutor,
)
from kindling.injection import GlobalInjector
from kindling.signaling import SignalEmitter, SignalProvider
from kindling.spark_log_provider import PythonLoggerProvider


@GlobalInjector.singleton_autobind()
class ExecutionOrchestrator(SignalEmitter):
    """Facade for DAG plan generation + execution.

    This class closes the gap between low-level DAG primitives and user-facing
    execution by offering a single execute() call.
    """

    EMITS = [
        "orchestrator.plan_generated",
    ]

    @inject
    def __init__(
        self,
        plan_generator: ExecutionPlanGenerator,
        generation_executor: GenerationExecutor,
        logger_provider: PythonLoggerProvider,
        signal_provider: SignalProvider = None,
    ):
        self.plan_generator = plan_generator
        self.generation_executor = generation_executor
        self.logger = logger_provider.get_logger("execution_orchestrator")
        self._init_signal_emitter(signal_provider)

    def execute(
        self,
        pipe_ids: List[str],
        strategy: Optional[ExecutionStrategy] = None,
        parallel: bool = False,
        max_workers: int = 4,
        error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST,
        pipe_timeout: Optional[float] = None,
        streaming_options: Optional[dict[str, Any]] = None,
        auto_cache: bool = False,
    ) -> ExecutionResult:
        """Generate an execution plan and run it through GenerationExecutor."""
        plan = self.plan_generator.generate_plan(pipe_ids, strategy=strategy)

        self.emit(
            "orchestrator.plan_generated",
            strategy=plan.strategy,
            pipe_ids=list(pipe_ids),
            pipe_count=len(pipe_ids),
            generation_count=plan.total_generations(),
            max_parallelism=plan.max_parallelism(),
            metadata=dict(plan.metadata),
        )

        self.logger.info(
            f"Executing DAG plan strategy={plan.strategy} "
            f"pipes={len(pipe_ids)} generations={plan.total_generations()}"
        )

        return self.generation_executor.execute(
            plan=plan,
            parallel=parallel,
            max_workers=max_workers,
            error_strategy=error_strategy,
            pipe_timeout=pipe_timeout,
            streaming_options=streaming_options,
            auto_cache=auto_cache,
        )

    def execute_batch(
        self,
        pipe_ids: List[str],
        parallel: bool = False,
        max_workers: int = 4,
        error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST,
        pipe_timeout: Optional[float] = None,
        auto_cache: bool = False,
    ) -> ExecutionResult:
        """Execute in batch mode using the batch strategy."""
        strategy = self.plan_generator.batch_strategy
        return self.execute(
            pipe_ids=pipe_ids,
            strategy=strategy,
            parallel=parallel,
            max_workers=max_workers,
            error_strategy=error_strategy,
            pipe_timeout=pipe_timeout,
            auto_cache=auto_cache,
        )

    def execute_streaming(
        self,
        pipe_ids: List[str],
        error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST,
        streaming_options: Optional[dict[str, Any]] = None,
    ) -> ExecutionResult:
        """Execute in streaming mode using the streaming strategy."""
        strategy = self.plan_generator.streaming_strategy
        return self.execute(
            pipe_ids=pipe_ids,
            strategy=strategy,
            parallel=False,
            error_strategy=error_strategy,
            streaming_options=streaming_options,
        )
