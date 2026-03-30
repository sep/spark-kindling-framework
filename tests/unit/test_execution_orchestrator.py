"""Unit tests for execution_orchestrator module."""

from unittest.mock import Mock

from kindling.execution_orchestrator import ExecutionOrchestrator
from kindling.generation_executor import ErrorStrategy


def test_execute_generates_plan_emits_signal_and_delegates():
    logger_provider = Mock()
    logger = Mock()
    logger_provider.get_logger.return_value = logger

    signal_provider = Mock()
    signal = Mock()
    signal.send = Mock(return_value=[])
    signal_provider.get_signal = Mock(return_value=signal)
    signal_provider.create_signal = Mock(return_value=signal)

    plan = Mock()
    plan.strategy = "batch"
    plan.metadata = {"cache_candidate_count": 1}
    plan.total_generations.return_value = 2
    plan.max_parallelism.return_value = 3

    plan_generator = Mock()
    plan_generator.generate_plan.return_value = plan

    expected_result = Mock()
    generation_executor = Mock()
    generation_executor.execute.return_value = expected_result

    orchestrator = ExecutionOrchestrator(
        plan_generator=plan_generator,
        generation_executor=generation_executor,
        logger_provider=logger_provider,
        signal_provider=signal_provider,
    )

    result = orchestrator.execute(
        pipe_ids=["pipe_a", "pipe_b"],
        strategy=Mock(),
        parallel=True,
        max_workers=8,
        error_strategy=ErrorStrategy.CONTINUE,
        pipe_timeout=10.0,
        streaming_options={"pipe_a": {"checkpoint": "/tmp/checkpoints"}},
        auto_cache=True,
    )

    assert result is expected_result
    plan_generator.generate_plan.assert_called_once()
    generation_executor.execute.assert_called_once_with(
        plan=plan,
        parallel=True,
        max_workers=8,
        error_strategy=ErrorStrategy.CONTINUE,
        pipe_timeout=10.0,
        streaming_options={"pipe_a": {"checkpoint": "/tmp/checkpoints"}},
        auto_cache=True,
    )

    signal.send.assert_called_once()
    _, payload = signal.send.call_args
    assert payload["strategy"] == "batch"
    assert payload["pipe_count"] == 2
    assert payload["generation_count"] == 2
    assert payload["max_parallelism"] == 3


def test_execute_batch_uses_batch_strategy():
    logger_provider = Mock()
    logger_provider.get_logger.return_value = Mock()
    plan_generator = Mock()
    plan_generator.batch_strategy = Mock()
    generation_executor = Mock()

    orchestrator = ExecutionOrchestrator(
        plan_generator=plan_generator,
        generation_executor=generation_executor,
        logger_provider=logger_provider,
    )
    orchestrator.execute = Mock(return_value="ok")

    result = orchestrator.execute_batch(["p1"], auto_cache=True)

    assert result == "ok"
    orchestrator.execute.assert_called_once_with(
        pipe_ids=["p1"],
        strategy=plan_generator.batch_strategy,
        parallel=False,
        max_workers=4,
        error_strategy=ErrorStrategy.FAIL_FAST,
        pipe_timeout=None,
        auto_cache=True,
    )


def test_execute_streaming_uses_streaming_strategy():
    logger_provider = Mock()
    logger_provider.get_logger.return_value = Mock()
    plan_generator = Mock()
    plan_generator.streaming_strategy = Mock()
    generation_executor = Mock()

    orchestrator = ExecutionOrchestrator(
        plan_generator=plan_generator,
        generation_executor=generation_executor,
        logger_provider=logger_provider,
    )
    orchestrator.execute = Mock(return_value="stream")

    result = orchestrator.execute_streaming(
        ["p1", "p2"],
        error_strategy=ErrorStrategy.CONTINUE,
        streaming_options={"p2": {"checkpoint": "/tmp/cp"}},
    )

    assert result == "stream"
    orchestrator.execute.assert_called_once_with(
        pipe_ids=["p1", "p2"],
        strategy=plan_generator.streaming_strategy,
        parallel=False,
        error_strategy=ErrorStrategy.CONTINUE,
        streaming_options={"p2": {"checkpoint": "/tmp/cp"}},
    )
