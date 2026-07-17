"""Unit tests for generation_executor module.

Tests the GenerationExecutor which runs pipes generation by generation
from an ExecutionPlan, supporting both batch and streaming modes.

See: GitHub Issue #24 - Generation Executor
"""

import time
from concurrent.futures import Future
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from kindling.data_pipes import PipeMetadata
from kindling.entity_provider import (
    StreamableEntityProvider,
    StreamWritableEntityProvider,
)
from kindling.execution_strategy import ExecutionPlan, Generation
from kindling.generation_executor import (
    UNSET,
    ErrorStrategy,
    ExecutionResult,
    GenerationExecutor,
    GenerationResult,
    PipeResult,
    RetryPolicy,
)
from kindling.pipe_graph import PipeEdge, PipeGraph, PipeNode
from kindling.pipe_streaming import SimplePipeStreamStarter

# ---- Fixtures ----


@pytest.fixture
def logger_provider():
    """Create mock logger provider."""
    provider = Mock()
    logger = Mock()
    logger.info = Mock()
    logger.debug = Mock()
    logger.error = Mock()
    provider.get_logger = Mock(return_value=logger)
    return provider


@pytest.fixture
def pipes_registry():
    """Create mock pipes registry."""
    registry = Mock()
    return registry


@pytest.fixture
def entity_registry():
    """Create mock entity registry."""
    registry = Mock()
    return registry


@pytest.fixture
def persist_strategy():
    """Create mock persist strategy."""
    strategy = Mock()
    return strategy


@pytest.fixture
def provider_registry():
    """Create mock entity provider registry."""
    registry = Mock()
    return registry


@pytest.fixture
def entity_path_locator():
    """Create mock entity path locator."""
    locator = Mock()
    locator.get_table_path = Mock(return_value="/data/output")
    return locator


@pytest.fixture
def config_service():
    """Create mock config service.

    `kindling.execution.*` keys fall through to the caller's default so
    option resolution behaves as if unconfigured; other keys return a
    checkpoint path for the streaming code paths.
    """
    service = Mock()

    def get(key, default=None):
        if key.startswith("kindling.execution."):
            return default
        return "/checkpoints"

    service.get = Mock(side_effect=get)
    return service


@pytest.fixture
def trace_provider():
    """Create mock trace provider with context manager span."""
    tp = Mock()
    span_cm = MagicMock()
    span_cm.__enter__ = Mock(return_value=None)
    span_cm.__exit__ = Mock(return_value=False)
    tp.span = Mock(return_value=span_cm)
    return tp


@pytest.fixture
def signal_provider():
    """Create mock signal provider."""
    sp = Mock()
    signal = Mock()
    signal.send = Mock(return_value=[])
    sp.create_signal = Mock(return_value=signal)
    return sp


@pytest.fixture
def executor(
    pipes_registry,
    entity_registry,
    provider_registry,
    entity_path_locator,
    config_service,
    persist_strategy,
    trace_provider,
    logger_provider,
    signal_provider,
):
    """Create GenerationExecutor with mocked dependencies."""
    pipe_stream_starter = SimplePipeStreamStarter(
        cs=config_service,
        dpr=pipes_registry,
        provider_registry=provider_registry,
        der=entity_registry,
        epl=entity_path_locator,
        plp=logger_provider,
    )
    return GenerationExecutor(
        pipes_registry=pipes_registry,
        entity_registry=entity_registry,
        pipe_stream_starter=pipe_stream_starter,
        config_service=config_service,
        persist_strategy=persist_strategy,
        trace_provider=trace_provider,
        logger_provider=logger_provider,
        signal_provider=signal_provider,
    )


def make_pipe(pipe_id, inputs=None, output=None, tags=None, use_watermark=False):
    """Helper to create a PipeMetadata object."""
    return PipeMetadata(
        pipeid=pipe_id,
        name=pipe_id,
        execute=lambda *args, **kwargs: Mock(),  # Returns mock DataFrame
        tags=tags or {},
        input_entity_ids=inputs or [],
        output_entity_id=output or f"entity.{pipe_id}",
        output_type="delta",
        use_watermark=use_watermark,
    )


def make_plan(pipe_ids, generations, strategy="batch", metadata=None):
    """Helper to create an ExecutionPlan."""
    graph = Mock()
    graph.get_dependencies = Mock(return_value=[])
    graph.get_dependents = Mock(return_value=[])
    return ExecutionPlan(
        pipe_ids=pipe_ids,
        generations=generations,
        graph=graph,
        strategy=strategy,
        metadata=metadata or {},
    )


def make_graph_with_shared_input():
    """Create a graph where two pipes read the same input entity."""
    graph = PipeGraph()
    graph.add_node(PipeNode("pipe1", ["entity.shared"], "entity.out1"))
    graph.add_node(PipeNode("pipe2", ["entity.shared"], "entity.out2"))
    return graph


def setup_streaming_mocks(
    entity_registry,
    provider_registry,
    entity_path_locator,
    config_service,
    pipe_ids=None,
    mock_query=None,
):
    """Configure mocks for streaming pipe execution.

    Sets up entity_registry, provider_registry, entity_path_locator, and
    config_service so that ``_execute_pipe_streaming`` can resolve providers
    and write streams successfully.

    Returns the mock_query that will be produced by ``append_as_stream().start()``.
    """
    if mock_query is None:
        mock_query = Mock()
        mock_query.id = "query-123"

    # Create a provider mock that passes isinstance checks for StreamableEntityProvider
    provider = Mock(spec=StreamableEntityProvider)
    stream_df = Mock()
    provider.read_entity_as_stream = Mock(return_value=stream_df)
    provider.ensure_entity_table = Mock()
    writer = Mock()
    writer.start = Mock(return_value=mock_query)
    writer.toTable = Mock(return_value=mock_query)
    provider.append_as_stream = Mock(return_value=writer)

    # Entity registry returns mock entities
    def make_entity(eid):
        entity = Mock()
        entity.entityid = eid
        entity.tags = {"provider_type": "delta"}
        return entity

    entities = {}

    def get_entity(name):
        if name not in entities:
            entities[name] = make_entity(name)
        return entities[name]

    entity_registry.get_entity_definition = Mock(side_effect=get_entity)
    provider_registry.get_provider_for_entity = Mock(return_value=provider)
    entity_path_locator.get_table_path = Mock(return_value="/data/output")
    config_service.get = Mock(
        side_effect=lambda key, default=None: (
            default if key.startswith("kindling.execution.") else "/checkpoints"
        )
    )

    return mock_query, provider


# ---- Data Structure Tests ----


class TestPipeResult:
    """Tests for PipeResult dataclass."""

    def test_success_result(self):
        result = PipeResult(pipe_id="pipe1", status="success", duration_seconds=1.5)
        assert result.pipe_id == "pipe1"
        assert result.status == "success"
        assert result.duration_seconds == 1.5
        assert result.error is None

    def test_failed_result(self):
        result = PipeResult(
            pipe_id="pipe1",
            status="failed",
            error="Something went wrong",
            error_type="ValueError",
        )
        assert result.status == "failed"
        assert result.error == "Something went wrong"
        assert result.error_type == "ValueError"

    def test_skipped_result(self):
        result = PipeResult(pipe_id="pipe1", status="skipped")
        assert result.status == "skipped"

    def test_streaming_result_with_query(self):
        query = Mock()
        result = PipeResult(pipe_id="pipe1", status="success", streaming_query=query)
        assert result.streaming_query is query


class TestGenerationResult:
    """Tests for GenerationResult dataclass."""

    def test_empty_generation(self):
        result = GenerationResult(generation_number=0)
        assert result.success_count == 0
        assert result.failed_count == 0
        assert result.skipped_count == 0
        assert result.all_succeeded is True
        assert result.failed_pipes == []

    def test_all_success(self):
        result = GenerationResult(
            generation_number=0,
            pipe_results=[
                PipeResult(pipe_id="pipe1", status="success"),
                PipeResult(pipe_id="pipe2", status="success"),
            ],
        )
        assert result.success_count == 2
        assert result.failed_count == 0
        assert result.all_succeeded is True

    def test_mixed_results(self):
        result = GenerationResult(
            generation_number=0,
            pipe_results=[
                PipeResult(pipe_id="pipe1", status="success"),
                PipeResult(pipe_id="pipe2", status="failed", error="err"),
                PipeResult(pipe_id="pipe3", status="skipped"),
            ],
        )
        assert result.success_count == 1
        assert result.failed_count == 1
        assert result.skipped_count == 1
        assert result.all_succeeded is False
        assert result.failed_pipes == ["pipe2"]

    def test_skipped_counts_as_succeeded(self):
        result = GenerationResult(
            generation_number=0,
            pipe_results=[
                PipeResult(pipe_id="pipe1", status="skipped"),
            ],
        )
        assert result.all_succeeded is True


class TestExecutionResult:
    """Tests for ExecutionResult dataclass."""

    def test_empty_result(self):
        plan = make_plan([], [])
        result = ExecutionResult(plan=plan)
        assert result.success_count == 0
        assert result.all_succeeded is True
        assert result.failed_pipes == []

    def test_aggregates_across_generations(self):
        plan = make_plan(["p1", "p2", "p3"], [])
        result = ExecutionResult(
            plan=plan,
            generation_results=[
                GenerationResult(
                    generation_number=0,
                    pipe_results=[PipeResult(pipe_id="p1", status="success")],
                ),
                GenerationResult(
                    generation_number=1,
                    pipe_results=[
                        PipeResult(pipe_id="p2", status="success"),
                        PipeResult(pipe_id="p3", status="failed", error="err"),
                    ],
                ),
            ],
        )
        assert result.success_count == 2
        assert result.failed_count == 1
        assert result.all_succeeded is False
        assert result.failed_pipes == ["p3"]

    def test_get_summary(self):
        plan = make_plan(["p1"], [], strategy="batch")
        result = ExecutionResult(
            plan=plan,
            generation_results=[
                GenerationResult(
                    generation_number=0,
                    pipe_results=[PipeResult(pipe_id="p1", status="success")],
                ),
            ],
            duration_seconds=1.23,
        )
        summary = result.get_summary()
        assert summary["strategy"] == "batch"
        assert summary["success_count"] == 1
        assert summary["all_succeeded"] is True
        assert summary["duration_seconds"] == 1.23


# ---- Batch Execution Tests ----


class TestBatchExecution:
    """Tests for batch execution mode."""

    def test_single_pipe_batch(self, executor, pipes_registry, persist_strategy, entity_registry):
        """Execute single pipe in batch mode."""
        pipe = make_pipe("pipe1", inputs=["entity.src"], output="entity.dst")
        pipes_registry.get_pipe_definition.return_value = pipe
        entity_registry.get_entity_definition.return_value = Mock(entityid="entity.src")

        # Set up persist strategy - reader returns a mock DataFrame
        mock_df = Mock()
        reader = Mock(return_value=mock_df)
        persist_strategy.create_pipe_entity_reader.return_value = reader
        activator = Mock()
        persist_strategy.create_pipe_persist_activator.return_value = activator

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
            strategy="batch",
        )

        result = executor.execute(plan)

        assert result.success_count == 1
        assert result.all_succeeded is True
        assert len(result.generation_results) == 1
        persist_strategy.create_pipe_entity_reader.assert_called_once_with(pipe)
        persist_strategy.create_pipe_persist_activator.assert_called_once_with(pipe)

    def test_no_watermark_uses_per_run_pipe_override(
        self, executor, pipes_registry, persist_strategy, entity_registry
    ):
        """DAG batch execution honors no_watermark without mutating registry metadata."""
        pipe = make_pipe(
            "pipe1",
            inputs=["entity.src"],
            output="entity.dst",
            use_watermark=True,
        )
        pipes_registry.get_pipe_definition.return_value = pipe
        entity_registry.get_entity_definition.return_value = Mock(entityid="entity.src")

        mock_df = Mock()
        reader = Mock(return_value=mock_df)
        persist_strategy.create_pipe_entity_reader.return_value = reader
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
            strategy="batch",
        )

        result = executor.execute(plan, no_watermark=True)

        assert result.success_count == 1
        effective_pipe = persist_strategy.create_pipe_entity_reader.call_args.args[0]
        assert effective_pipe.use_watermark is False
        assert effective_pipe is not pipe
        assert pipe.use_watermark is True
        reader.assert_called_once()
        assert reader.call_args.args[1] is False

    def test_multi_generation_batch(
        self, executor, pipes_registry, persist_strategy, entity_registry
    ):
        """Execute multiple generations sequentially."""
        pipe1 = make_pipe("pipe1", inputs=["entity.src"], output="entity.mid")
        pipe2 = make_pipe("pipe2", inputs=["entity.mid"], output="entity.dst")
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]

        entity_registry.get_entity_definition.return_value = Mock()
        mock_df = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=mock_df)
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1", "pipe2"],
            [
                Generation(number=0, pipe_ids=["pipe1"], dependencies=[]),
                Generation(number=1, pipe_ids=["pipe2"], dependencies=["pipe1"]),
            ],
            strategy="batch",
        )

        result = executor.execute(plan)

        assert result.success_count == 2
        assert result.all_succeeded is True
        assert len(result.generation_results) == 2

    def test_batch_shared_input_cache_hit_miss_metrics(
        self, executor, pipes_registry, persist_strategy, entity_registry, signal_provider
    ):
        """Shared input should emit recommendation and track hit/miss metrics."""
        pipe1 = make_pipe("pipe1", inputs=["entity.shared"], output="entity.out1")
        pipe2 = make_pipe("pipe2", inputs=["entity.shared"], output="entity.out2")
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]

        entity_registry.get_entity_definition.side_effect = lambda eid: Mock(entityid=eid)
        mock_df = Mock()
        read_calls = []

        def reader(entity, usewm):
            read_calls.append(entity.entityid)
            return mock_df

        persist_strategy.create_pipe_entity_reader.return_value = reader
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = ExecutionPlan(
            pipe_ids=["pipe1", "pipe2"],
            generations=[Generation(number=0, pipe_ids=["pipe1", "pipe2"], dependencies=[])],
            graph=make_graph_with_shared_input(),
            strategy="batch",
            metadata={"cache_recommendations": {"entity.shared": "MEMORY_AND_DISK"}},
        )

        result = executor.execute(plan)

        assert result.success_count == 2
        assert read_calls == ["entity.shared"]
        assert result.cache_metrics["enabled"] is True
        assert result.cache_metrics["recommended_count"] == 1
        assert result.cache_metrics["miss_count"] == 1
        assert result.cache_metrics["hit_count"] == 1
        assert result.cache_metrics["hits_by_entity"]["entity.shared"] == 1
        assert result.cache_metrics["misses_by_entity"]["entity.shared"] == 1
        signal_provider.get_signal.assert_any_call("orchestrator.cache_recommended")
        signal_provider.get_signal.assert_any_call("orchestrator.cache_hit")
        signal_provider.get_signal.assert_any_call("orchestrator.cache_miss")

    def test_batch_auto_cache_persists_and_unpersists_shared_frame(
        self, executor, pipes_registry, persist_strategy, entity_registry
    ):
        """auto_cache should persist on first miss and unpersist after execution."""
        pipe1 = make_pipe("pipe1", inputs=["entity.shared"], output="entity.out1")
        pipe2 = make_pipe("pipe2", inputs=["entity.shared"], output="entity.out2")
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]

        entity_registry.get_entity_definition.side_effect = lambda eid: Mock(entityid=eid)
        cached_df = Mock()

        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=cached_df)
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = ExecutionPlan(
            pipe_ids=["pipe1", "pipe2"],
            generations=[Generation(number=0, pipe_ids=["pipe1", "pipe2"], dependencies=[])],
            graph=make_graph_with_shared_input(),
            strategy="batch",
            metadata={"cache_recommendations": {"entity.shared": "MEMORY_AND_DISK"}},
        )

        result = executor.execute_batch(plan, auto_cache=True)

        assert result.success_count == 2
        assert result.cache_metrics["auto_cache"] is True
        cached_df.persist.assert_called_once()
        cached_df.unpersist.assert_called_once_with(blocking=False)

    def test_batch_pipe_skipped_no_data(
        self, executor, pipes_registry, persist_strategy, entity_registry
    ):
        """Pipe is skipped when first input entity returns None."""
        pipe = make_pipe("pipe1", inputs=["entity.src"], output="entity.dst")
        pipes_registry.get_pipe_definition.return_value = pipe
        entity_registry.get_entity_definition.return_value = Mock(entityid="entity.src")

        # Reader returns None (no data)
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=None)
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
        )

        result = executor.execute(plan)

        assert result.skipped_count == 1
        assert result.success_count == 0
        assert result.all_succeeded is True  # skipped counts as succeeded

    def test_batch_fail_fast(self, executor, pipes_registry, persist_strategy, entity_registry):
        """Fail fast stops on first error within a generation."""
        pipe1 = make_pipe("pipe1", inputs=["entity.src"])
        pipe2 = make_pipe("pipe2", inputs=["entity.src"])
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]

        entity_registry.get_entity_definition.return_value = Mock()

        # pipe1 reader throws
        def failing_reader(entity, usewm):
            raise RuntimeError("pipe1 failed")

        persist_strategy.create_pipe_entity_reader.side_effect = lambda pipe: (
            failing_reader if pipe.pipeid == "pipe1" else Mock(return_value=Mock())
        )
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1", "pipe2"],
            [Generation(number=0, pipe_ids=["pipe1", "pipe2"], dependencies=[])],
        )

        result = executor.execute(plan, error_strategy=ErrorStrategy.FAIL_FAST)

        assert result.failed_count == 1
        assert "pipe1" in result.failed_pipes
        # pipe2 should not have been attempted (fail fast within generation)
        assert len(result.generation_results[0].pipe_results) == 1

    def test_batch_continue_on_error(
        self, executor, pipes_registry, persist_strategy, entity_registry
    ):
        """Continue mode runs all pipes even if some fail."""
        pipe1 = make_pipe("pipe1", inputs=["entity.src"])
        pipe2 = make_pipe("pipe2", inputs=["entity.src"])
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]

        entity_registry.get_entity_definition.return_value = Mock()
        mock_df = Mock()

        def reader_for_pipe(pipe):
            if pipe.pipeid == "pipe1":

                def failing(entity, usewm):
                    raise RuntimeError("pipe1 failed")

                return failing
            return Mock(return_value=mock_df)

        persist_strategy.create_pipe_entity_reader.side_effect = reader_for_pipe
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1", "pipe2"],
            [Generation(number=0, pipe_ids=["pipe1", "pipe2"], dependencies=[])],
        )

        result = executor.execute(plan, error_strategy=ErrorStrategy.CONTINUE)

        assert result.failed_count == 1
        assert result.success_count == 1
        assert len(result.generation_results[0].pipe_results) == 2

    def test_fail_fast_stops_across_generations(
        self, executor, pipes_registry, persist_strategy, entity_registry
    ):
        """Fail fast stops executing subsequent generations."""
        pipe1 = make_pipe("pipe1", inputs=["entity.src"])
        pipe2 = make_pipe("pipe2", inputs=["entity.mid"])
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]

        entity_registry.get_entity_definition.return_value = Mock()

        def failing_reader(entity, usewm):
            raise RuntimeError("pipe1 failed")

        persist_strategy.create_pipe_entity_reader.return_value = failing_reader
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1", "pipe2"],
            [
                Generation(number=0, pipe_ids=["pipe1"], dependencies=[]),
                Generation(number=1, pipe_ids=["pipe2"], dependencies=["pipe1"]),
            ],
        )

        result = executor.execute(plan, error_strategy=ErrorStrategy.FAIL_FAST)

        # Only generation 0 should have been attempted
        assert len(result.generation_results) == 1
        assert result.failed_count == 1

    def test_pipe_not_found_raises(self, executor, pipes_registry):
        """ValueError when pipe not found in registry."""
        pipes_registry.get_pipe_definition.return_value = None

        plan = make_plan(
            ["nonexistent"],
            [Generation(number=0, pipe_ids=["nonexistent"], dependencies=[])],
        )

        result = executor.execute(plan)
        assert result.failed_count == 1
        assert "nonexistent" in result.failed_pipes


# ---- Streaming Execution Tests ----


class TestStreamingExecution:
    """Tests for streaming execution mode."""

    def test_streaming_plan_detected(self, executor):
        """Streaming strategy is detected from plan."""
        plan = make_plan(["p1"], [], strategy="streaming")
        assert executor._is_streaming_plan(plan) is True

    def test_batch_plan_detected(self, executor):
        """Batch strategy is detected from plan."""
        plan = make_plan(["p1"], [], strategy="batch")
        assert executor._is_streaming_plan(plan) is False

    def test_config_based_streaming_detected(self, executor):
        """Config-based plan with streaming mode is detected."""
        plan = make_plan(
            ["p1"], [], strategy="config_based", metadata={"detected_mode": "streaming"}
        )
        assert executor._is_streaming_plan(plan) is True

    def test_config_based_batch_detected(self, executor):
        """Config-based plan with batch mode defaults to batch."""
        plan = make_plan(["p1"], [], strategy="config_based", metadata={"detected_mode": "batch"})
        assert executor._is_streaming_plan(plan) is False

    def test_single_pipe_streaming(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """Start a single pipe as streaming processor via entity providers."""
        pipe = make_pipe("pipe1", inputs=["entity.src"], output="entity.dst")
        pipes_registry.get_pipe_definition.return_value = pipe

        mock_query, provider = setup_streaming_mocks(
            entity_registry,
            provider_registry,
            entity_path_locator,
            config_service,
        )

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
            strategy="streaming",
        )

        result = executor.execute(plan)

        assert result.success_count == 1
        assert result.all_succeeded is True
        assert "pipe1" in result.streaming_queries
        assert result.streaming_queries["pipe1"] is mock_query
        provider.read_entity_as_stream.assert_called_once()
        provider.ensure_entity_table.assert_called_once()
        provider.append_as_stream.assert_called_once()

    def test_streaming_with_per_pipe_options(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """Per-pipe checkpoint paths are resolved from streaming_options."""
        pipe1 = make_pipe("pipe1", inputs=["entity.src"])
        pipe2 = make_pipe("pipe2", inputs=["entity.mid"])
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]

        mock_query, provider = setup_streaming_mocks(
            entity_registry,
            provider_registry,
            entity_path_locator,
            config_service,
        )

        options = {
            "pipe1": {"base_checkpoint_path": "/chk/pipe1"},
            "pipe2": {"base_checkpoint_path": "/chk/pipe2"},
        }

        plan = make_plan(
            ["pipe1", "pipe2"],
            [
                Generation(number=0, pipe_ids=["pipe2"], dependencies=[]),
                Generation(number=1, pipe_ids=["pipe1"], dependencies=["pipe2"]),
            ],
            strategy="streaming",
        )

        result = executor.execute(plan, streaming_options=options)

        assert result.success_count == 2
        # Verify checkpoint paths include pipe id suffix
        calls = provider.append_as_stream.call_args_list
        checkpoint_paths = [c[0][2] for c in calls]
        assert "/chk/pipe2/pipe2" in checkpoint_paths
        assert "/chk/pipe1/pipe1" in checkpoint_paths

    def test_streaming_multi_input_first_stream_rest_batch(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
    ):
        """First input is streaming, additional inputs are direct reads."""
        stream_df = Mock(name="stream_df")
        lookup_df = Mock(name="lookup_df")
        transformed_df = Mock(name="transformed_df")
        execute_mock = Mock(return_value=transformed_df)

        pipe = PipeMetadata(
            pipeid="join_pipe",
            name="join_pipe",
            execute=execute_mock,
            tags={},
            input_entity_ids=["entity.stream", "entity.lookup"],
            output_entity_id="entity.out",
            output_type="delta",
        )
        pipes_registry.get_pipe_definition.return_value = pipe

        stream_entity = Mock(entityid="entity.stream", tags={"provider_type": "delta"})
        lookup_entity = Mock(entityid="entity.lookup", tags={"provider_type": "delta"})
        out_entity = Mock(
            entityid="entity.out", tags={"provider_type": "delta", "provider.path": "/out"}
        )
        entity_registry.get_entity_definition.side_effect = lambda eid: {
            "entity.stream": stream_entity,
            "entity.lookup": lookup_entity,
            "entity.out": out_entity,
        }[eid]

        stream_provider = Mock(spec=StreamableEntityProvider)
        stream_provider.read_entity_as_stream.return_value = stream_df

        lookup_provider = Mock()
        lookup_provider.read_entity.return_value = lookup_df

        output_provider = Mock(spec=StreamWritableEntityProvider)
        output_provider.ensure_entity_table = Mock()
        writer = Mock()
        query = Mock(id="query-123")
        writer.start.return_value = query
        output_provider.append_as_stream.return_value = writer

        provider_registry.get_provider_for_entity.side_effect = lambda entity: {
            "entity.stream": stream_provider,
            "entity.lookup": lookup_provider,
            "entity.out": output_provider,
        }[entity.entityid]

        plan = make_plan(
            ["join_pipe"],
            [Generation(number=0, pipe_ids=["join_pipe"], dependencies=[])],
            strategy="streaming",
        )

        result = executor.execute(plan)

        assert result.success_count == 1
        stream_provider.read_entity_as_stream.assert_called_once_with(stream_entity)
        lookup_provider.read_entity.assert_called_once_with(lookup_entity)
        execute_mock.assert_called_once_with(
            entity_stream=stream_df,
            entity_lookup=lookup_df,
        )
        output_provider.append_as_stream.assert_called_once_with(
            transformed_df,
            out_entity,
            "/checkpoints/join_pipe",
        )
        output_provider.ensure_entity_table.assert_called_once_with(out_entity)

    def test_streaming_with_global_options(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """Global options (flat dict) apply to all pipes."""
        pipe = make_pipe("pipe1", inputs=["entity.src"])
        pipes_registry.get_pipe_definition.return_value = pipe

        mock_query, provider = setup_streaming_mocks(
            entity_registry,
            provider_registry,
            entity_path_locator,
            config_service,
        )

        global_options = {
            "base_checkpoint_path": "/custom/checkpoints",
        }

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
            strategy="streaming",
        )

        result = executor.execute(plan, streaming_options=global_options)

        # Global base_checkpoint_path should be used
        call_args = provider.append_as_stream.call_args[0]
        assert call_args[2] == "/custom/checkpoints/pipe1"

    def test_streaming_for_name_uses_table_target(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """catalog mode should write stream via table target (toTable), not path."""
        transformed_df = Mock(name="transformed_df")
        pipe = PipeMetadata(
            pipeid="pipe1",
            name="pipe1",
            execute=Mock(return_value=transformed_df),
            tags={},
            input_entity_ids=["entity.src"],
            output_entity_id="entity.dst",
            output_type="delta",
        )
        pipes_registry.get_pipe_definition.return_value = pipe

        src_entity = Mock(entityid="entity.src", tags={"provider_type": "delta"})
        dst_entity = Mock(
            entityid="entity.dst",
            tags={
                "provider_type": "delta",
                "provider.access_mode": "catalog",
                "provider.table_name": "main.analytics.entity_dst",
            },
        )
        entity_registry.get_entity_definition.side_effect = lambda eid: {
            "entity.src": src_entity,
            "entity.dst": dst_entity,
        }[eid]

        stream_provider = Mock(spec=StreamableEntityProvider)
        stream_provider.read_entity_as_stream.return_value = Mock(name="stream_df")

        output_provider = Mock(spec=StreamWritableEntityProvider)
        output_provider.ensure_entity_table = Mock()
        writer = Mock()
        writer.toTable.return_value = Mock(id="query-1")
        writer.start.return_value = Mock(id="query-start")
        output_provider.append_as_stream.return_value = writer

        provider_registry.get_provider_for_entity.side_effect = lambda entity: {
            "entity.src": stream_provider,
            "entity.dst": output_provider,
        }[entity.entityid]
        config_service.get.side_effect = lambda key: (
            "/checkpoints" if key == "kindling.storage.checkpoint_root" else None
        )

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
            strategy="streaming",
        )

        result = executor.execute(plan)

        assert result.success_count == 1
        writer.toTable.assert_called_once_with("main.analytics.entity_dst")
        writer.start.assert_not_called()
        entity_path_locator.get_table_path.assert_not_called()

    def test_streaming_failure_captured(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """Streaming pipe failure is captured in result."""
        pipe = make_pipe("pipe1", inputs=["entity.src"])
        pipes_registry.get_pipe_definition.return_value = pipe

        setup_streaming_mocks(
            entity_registry,
            provider_registry,
            entity_path_locator,
            config_service,
        )
        # Make read_entity_as_stream raise
        provider = provider_registry.get_provider_for_entity.return_value
        provider.read_entity_as_stream.side_effect = RuntimeError("Stream failed")

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
            strategy="streaming",
        )

        result = executor.execute(plan)
        assert result.failed_count == 1
        assert result.failed_pipes == ["pipe1"]

    def test_streaming_no_ensure_method_still_starts(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
    ):
        """Streaming sink starts even if provider does not expose ensure_entity_table."""
        transformed_df = Mock(name="transformed_df")
        pipe = PipeMetadata(
            pipeid="pipe1",
            name="pipe1",
            execute=Mock(return_value=transformed_df),
            tags={},
            input_entity_ids=["entity.src"],
            output_entity_id="entity.dst",
            output_type="delta",
        )
        pipes_registry.get_pipe_definition.return_value = pipe

        src_entity = Mock(entityid="entity.src", tags={"provider_type": "delta"})
        dst_entity = Mock(
            entityid="entity.dst", tags={"provider_type": "delta", "provider.path": "/out"}
        )
        entity_registry.get_entity_definition.side_effect = lambda eid: {
            "entity.src": src_entity,
            "entity.dst": dst_entity,
        }[eid]

        src_provider = Mock(spec=StreamableEntityProvider)
        src_provider.read_entity_as_stream.return_value = Mock(name="stream_df")

        output_provider = Mock(spec=StreamWritableEntityProvider)
        writer = Mock()
        writer.start.return_value = Mock(id="q-1")
        output_provider.append_as_stream.return_value = writer
        # intentionally no ensure_entity_table attribute

        provider_registry.get_provider_for_entity.side_effect = lambda entity: {
            "entity.src": src_provider,
            "entity.dst": output_provider,
        }[entity.entityid]

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
            strategy="streaming",
        )

        result = executor.execute(plan)

        assert result.success_count == 1
        output_provider.append_as_stream.assert_called_once()

    def test_streaming_multi_generation_ordering(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """Streaming executes sinks first (generation 0), then sources."""
        sink = make_pipe("sink", inputs=["entity.mid"])
        source = make_pipe("source", inputs=["entity.x"], output="entity.mid")
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "sink": sink,
            "source": source,
        }[pid]

        call_order = []
        mock_query, provider = setup_streaming_mocks(
            entity_registry,
            provider_registry,
            entity_path_locator,
            config_service,
        )

        original_read = provider.read_entity_as_stream

        def track_read(entity):
            # Determine which pipe is executing based on input entity
            call_order.append(entity.entityid)
            return original_read(entity)

        provider.read_entity_as_stream = Mock(side_effect=track_read)

        plan = make_plan(
            ["sink", "source"],
            [
                Generation(number=0, pipe_ids=["sink"], dependencies=[]),
                Generation(number=1, pipe_ids=["source"], dependencies=["sink"]),
            ],
            strategy="streaming",
        )

        executor.execute(plan)

        # sink reads entity.mid first, source reads entity.x second
        assert call_order == ["entity.mid", "entity.x"]


# ---- Parallel Execution Tests ----


class TestParallelExecution:
    """Tests for parallel batch execution."""

    def test_parallel_runs_multiple_pipes(
        self, executor, pipes_registry, persist_strategy, entity_registry
    ):
        """Parallel mode runs multiple pipes in a generation concurrently."""
        pipe1 = make_pipe("pipe1", inputs=["entity.a"])
        pipe2 = make_pipe("pipe2", inputs=["entity.b"])
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]

        entity_registry.get_entity_definition.return_value = Mock()
        mock_df = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=mock_df)
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1", "pipe2"],
            [Generation(number=0, pipe_ids=["pipe1", "pipe2"], dependencies=[])],
        )

        result = executor.execute(plan, parallel=True, max_workers=2)

        assert result.success_count == 2
        assert result.all_succeeded is True

    def test_parallel_not_used_for_streaming(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """Parallel flag is ignored for streaming plans (order matters)."""
        pipe1 = make_pipe("pipe1", inputs=["entity.a"])
        pipe2 = make_pipe("pipe2", inputs=["entity.b"])
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]

        call_order = []
        mock_query, provider = setup_streaming_mocks(
            entity_registry,
            provider_registry,
            entity_path_locator,
            config_service,
        )

        original_read = provider.read_entity_as_stream

        def track_read(entity):
            call_order.append(entity.entityid)
            return original_read(entity)

        provider.read_entity_as_stream = Mock(side_effect=track_read)

        plan = make_plan(
            ["pipe1", "pipe2"],
            [Generation(number=0, pipe_ids=["pipe1", "pipe2"], dependencies=[])],
            strategy="streaming",
        )

        result = executor.execute(plan, parallel=True)

        # Should run sequentially despite parallel=True
        assert call_order == ["entity.a", "entity.b"]

    def test_parallel_not_used_for_single_pipe(
        self, executor, pipes_registry, persist_strategy, entity_registry
    ):
        """Parallel mode falls back to sequential for single-pipe generations."""
        pipe = make_pipe("pipe1", inputs=["entity.src"])
        pipes_registry.get_pipe_definition.return_value = pipe
        entity_registry.get_entity_definition.return_value = Mock()
        mock_df = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=mock_df)
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
        )

        result = executor.execute(plan, parallel=True)

        assert result.success_count == 1


# ---- Signal Tests ----


class TestSignals:
    """Tests for signal emissions."""

    def test_execution_started_signal(self, executor, pipes_registry, signal_provider):
        """execution_started signal is emitted."""
        pipes_registry.get_pipe_definition.return_value = None

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
        )

        executor.execute(plan)

        # Verify signal was looked up (emit calls get_signal then send)
        signal_provider.get_signal.assert_any_call("orchestrator.execution_started")

    def test_execution_completed_signal(
        self, executor, pipes_registry, persist_strategy, entity_registry, signal_provider
    ):
        """execution_completed signal is emitted on success."""
        pipe = make_pipe("pipe1", inputs=["entity.src"])
        pipes_registry.get_pipe_definition.return_value = pipe
        entity_registry.get_entity_definition.return_value = Mock(entityid="entity.src")
        mock_df = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=mock_df)
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
        )

        executor.execute(plan)

        signal_provider.get_signal.assert_any_call("orchestrator.execution_completed")


# ---- Convenience Method Tests ----


class TestConvenienceMethods:
    """Tests for execute_batch and execute_streaming helpers."""

    def test_execute_batch(self, executor, pipes_registry, persist_strategy, entity_registry):
        """execute_batch delegates to execute with batch defaults."""
        pipe = make_pipe("pipe1", inputs=["entity.src"])
        pipes_registry.get_pipe_definition.return_value = pipe
        entity_registry.get_entity_definition.return_value = Mock()
        mock_df = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=mock_df)
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
        )

        result = executor.execute_batch(plan)
        assert result.all_succeeded is True

    def test_execute_streaming(
        self,
        executor,
        pipes_registry,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """execute_streaming delegates to execute with streaming defaults."""
        pipe = make_pipe("pipe1", inputs=["entity.src"])
        pipes_registry.get_pipe_definition.return_value = pipe

        mock_query, _ = setup_streaming_mocks(
            entity_registry,
            provider_registry,
            entity_path_locator,
            config_service,
        )

        plan = make_plan(
            ["pipe1"],
            [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])],
            strategy="streaming",
        )

        result = executor.execute_streaming(plan)

        assert result.all_succeeded is True
        assert "pipe1" in result.streaming_queries


# ---- Config-Driven Execution Option Tests ----


def make_config(values):
    """Mock config service backed by a dict of kindling.execution.* values."""
    service = Mock()
    service.get = Mock(side_effect=lambda key, default=None: values.get(key, default))
    return service


class TestConfigDrivenOptions:
    """Execution options resolve config-first; params override just-in-time."""

    def _make_executor(self, config_service, **fixtures):
        return GenerationExecutor(
            pipes_registry=fixtures["pipes_registry"],
            entity_registry=fixtures["entity_registry"],
            pipe_stream_starter=Mock(),
            config_service=config_service,
            persist_strategy=fixtures["persist_strategy"],
            trace_provider=fixtures["trace_provider"],
            logger_provider=fixtures["logger_provider"],
            signal_provider=fixtures["signal_provider"],
        )

    def _two_pipe_setup(self, pipes_registry, entity_registry, persist_strategy):
        pipe1 = make_pipe("pipe1", inputs=["entity.a"])
        pipe2 = make_pipe("pipe2", inputs=["entity.b"])
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "pipe1": pipe1,
            "pipe2": pipe2,
        }[pid]
        entity_registry.get_entity_definition.return_value = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=Mock())
        persist_strategy.create_pipe_persist_activator.return_value = Mock()
        return make_plan(
            ["pipe1", "pipe2"],
            [Generation(number=0, pipe_ids=["pipe1", "pipe2"], dependencies=[])],
        )

    def test_config_enables_parallel(
        self,
        pipes_registry,
        entity_registry,
        persist_strategy,
        trace_provider,
        logger_provider,
        signal_provider,
    ):
        """kindling.execution.parallel=true routes to the parallel path."""
        config = make_config(
            {"kindling.execution.parallel": True, "kindling.execution.max_workers": 2}
        )
        executor = self._make_executor(
            config,
            pipes_registry=pipes_registry,
            entity_registry=entity_registry,
            persist_strategy=persist_strategy,
            trace_provider=trace_provider,
            logger_provider=logger_provider,
            signal_provider=signal_provider,
        )
        plan = self._two_pipe_setup(pipes_registry, entity_registry, persist_strategy)

        with patch.object(
            executor,
            "_execute_generation_parallel",
            wraps=executor._execute_generation_parallel,
        ) as spy:
            result = executor.execute(plan)

        spy.assert_called_once()
        assert spy.call_args.args[2] == 2  # max_workers from config
        assert result.success_count == 2

    def test_param_overrides_config_parallel(
        self,
        pipes_registry,
        entity_registry,
        persist_strategy,
        trace_provider,
        logger_provider,
        signal_provider,
    ):
        """Explicit parallel=False wins over config parallel=true."""
        config = make_config({"kindling.execution.parallel": True})
        executor = self._make_executor(
            config,
            pipes_registry=pipes_registry,
            entity_registry=entity_registry,
            persist_strategy=persist_strategy,
            trace_provider=trace_provider,
            logger_provider=logger_provider,
            signal_provider=signal_provider,
        )
        plan = self._two_pipe_setup(pipes_registry, entity_registry, persist_strategy)

        with patch.object(
            executor,
            "_execute_generation_parallel",
            wraps=executor._execute_generation_parallel,
        ) as spy:
            result = executor.execute(plan, parallel=False)

        spy.assert_not_called()
        assert result.success_count == 2

    def test_config_string_values_coerced(self, executor):
        """YAML/env config may deliver strings; they coerce to typed values."""
        executor.config_service = make_config(
            {
                "kindling.execution.parallel": "true",
                "kindling.execution.max_workers": "8",
                "kindling.execution.error_strategy": "continue",
                "kindling.execution.pipe_timeout": "90.5",
            }
        )
        assert executor._resolve_bool_option(UNSET, "parallel") is True
        assert executor._resolve_int_option(UNSET, "max_workers") == 8
        assert executor._resolve_error_strategy_option(UNSET) == ErrorStrategy.CONTINUE
        assert executor._resolve_float_option(UNSET, "pipe_timeout") == 90.5

    def test_invalid_config_falls_back_with_warning(self, executor):
        """Unparseable config values warn and use built-in defaults."""
        executor.config_service = make_config(
            {
                "kindling.execution.parallel": "bananas",
                "kindling.execution.max_workers": "many",
                "kindling.execution.error_strategy": "explode",
            }
        )
        assert executor._resolve_bool_option(UNSET, "parallel") is False
        assert executor._resolve_int_option(UNSET, "max_workers") == 4
        assert executor._resolve_error_strategy_option(UNSET) == ErrorStrategy.FAIL_FAST
        assert executor.logger.warning.call_count == 3

    def test_unconfigured_defaults_unchanged(self, executor):
        """With nothing configured, resolution yields the historical defaults."""
        assert executor._resolve_bool_option(UNSET, "parallel") is False
        assert executor._resolve_int_option(UNSET, "max_workers") == 4
        assert executor._resolve_error_strategy_option(UNSET) == ErrorStrategy.FAIL_FAST
        assert executor._resolve_float_option(UNSET, "pipe_timeout") is None
        assert executor._resolve_bool_option(UNSET, "auto_cache") is False

    def test_config_error_strategy_continue_applies(
        self,
        pipes_registry,
        entity_registry,
        persist_strategy,
        trace_provider,
        logger_provider,
        signal_provider,
    ):
        """error_strategy=continue from config lets later pipes run after a failure."""
        config = make_config({"kindling.execution.error_strategy": "continue"})
        executor = self._make_executor(
            config,
            pipes_registry=pipes_registry,
            entity_registry=entity_registry,
            persist_strategy=persist_strategy,
            trace_provider=trace_provider,
            logger_provider=logger_provider,
            signal_provider=signal_provider,
        )

        failing = make_pipe("failing", inputs=["entity.a"])
        failing.execute = Mock(side_effect=RuntimeError("boom"))
        ok = make_pipe("ok", inputs=["entity.b"])
        pipes_registry.get_pipe_definition.side_effect = lambda pid: {
            "failing": failing,
            "ok": ok,
        }[pid]
        entity_registry.get_entity_definition.return_value = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=Mock())
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        plan = make_plan(
            ["failing", "ok"],
            [Generation(number=0, pipe_ids=["failing", "ok"], dependencies=[])],
        )

        result = executor.execute(plan)

        assert result.failed_count == 1
        assert result.success_count == 1

    def test_range_validation_falls_back(self, executor):
        """Out-of-range config values (workers < 1, negative timeout) use defaults."""
        executor.config_service = make_config(
            {
                "kindling.execution.max_workers": 0,
                "kindling.execution.pipe_timeout": -5,
            }
        )
        assert executor._resolve_int_option(UNSET, "max_workers", minimum=1) == 4
        assert executor._resolve_float_option(UNSET, "pipe_timeout", minimum=0.0) is None
        assert executor.logger.warning.call_count == 2

    def test_config_lookup_failure_warns_and_defaults(self, executor):
        """A raising ConfigService is logged, not silently swallowed."""
        executor.config_service = Mock()
        executor.config_service.get = Mock(side_effect=RuntimeError("config down"))
        assert executor._resolve_bool_option(UNSET, "parallel") is False
        assert executor.logger.warning.call_count == 1
        assert "config down" in executor.logger.warning.call_args.args[0]

    def test_explicit_none_pipe_timeout_overrides_config(self, executor):
        """pipe_timeout=None passed explicitly disables the timeout despite config."""
        executor.config_service = make_config({"kindling.execution.pipe_timeout": 900})
        assert executor._resolve_float_option(None, "pipe_timeout", minimum=0.0) is None
        assert executor._resolve_float_option(UNSET, "pipe_timeout", minimum=0.0) == 900.0


# ---- Retry Tests (Phase 2: per-pipe retry) ----


class TestRetry:
    """Config-driven per-pipe retry in _execute_pipe."""

    def _single_pipe_plan(self, pipes_registry, entity_registry, persist_strategy, execute_fn):
        pipe = make_pipe("pipe1", inputs=["entity.a"])
        pipe.execute = execute_fn
        pipes_registry.get_pipe_definition.return_value = pipe
        entity_registry.get_entity_definition.return_value = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=Mock())
        persist_strategy.create_pipe_persist_activator.return_value = Mock()
        return make_plan(["pipe1"], [Generation(number=0, pipe_ids=["pipe1"], dependencies=[])])

    def test_no_retry_by_default(self, executor, pipes_registry, entity_registry, persist_strategy):
        plan = self._single_pipe_plan(
            pipes_registry,
            entity_registry,
            persist_strategy,
            Mock(side_effect=RuntimeError("boom")),
        )

        with patch.object(executor, "emit", wraps=executor.emit) as emit_spy:
            result = executor.execute(plan, error_strategy=ErrorStrategy.CONTINUE)

        assert result.failed_count == 1
        assert result.generation_results[0].pipe_results[0].attempts == 1
        assert not any(c.args[0] == "orchestrator.pipe_retrying" for c in emit_spy.call_args_list)

    def test_retry_recovers_after_transient_failure(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        executor.config_service = make_config({"kindling.execution.retry.attempts": 2})
        plan = self._single_pipe_plan(
            pipes_registry,
            entity_registry,
            persist_strategy,
            Mock(side_effect=[RuntimeError("transient"), Mock()]),
        )

        with (
            patch("kindling.generation_executor.time.sleep") as sleep_spy,
            patch.object(executor, "emit", wraps=executor.emit) as emit_spy,
        ):
            result = executor.execute(plan)

        assert result.success_count == 1
        assert result.generation_results[0].pipe_results[0].attempts == 2
        retrying = [c for c in emit_spy.call_args_list if c.args[0] == "orchestrator.pipe_retrying"]
        failed = [c for c in emit_spy.call_args_list if c.args[0] == "orchestrator.pipe_failed"]
        assert len(retrying) == 1
        assert retrying[0].kwargs["attempt"] == 1
        assert retrying[0].kwargs["max_attempts"] == 3
        assert failed == []
        sleep_spy.assert_called_once_with(30.0)  # default interval

    def test_retry_exhaustion_reports_attempts(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        executor.config_service = make_config(
            {
                "kindling.execution.retry.attempts": 1,
                "kindling.execution.retry.interval_seconds": 0,
            }
        )
        plan = self._single_pipe_plan(
            pipes_registry,
            entity_registry,
            persist_strategy,
            Mock(side_effect=RuntimeError("boom")),
        )

        with patch.object(executor, "emit", wraps=executor.emit) as emit_spy:
            result = executor.execute(plan, error_strategy=ErrorStrategy.CONTINUE)

        pipe_result = result.generation_results[0].pipe_results[0]
        assert pipe_result.status == "failed"
        assert pipe_result.attempts == 2
        failed = [c for c in emit_spy.call_args_list if c.args[0] == "orchestrator.pipe_failed"]
        assert len(failed) == 1
        assert failed[0].kwargs["attempts"] == 2

    def test_per_pipe_config_overrides_run_default(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        executor.config_service = make_config(
            {
                "kindling.execution.retry.attempts": 0,
                "kindling.execution.pipes.pipe1.retry.attempts": 1,
                "kindling.execution.pipes.pipe1.retry.interval_seconds": 0,
            }
        )
        plan = self._single_pipe_plan(
            pipes_registry,
            entity_registry,
            persist_strategy,
            Mock(side_effect=[RuntimeError("transient"), Mock()]),
        )

        result = executor.execute(plan)

        assert result.success_count == 1
        assert result.generation_results[0].pipe_results[0].attempts == 2

    def test_param_overrides_config_retry(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        executor.config_service = make_config({"kindling.execution.retry.attempts": 5})
        plan = self._single_pipe_plan(
            pipes_registry,
            entity_registry,
            persist_strategy,
            Mock(side_effect=RuntimeError("boom")),
        )

        result = executor.execute(plan, error_strategy=ErrorStrategy.CONTINUE, retry_attempts=0)

        assert result.generation_results[0].pipe_results[0].attempts == 1

    def test_custom_interval_is_used(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        executor.config_service = make_config(
            {
                "kindling.execution.retry.attempts": 1,
                "kindling.execution.retry.interval_seconds": 5,
            }
        )
        plan = self._single_pipe_plan(
            pipes_registry,
            entity_registry,
            persist_strategy,
            Mock(side_effect=[RuntimeError("transient"), Mock()]),
        )

        with patch("kindling.generation_executor.time.sleep") as sleep_spy:
            executor.execute(plan)

        sleep_spy.assert_called_once_with(5.0)

    def test_invalid_per_pipe_value_falls_back(self, executor):
        executor.config_service = make_config(
            {"kindling.execution.pipes.pipe1.retry.attempts": "many"}
        )
        executor._retry_defaults = RetryPolicy(attempts=0)

        policy = executor._resolve_pipe_retry_policy("pipe1")

        assert policy.attempts == 0
        executor.logger.warning.assert_called()

    def test_warns_when_retry_pipe_provider_lacks_merge(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        executor.config_service = make_config({"kindling.execution.retry.attempts": 1})
        plan = self._single_pipe_plan(
            pipes_registry, entity_registry, persist_strategy, Mock(return_value=Mock())
        )
        provider = Mock(spec=[])  # no merge_to_entity attribute
        executor.provider_registry = Mock()
        executor.provider_registry.get_provider_for_entity.return_value = provider

        executor.execute(plan)

        warnings = [str(c) for c in executor.logger.warning.call_args_list]
        assert any("does not support merge" in w for w in warnings)

    def test_no_warning_for_merge_capable_provider(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        executor.config_service = make_config({"kindling.execution.retry.attempts": 1})
        plan = self._single_pipe_plan(
            pipes_registry, entity_registry, persist_strategy, Mock(return_value=Mock())
        )
        provider = Mock(spec=["merge_to_entity"])
        executor.provider_registry = Mock()
        executor.provider_registry.get_provider_for_entity.return_value = provider

        executor.execute(plan)

        warnings = [str(c) for c in executor.logger.warning.call_args_list]
        assert not any("does not support merge" in w for w in warnings)

    def test_dotted_pipe_id_resolves_via_literal_mapping(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        """Pipe ids containing dots index the pipes map literally, not by dotted traversal."""
        executor.config_service = make_config(
            {
                "kindling.execution.pipes": {
                    "ingest.orders": {"retry": {"attempts": 1, "interval_seconds": 0}}
                }
            }
        )
        pipe = make_pipe("ingest.orders", inputs=["entity.a"])
        pipe.execute = Mock(side_effect=[RuntimeError("transient"), Mock()])
        pipes_registry.get_pipe_definition.return_value = pipe
        entity_registry.get_entity_definition.return_value = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=Mock())
        persist_strategy.create_pipe_persist_activator.return_value = Mock()
        plan = make_plan(
            ["ingest.orders"],
            [Generation(number=0, pipe_ids=["ingest.orders"], dependencies=[])],
        )

        result = executor.execute(plan)

        assert result.success_count == 1
        assert result.generation_results[0].pipe_results[0].attempts == 2


# ---- Skip-Dependents Tests (Phase 3) ----


def make_chain_graph():
    """a -> b -> c chain plus independent pipe x."""
    graph = PipeGraph()
    graph.add_node(PipeNode("a", [], "entity.a"))
    graph.add_node(PipeNode("b", ["entity.a"], "entity.b"))
    graph.add_node(PipeNode("c", ["entity.b"], "entity.c"))
    graph.add_node(PipeNode("x", [], "entity.x"))
    graph.add_edge(PipeEdge(from_pipe="a", to_pipe="b", entity="entity.a"))
    graph.add_edge(PipeEdge(from_pipe="b", to_pipe="c", entity="entity.b"))
    return graph


class TestSkipDependents:
    """ErrorStrategy.SKIP_DEPENDENTS skips downstream pipes, runs independent ones."""

    def _setup(self, pipes_registry, entity_registry, persist_strategy, failing_ids):
        def get_pipe(pid):
            inputs = {"a": [], "b": ["entity.a"], "c": ["entity.b"], "x": []}[pid]
            pipe = make_pipe(pid, inputs=inputs, output=f"entity.{pid}")
            if pid in failing_ids:
                pipe.execute = Mock(side_effect=RuntimeError(f"{pid} boom"))
            return pipe

        pipes_registry.get_pipe_definition.side_effect = get_pipe
        entity_registry.get_entity_definition.return_value = Mock()
        persist_strategy.create_pipe_entity_reader.return_value = Mock(return_value=Mock())
        persist_strategy.create_pipe_persist_activator.return_value = Mock()

        graph = make_chain_graph()
        generations = [
            Generation(number=0, pipe_ids=["a", "x"], dependencies=[]),
            Generation(number=1, pipe_ids=["b"], dependencies=["a"]),
            Generation(number=2, pipe_ids=["c"], dependencies=["b"]),
        ]
        return ExecutionPlan(
            pipe_ids=["a", "x", "b", "c"],
            generations=generations,
            graph=graph,
            strategy="batch",
            metadata={},
        )

    def test_dependents_skipped_independent_branch_runs(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        plan = self._setup(pipes_registry, entity_registry, persist_strategy, failing_ids={"a"})

        with patch.object(executor, "emit", wraps=executor.emit) as emit_spy:
            result = executor.execute(plan, error_strategy=ErrorStrategy.SKIP_DEPENDENTS)

        statuses = {r.pipe_id: r.status for g in result.generation_results for r in g.pipe_results}
        assert statuses == {"a": "failed", "x": "success", "b": "skipped", "c": "skipped"}

        upstream_skips = [
            c
            for c in emit_spy.call_args_list
            if c.args[0] == "orchestrator.pipe_skipped"
            and c.kwargs.get("reason") == "upstream_failed"
        ]
        assert {c.kwargs["pipe_id"] for c in upstream_skips} == {"b", "c"}
        assert all(c.kwargs["upstream_pipe"] == "a" for c in upstream_skips)

    def test_mid_chain_failure_skips_only_downstream(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        plan = self._setup(pipes_registry, entity_registry, persist_strategy, failing_ids={"b"})

        result = executor.execute(plan, error_strategy=ErrorStrategy.SKIP_DEPENDENTS)

        statuses = {r.pipe_id: r.status for g in result.generation_results for r in g.pipe_results}
        assert statuses == {"a": "success", "x": "success", "b": "failed", "c": "skipped"}

    def test_skipped_result_carries_upstream_error(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        plan = self._setup(pipes_registry, entity_registry, persist_strategy, failing_ids={"a"})

        result = executor.execute(plan, error_strategy=ErrorStrategy.SKIP_DEPENDENTS)

        skipped = [
            r for g in result.generation_results for r in g.pipe_results if r.status == "skipped"
        ]
        assert all(r.error_type == "UpstreamFailure" for r in skipped)
        assert all("'a' failed" in r.error for r in skipped)

    def test_fail_fast_behavior_unchanged(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        plan = self._setup(pipes_registry, entity_registry, persist_strategy, failing_ids={"a"})

        result = executor.execute(plan, error_strategy=ErrorStrategy.FAIL_FAST)

        # fail_fast stops after generation 0 — b and c never appear at all
        assert len(result.generation_results) == 1

    def test_config_selects_skip_dependents(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        executor.config_service = make_config(
            {"kindling.execution.error_strategy": "skip_dependents"}
        )
        plan = self._setup(pipes_registry, entity_registry, persist_strategy, failing_ids={"a"})

        result = executor.execute(plan)

        statuses = {r.pipe_id: r.status for g in result.generation_results for r in g.pipe_results}
        assert statuses["b"] == "skipped"
        assert statuses["x"] == "success"

    def test_parallel_mode_skips_dependents(
        self, executor, pipes_registry, entity_registry, persist_strategy
    ):
        plan = self._setup(pipes_registry, entity_registry, persist_strategy, failing_ids={"a"})

        result = executor.execute(
            plan, error_strategy=ErrorStrategy.SKIP_DEPENDENTS, parallel=True, max_workers=2
        )

        statuses = {r.pipe_id: r.status for g in result.generation_results for r in g.pipe_results}
        assert statuses == {"a": "failed", "x": "success", "b": "skipped", "c": "skipped"}
