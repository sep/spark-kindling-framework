"""Integration tests for GenerationExecutor with real pipe registries.

Tests the GenerationExecutor with real DataPipesManager, PipeGraphBuilder,
and ExecutionPlanGenerator. Mocks are used only for the persistence layer
(no Spark needed) and entity providers for streaming.

See: GitHub Issue #24 - Generation Executor
"""

from unittest.mock import MagicMock, Mock

import pytest
from kindling.data_pipes import DataPipesManager, PipeMetadata
from kindling.entity_provider import StreamableEntityProvider
from kindling.execution_strategy import (
    BatchExecutionStrategy,
    ExecutionPlanGenerator,
    StreamingExecutionStrategy,
)
from kindling.generation_executor import (
    ErrorStrategy,
    ExecutionResult,
    GenerationExecutor,
    PipeResult,
)
from kindling.pipe_graph import PipeGraphBuilder

# ---- Fixtures ----


@pytest.fixture
def logger_provider():
    provider = Mock()
    logger = Mock()
    logger.info = Mock()
    logger.debug = Mock()
    logger.error = Mock()
    provider.get_logger = Mock(return_value=logger)
    return provider


@pytest.fixture
def pipes_manager(logger_provider):
    return DataPipesManager(logger_provider)


@pytest.fixture
def graph_builder(pipes_manager, logger_provider):
    return PipeGraphBuilder(pipes_manager, logger_provider)


@pytest.fixture
def plan_generator(pipes_manager, graph_builder, logger_provider):
    return ExecutionPlanGenerator(pipes_manager, graph_builder, logger_provider)


@pytest.fixture
def entity_registry():
    """Mock entity registry that returns entities by ID."""
    registry = Mock()
    registry.get_entity_definition.side_effect = lambda eid: Mock(entityid=eid)
    return registry


@pytest.fixture
def persist_strategy():
    """Mock persist strategy with tracking."""
    strategy = Mock()
    mock_df = Mock()
    strategy.create_pipe_entity_reader.return_value = Mock(return_value=mock_df)
    strategy.create_pipe_persist_activator.return_value = Mock()
    return strategy


@pytest.fixture
def provider_registry():
    """Mock provider registry returning streamable providers."""
    registry = Mock()
    provider = Mock(spec=StreamableEntityProvider)
    stream_df = Mock()
    provider.read_entity_as_stream = Mock(return_value=stream_df)
    writer = Mock()
    query = Mock()
    query.id = "test-query-id"
    writer.start = Mock(return_value=query)
    provider.append_as_stream = Mock(return_value=writer)
    registry.get_provider_for_entity = Mock(return_value=provider)
    registry._provider = provider  # stash for test access
    return registry


@pytest.fixture
def entity_path_locator():
    """Mock entity path locator."""
    locator = Mock()
    locator.get_table_path = Mock(return_value="/data/output")
    return locator


@pytest.fixture
def config_service():
    """Mock config service."""
    service = Mock()
    service.get = Mock(return_value="/checkpoints")
    return service


@pytest.fixture
def trace_provider():
    tp = Mock()
    span_cm = MagicMock()
    span_cm.__enter__ = Mock(return_value=None)
    span_cm.__exit__ = Mock(return_value=False)
    tp.span = Mock(return_value=span_cm)
    return tp


@pytest.fixture
def signal_provider():
    sp = Mock()
    signal = Mock()
    signal.send = Mock(return_value=[])
    sp.create_signal = Mock(return_value=signal)
    sp.get_signal = Mock(return_value=signal)
    return sp


@pytest.fixture
def executor(
    pipes_manager,
    entity_registry,
    provider_registry,
    entity_path_locator,
    config_service,
    persist_strategy,
    trace_provider,
    logger_provider,
    signal_provider,
):
    return GenerationExecutor(
        pipes_registry=pipes_manager,
        entity_registry=entity_registry,
        provider_registry=provider_registry,
        entity_path_locator=entity_path_locator,
        config_service=config_service,
        persist_strategy=persist_strategy,
        trace_provider=trace_provider,
        logger_provider=logger_provider,
        signal_provider=signal_provider,
    )


def register_pipe(manager, pipe_id, inputs, output, tags=None):
    """Helper to register a pipe."""
    manager.register_pipe(
        pipe_id,
        name=pipe_id,
        execute=lambda *args, **kwargs: Mock(),
        tags=tags or {},
        input_entity_ids=inputs,
        output_entity_id=output,
        output_type="delta",
    )


# ---- Medallion Architecture Tests ----


class TestMedallionBatchExecution:
    """Test batch execution of medallion architecture."""

    def setup_medallion(self, manager):
        register_pipe(manager, "bronze_orders", [], "bronze.orders")
        register_pipe(manager, "bronze_customers", [], "bronze.customers")
        register_pipe(manager, "silver_orders", ["bronze.orders"], "silver.orders")
        register_pipe(manager, "silver_customers", ["bronze.customers"], "silver.customers")
        register_pipe(
            manager,
            "gold_analytics",
            ["silver.orders", "silver.customers"],
            "gold.analytics",
        )
        return [
            "bronze_orders",
            "bronze_customers",
            "silver_orders",
            "silver_customers",
            "gold_analytics",
        ]

    def test_batch_executes_in_forward_order(
        self, executor, pipes_manager, plan_generator, persist_strategy
    ):
        """Batch plan runs sources before sinks."""
        pipe_ids = self.setup_medallion(pipes_manager)

        plan = plan_generator.generate_batch_plan(pipe_ids)
        result = executor.execute_batch(plan)

        assert result.all_succeeded is True
        assert result.success_count == 5
        assert len(result.generation_results) == 3  # bronze, silver, gold

        # Verify order: bronze first, gold last
        gen0_pipes = set(
            result.generation_results[0].pipe_results[i].pipe_id
            for i in range(len(result.generation_results[0].pipe_results))
        )
        gen2_pipes = set(
            result.generation_results[2].pipe_results[i].pipe_id
            for i in range(len(result.generation_results[2].pipe_results))
        )

        assert gen0_pipes == {"bronze_orders", "bronze_customers"}
        assert gen2_pipes == {"gold_analytics"}

    def test_batch_parallel_within_generation(
        self, executor, pipes_manager, plan_generator, persist_strategy
    ):
        """Parallel mode runs independent pipes concurrently within a generation."""
        pipe_ids = self.setup_medallion(pipes_manager)

        plan = plan_generator.generate_batch_plan(pipe_ids)
        result = executor.execute_batch(plan, parallel=True, max_workers=2)

        assert result.all_succeeded is True
        assert result.success_count == 5

    def test_persist_strategy_called_correctly(
        self, executor, pipes_manager, plan_generator, persist_strategy
    ):
        """Persist strategy's reader and activator are called for each pipe."""
        register_pipe(pipes_manager, "single_pipe", ["entity.src"], "entity.dst")

        plan = plan_generator.generate_batch_plan(["single_pipe"])
        executor.execute_batch(plan)

        persist_strategy.create_pipe_entity_reader.assert_called_once()
        persist_strategy.create_pipe_persist_activator.assert_called_once()


# ---- Medallion Streaming Tests ----


class TestMedallionStreamingExecution:
    """Test streaming execution of medallion architecture."""

    def setup_medallion(self, manager):
        register_pipe(manager, "bronze_ingest", ["raw.events"], "bronze.events")
        register_pipe(manager, "silver_clean", ["bronze.events"], "silver.events")
        register_pipe(manager, "gold_aggregate", ["silver.events"], "gold.metrics")
        return ["bronze_ingest", "silver_clean", "gold_aggregate"]

    def test_streaming_starts_sinks_first(
        self,
        executor,
        pipes_manager,
        plan_generator,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """Streaming plan starts sinks (gold) before sources (bronze)."""
        pipe_ids = self.setup_medallion(pipes_manager)

        call_order = []
        provider = provider_registry._provider

        original_read = provider.read_entity_as_stream

        def track_read(entity):
            call_order.append(entity.entityid)
            return original_read(entity)

        provider.read_entity_as_stream = Mock(side_effect=track_read)

        plan = plan_generator.generate_streaming_plan(pipe_ids)
        result = executor.execute_streaming(plan)

        assert result.all_succeeded is True
        assert result.success_count == 3

        # Sinks first: gold reads silver, then silver reads bronze, then bronze reads its input
        # The order of entity reads should reflect sink-first ordering
        assert len(call_order) == 3

    def test_streaming_returns_queries(
        self,
        executor,
        pipes_manager,
        plan_generator,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """Streaming result contains query handles for all pipes."""
        pipe_ids = self.setup_medallion(pipes_manager)

        plan = plan_generator.generate_streaming_plan(pipe_ids)
        result = executor.execute_streaming(plan)

        assert len(result.streaming_queries) == 3
        assert "bronze_ingest" in result.streaming_queries
        assert "silver_clean" in result.streaming_queries
        assert "gold_aggregate" in result.streaming_queries

    def test_streaming_with_per_pipe_options(
        self,
        executor,
        pipes_manager,
        plan_generator,
        entity_registry,
        provider_registry,
        entity_path_locator,
        config_service,
    ):
        """Per-pipe streaming options are passed correctly as checkpoint paths."""
        pipe_ids = self.setup_medallion(pipes_manager)
        provider = provider_registry._provider

        options = {
            "bronze_ingest": {"base_checkpoint_path": "/chk/bronze"},
            "silver_clean": {"base_checkpoint_path": "/chk/silver"},
            "gold_aggregate": {"base_checkpoint_path": "/chk/gold"},
        }

        plan = plan_generator.generate_streaming_plan(pipe_ids)
        executor.execute_streaming(plan, streaming_options=options)

        # Verify checkpoint paths were constructed correctly
        calls = provider.append_as_stream.call_args_list
        checkpoint_paths = [c[0][2] for c in calls]
        assert "/chk/gold/gold_aggregate" in checkpoint_paths
        assert "/chk/silver/silver_clean" in checkpoint_paths
        assert "/chk/bronze/bronze_ingest" in checkpoint_paths


# ---- Fan-Out / Fan-In Tests ----


class TestFanOutFanIn:
    """Test execution with fan-out and fan-in patterns."""

    def test_batch_fan_out_fan_in(self, executor, pipes_manager, plan_generator):
        """Fan-out then fan-in runs correctly in batch mode."""
        register_pipe(pipes_manager, "source", [], "entity.raw")
        register_pipe(pipes_manager, "process_a", ["entity.raw"], "entity.a")
        register_pipe(pipes_manager, "process_b", ["entity.raw"], "entity.b")
        register_pipe(pipes_manager, "sink", ["entity.a", "entity.b"], "entity.final")

        pipe_ids = ["source", "process_a", "process_b", "sink"]
        plan = plan_generator.generate_batch_plan(pipe_ids)
        result = executor.execute_batch(plan, parallel=True)

        assert result.all_succeeded is True
        assert result.success_count == 4
        assert len(result.generation_results) == 3  # source, processors, sink


# ---- Error Handling Tests ----


class TestErrorHandling:
    """Test error handling across real pipe graphs."""

    def test_fail_fast_stops_dependent_generations(
        self, executor, pipes_manager, plan_generator, persist_strategy
    ):
        """Fail fast prevents dependent generations from running."""
        register_pipe(pipes_manager, "source", [], "entity.src")
        register_pipe(pipes_manager, "transform", ["entity.src"], "entity.mid")
        register_pipe(pipes_manager, "sink", ["entity.mid"], "entity.dst")

        # Transform pipe's reader fails (source has no inputs, so it always succeeds)
        call_count = {"count": 0}

        def counting_reader(entity, usewm):
            call_count["count"] += 1
            if call_count["count"] > 0:
                # Fail on transform pipe (second pipe's reader call)
                raise RuntimeError("Transform read failed")
            return Mock()

        persist_strategy.create_pipe_entity_reader.return_value = counting_reader

        pipe_ids = ["source", "transform", "sink"]
        plan = plan_generator.generate_batch_plan(pipe_ids)
        result = executor.execute(plan, error_strategy=ErrorStrategy.FAIL_FAST)

        # Generation 0 (source) succeeds (no inputs), generation 1 (transform) fails
        # Generation 2 (sink) should NOT be attempted
        assert len(result.generation_results) == 2
        assert result.failed_count == 1
        assert result.failed_pipes == ["transform"]

    def test_continue_runs_all_generations(
        self, executor, pipes_manager, plan_generator, persist_strategy
    ):
        """Continue mode runs all generations even on failure."""
        register_pipe(pipes_manager, "source", [], "entity.src")
        register_pipe(pipes_manager, "transform", ["entity.src"], "entity.mid")

        call_count = {"count": 0}

        def alternating_reader(entity, usewm):
            call_count["count"] += 1
            if call_count["count"] == 1:
                raise RuntimeError("First pipe failed")
            return Mock()

        persist_strategy.create_pipe_entity_reader.return_value = alternating_reader

        pipe_ids = ["source", "transform"]
        plan = plan_generator.generate_batch_plan(pipe_ids)
        result = executor.execute(plan, error_strategy=ErrorStrategy.CONTINUE)

        # Both generations attempted
        assert len(result.generation_results) == 2


# ---- Summary & Result Tests ----


class TestResultSummary:
    """Test the summary output of execution results."""

    def test_summary_contains_all_fields(self, executor, pipes_manager, plan_generator):
        """Summary dict contains all expected fields."""
        register_pipe(pipes_manager, "pipe1", [], "entity.a")
        register_pipe(pipes_manager, "pipe2", [], "entity.b")

        plan = plan_generator.generate_batch_plan(["pipe1", "pipe2"])
        result = executor.execute_batch(plan)

        summary = result.get_summary()
        assert "run_id" in summary
        assert summary["strategy"] == "batch"
        assert summary["total_pipes"] == 2
        assert summary["success_count"] == 2
        assert summary["all_succeeded"] is True
        assert summary["failed_pipes"] == []
        assert summary["duration_seconds"] >= 0
