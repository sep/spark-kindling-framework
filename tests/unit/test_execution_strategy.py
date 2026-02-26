"""Unit tests for execution_strategy module."""

from unittest.mock import Mock

import pytest
from kindling.data_pipes import DataPipesManager, PipeMetadata
from kindling.execution_strategy import (
    BatchExecutionStrategy,
    ConfigBasedExecutionStrategy,
    ExecutionPlan,
    ExecutionPlanGenerator,
    ExecutionStrategy,
    Generation,
    StreamingExecutionStrategy,
)
from kindling.pipe_graph import PipeEdge, PipeGraph, PipeGraphBuilder, PipeNode


class TestGeneration:
    """Tests for Generation dataclass."""

    def test_generation_creation(self):
        """Test creating a generation."""
        gen = Generation(number=0, pipe_ids=["pipe1", "pipe2"], dependencies=["pipe0"])

        assert gen.number == 0
        assert gen.pipe_ids == ["pipe1", "pipe2"]
        assert gen.dependencies == ["pipe0"]

    def test_generation_len(self):
        """Test generation length."""
        gen = Generation(0, ["pipe1", "pipe2", "pipe3"])
        assert len(gen) == 3

    def test_generation_iter(self):
        """Test generation iteration."""
        gen = Generation(0, ["pipe1", "pipe2"])
        pipes = list(gen)
        assert pipes == ["pipe1", "pipe2"]

    def test_generation_contains(self):
        """Test generation membership."""
        gen = Generation(0, ["pipe1", "pipe2"])
        assert "pipe1" in gen
        assert "pipe3" not in gen


class TestExecutionPlan:
    """Tests for ExecutionPlan dataclass."""

    @pytest.fixture
    def simple_graph(self):
        """Create a simple graph."""
        graph = PipeGraph()
        graph.add_node(PipeNode("pipe1", [], "entity.a"))
        graph.add_node(PipeNode("pipe2", ["entity.a"], "entity.b"))
        return graph

    @pytest.fixture
    def simple_plan(self, simple_graph):
        """Create a simple execution plan."""
        gen1 = Generation(0, ["pipe1"], [])
        gen2 = Generation(1, ["pipe2"], ["pipe1"])

        return ExecutionPlan(
            pipe_ids=["pipe1", "pipe2"],
            generations=[gen1, gen2],
            graph=simple_graph,
            strategy="batch",
        )

    def test_total_pipes(self, simple_plan):
        """Test total pipes count."""
        assert simple_plan.total_pipes() == 2

    def test_total_generations(self, simple_plan):
        """Test total generations count."""
        assert simple_plan.total_generations() == 2

    def test_max_parallelism(self, simple_plan):
        """Test max parallelism calculation."""
        assert simple_plan.max_parallelism() == 1

        # Add a plan with parallel pipes
        gen1 = Generation(0, ["pipe1", "pipe2", "pipe3"], [])
        plan = ExecutionPlan(
            pipe_ids=["pipe1", "pipe2", "pipe3"],
            generations=[gen1],
            graph=simple_plan.graph,
            strategy="batch",
        )
        assert plan.max_parallelism() == 3

    def test_get_generation(self, simple_plan):
        """Test getting generation by number."""
        gen0 = simple_plan.get_generation(0)
        assert gen0 is not None
        assert gen0.number == 0
        assert gen0.pipe_ids == ["pipe1"]

        gen_invalid = simple_plan.get_generation(10)
        assert gen_invalid is None

    def test_find_pipe_generation(self, simple_plan):
        """Test finding which generation a pipe belongs to."""
        assert simple_plan.find_pipe_generation("pipe1") == 0
        assert simple_plan.find_pipe_generation("pipe2") == 1
        assert simple_plan.find_pipe_generation("pipe3") is None

    def test_validate_success(self, simple_plan):
        """Test successful validation."""
        assert simple_plan.validate() is True

    def test_validate_missing_pipes(self, simple_plan):
        """Test validation fails with missing pipes."""
        # Remove a pipe from generations
        simple_plan.generations[1].pipe_ids = []

        with pytest.raises(ValueError) as exc_info:
            simple_plan.validate()

        assert "Missing" in str(exc_info.value)
        assert "pipe2" in str(exc_info.value)

    def test_validate_unsatisfied_dependencies(self, simple_graph):
        """Test validation fails with unsatisfied dependencies."""
        # Create plan where pipe2 (depends on pipe1) runs before pipe1
        gen1 = Generation(0, ["pipe2"], [])  # Wrong order!
        gen2 = Generation(1, ["pipe1"], [])

        plan = ExecutionPlan(
            pipe_ids=["pipe1", "pipe2"],
            generations=[gen1, gen2],
            graph=simple_graph,
            strategy="batch",
        )

        # Add edge so pipe2 depends on pipe1
        simple_graph.add_edge(PipeEdge("pipe1", "pipe2", "entity.a"))

        with pytest.raises(ValueError) as exc_info:
            plan.validate()

        assert "Dependencies not satisfied" in str(exc_info.value)

    def test_get_summary(self, simple_plan):
        """Test getting plan summary."""
        summary = simple_plan.get_summary()

        assert summary["strategy"] == "batch"
        assert summary["total_pipes"] == 2
        assert summary["total_generations"] == 2
        assert summary["max_parallelism"] == 1
        assert "generation_sizes" in summary


class TestBatchExecutionStrategy:
    """Tests for BatchExecutionStrategy."""

    @pytest.fixture
    def logger_provider(self):
        """Create mock logger provider."""
        provider = Mock()
        logger = Mock()
        logger.info = Mock()
        logger.debug = Mock()
        logger.error = Mock()
        provider.get_logger = Mock(return_value=logger)
        return provider

    @pytest.fixture
    def strategy(self, logger_provider):
        """Create batch strategy."""
        return BatchExecutionStrategy(logger_provider)

    def create_graph(self, nodes_data):
        """Helper to create graph from node data."""
        graph = PipeGraph()
        for pipe_id, inputs, output in nodes_data:
            node = PipeNode(pipe_id, inputs, output)
            graph.add_node(node)

        # Add edges based on dependencies
        for pipe_id, inputs, output in nodes_data:
            for input_entity in inputs:
                # Find producer of this entity
                for producer_id, _, producer_output in nodes_data:
                    if producer_output == input_entity:
                        graph.add_edge(PipeEdge(producer_id, pipe_id, input_entity))

        return graph

    def test_strategy_name(self, strategy):
        """Test strategy name."""
        assert strategy.get_strategy_name() == "batch"

    def test_linear_graph(self, strategy):
        """Test batch strategy on linear graph."""
        # pipe1 -> pipe2 -> pipe3
        graph = self.create_graph(
            [
                ("pipe1", [], "entity.a"),
                ("pipe2", ["entity.a"], "entity.b"),
                ("pipe3", ["entity.b"], "entity.c"),
            ]
        )

        plan = strategy.plan(graph, ["pipe1", "pipe2", "pipe3"])

        assert plan.strategy == "batch"
        assert plan.total_generations() == 3
        assert plan.generations[0].pipe_ids == ["pipe1"]
        assert plan.generations[1].pipe_ids == ["pipe2"]
        assert plan.generations[2].pipe_ids == ["pipe3"]
        assert plan.max_parallelism() == 1

    def test_parallel_graph(self, strategy):
        """Test batch strategy on parallel graph."""
        # pipe1 -> pipe2, pipe3 (parallel)
        graph = self.create_graph(
            [
                ("pipe1", [], "entity.a"),
                ("pipe2", ["entity.a"], "entity.b"),
                ("pipe3", ["entity.a"], "entity.c"),
            ]
        )

        plan = strategy.plan(graph, ["pipe1", "pipe2", "pipe3"])

        assert plan.total_generations() == 2
        assert plan.generations[0].pipe_ids == ["pipe1"]
        assert set(plan.generations[1].pipe_ids) == {"pipe2", "pipe3"}
        assert plan.max_parallelism() == 2
        assert plan.metadata["cache_recommendations"] == {"entity.a": "MEMORY_AND_DISK"}
        assert plan.metadata["cache_candidate_count"] == 1
        assert plan.metadata["cache_candidate_entities"] == ["entity.a"]

    def test_diamond_graph(self, strategy):
        """Test batch strategy on diamond graph."""
        # pipe1 -> pipe2, pipe3 -> pipe4
        graph = self.create_graph(
            [
                ("pipe1", [], "entity.a"),
                ("pipe2", ["entity.a"], "entity.b"),
                ("pipe3", ["entity.a"], "entity.c"),
                ("pipe4", ["entity.b", "entity.c"], "entity.d"),
            ]
        )

        plan = strategy.plan(graph, ["pipe1", "pipe2", "pipe3", "pipe4"])

        assert plan.total_generations() == 3
        assert plan.generations[0].pipe_ids == ["pipe1"]
        assert set(plan.generations[1].pipe_ids) == {"pipe2", "pipe3"}
        assert plan.generations[2].pipe_ids == ["pipe4"]

    def test_forward_order(self, strategy):
        """Test that batch uses forward topological order."""
        graph = self.create_graph(
            [
                ("source", [], "entity.raw"),
                ("transform", ["entity.raw"], "entity.clean"),
                ("sink", ["entity.clean"], "entity.final"),
            ]
        )

        plan = strategy.plan(graph, ["source", "transform", "sink"])

        # Sources first, sinks last
        assert plan.generations[0].pipe_ids == ["source"]
        assert plan.generations[-1].pipe_ids == ["sink"]

    def test_external_sources(self, strategy):
        """Test handling of external data sources."""
        # Pipe reads from external source (not produced by any pipe in the graph)
        graph = self.create_graph(
            [
                ("process", ["external.data"], "entity.processed"),
                ("sink", ["entity.processed"], "entity.final"),
            ]
        )

        plan = strategy.plan(graph, ["process", "sink"])

        # process should be in first generation (no dependencies in the graph)
        assert plan.total_generations() == 2
        assert plan.generations[0].pipe_ids == ["process"]
        assert plan.generations[1].pipe_ids == ["sink"]

    def test_plan_validation(self, strategy):
        """Test that generated plan passes validation."""
        graph = self.create_graph(
            [
                ("pipe1", [], "entity.a"),
                ("pipe2", ["entity.a"], "entity.b"),
            ]
        )

        plan = strategy.plan(graph, ["pipe1", "pipe2"])

        # Should not raise
        assert plan.validate() is True


class TestStreamingExecutionStrategy:
    """Tests for StreamingExecutionStrategy."""

    @pytest.fixture
    def logger_provider(self):
        """Create mock logger provider."""
        provider = Mock()
        logger = Mock()
        logger.info = Mock()
        logger.debug = Mock()
        logger.error = Mock()
        provider.get_logger = Mock(return_value=logger)
        return provider

    @pytest.fixture
    def strategy(self, logger_provider):
        """Create streaming strategy."""
        return StreamingExecutionStrategy(logger_provider)

    def create_graph(self, nodes_data):
        """Helper to create graph from node data."""
        graph = PipeGraph()
        for pipe_id, inputs, output in nodes_data:
            node = PipeNode(pipe_id, inputs, output)
            graph.add_node(node)

        # Add edges based on dependencies
        for pipe_id, inputs, output in nodes_data:
            for input_entity in inputs:
                # Find producer of this entity
                for producer_id, _, producer_output in nodes_data:
                    if producer_output == input_entity:
                        graph.add_edge(PipeEdge(producer_id, pipe_id, input_entity))

        return graph

    def test_strategy_name(self, strategy):
        """Test strategy name."""
        assert strategy.get_strategy_name() == "streaming"

    def test_streaming_has_no_cache_recommendations(self, strategy):
        """Streaming plans should not include cache recommendations."""
        graph = self.create_graph(
            [
                ("source", [], "entity.a"),
                ("sink", ["entity.a"], "entity.b"),
            ]
        )

        plan = strategy.plan(graph, ["source", "sink"])

        assert plan.metadata["cache_recommendations"] == {}
        assert plan.metadata["cache_candidate_count"] == 0
        assert plan.metadata["cache_candidate_entities"] == []

    def test_linear_graph(self, strategy):
        """Test streaming strategy on linear graph."""
        # pipe1 -> pipe2 -> pipe3
        graph = self.create_graph(
            [
                ("pipe1", [], "entity.a"),
                ("pipe2", ["entity.a"], "entity.b"),
                ("pipe3", ["entity.b"], "entity.c"),
            ]
        )

        plan = strategy.plan(graph, ["pipe1", "pipe2", "pipe3"])

        assert plan.strategy == "streaming"
        assert plan.total_generations() == 3
        # Reverse order: sinks first
        assert plan.generations[0].pipe_ids == ["pipe3"]
        assert plan.generations[1].pipe_ids == ["pipe2"]
        assert plan.generations[2].pipe_ids == ["pipe1"]

    def test_parallel_graph(self, strategy):
        """Test streaming strategy on parallel graph."""
        # pipe1 -> pipe2, pipe3 (parallel)
        graph = self.create_graph(
            [
                ("pipe1", [], "entity.a"),
                ("pipe2", ["entity.a"], "entity.b"),
                ("pipe3", ["entity.a"], "entity.c"),
            ]
        )

        plan = strategy.plan(graph, ["pipe1", "pipe2", "pipe3"])

        assert plan.total_generations() == 2
        # Sinks first (parallel), then source
        assert set(plan.generations[0].pipe_ids) == {"pipe2", "pipe3"}
        assert plan.generations[1].pipe_ids == ["pipe1"]

    def test_diamond_graph(self, strategy):
        """Test streaming strategy on diamond graph."""
        # pipe1 -> pipe2, pipe3 -> pipe4
        graph = self.create_graph(
            [
                ("pipe1", [], "entity.a"),
                ("pipe2", ["entity.a"], "entity.b"),
                ("pipe3", ["entity.a"], "entity.c"),
                ("pipe4", ["entity.b", "entity.c"], "entity.d"),
            ]
        )

        plan = strategy.plan(graph, ["pipe1", "pipe2", "pipe3", "pipe4"])

        assert plan.total_generations() == 3
        # Reverse order: sink first, then middle, then source
        assert plan.generations[0].pipe_ids == ["pipe4"]
        assert set(plan.generations[1].pipe_ids) == {"pipe2", "pipe3"}
        assert plan.generations[2].pipe_ids == ["pipe1"]

    def test_reverse_order(self, strategy):
        """Test that streaming uses reverse topological order."""
        graph = self.create_graph(
            [
                ("source", [], "entity.raw"),
                ("transform", ["entity.raw"], "entity.clean"),
                ("sink", ["entity.clean"], "entity.final"),
            ]
        )

        plan = strategy.plan(graph, ["source", "transform", "sink"])

        # Sinks first, sources last
        assert plan.generations[0].pipe_ids == ["sink"]
        assert plan.generations[-1].pipe_ids == ["source"]

    def test_streaming_validation(self, strategy):
        """Test that streaming plan validation works."""
        graph = self.create_graph(
            [
                ("source", [], "entity.a"),
                ("sink", ["entity.a"], "entity.b"),
            ]
        )

        plan = strategy.plan(graph, ["source", "sink"])

        # Streaming validation checks that consumers start before producers
        # This should pass since we generated it correctly
        assert plan.validate() is True


class TestConfigBasedExecutionStrategy:
    """Tests for ConfigBasedExecutionStrategy (batch vs streaming mode detection)."""

    @pytest.fixture
    def logger_provider(self):
        """Create mock logger provider."""
        provider = Mock()
        logger = Mock()
        logger.info = Mock()
        logger.debug = Mock()
        logger.warning = Mock()
        logger.error = Mock()
        provider.get_logger = Mock(return_value=logger)
        return provider

    @pytest.fixture
    def strategy(self, logger_provider):
        """Create config-based strategy."""
        return ConfigBasedExecutionStrategy(logger_provider)

    def create_graph_with_modes(self, nodes_data):
        """Helper to create graph with processing mode tags.

        Args:
            nodes_data: List of (pipe_id, inputs, output, mode, tags)
                       mode can be "batch", "streaming", or None
        """
        graph = PipeGraph()
        for pipe_id, inputs, output, mode, extra_tags in nodes_data:
            # Create metadata with mode tag if specified
            tags = {**extra_tags}
            if mode:
                tags["processing_mode"] = mode
            metadata = PipeMetadata(
                pipeid=pipe_id,
                name=pipe_id,
                execute=lambda: None,
                tags=tags,
                input_entity_ids=inputs,
                output_entity_id=output,
                output_type="delta",
            )
            node = PipeNode(pipe_id, inputs, output, metadata=metadata)
            graph.add_node(node)

        # Add edges based on dependencies
        for pipe_id, inputs, output, mode, extra_tags in nodes_data:
            for input_entity in inputs:
                # Find producer of this entity
                for producer_id, _, producer_output, _, _ in nodes_data:
                    if producer_output == input_entity:
                        graph.add_edge(PipeEdge(producer_id, pipe_id, input_entity))

        return graph

    def test_strategy_name(self, strategy):
        """Test strategy name."""
        assert strategy.get_strategy_name() == "config_based"

    def test_batch_mode_forward_order(self, strategy):
        """Test batch mode uses forward topological order."""
        graph = self.create_graph_with_modes(
            [
                ("source", [], "entity.a", "batch", {}),
                ("transform", ["entity.a"], "entity.b", "batch", {}),
                ("sink", ["entity.b"], "entity.c", "batch", {}),
            ]
        )

        plan = strategy.plan(graph, ["source", "transform", "sink"])

        assert plan.strategy == "config_based"
        assert plan.metadata["detected_mode"] == "batch"
        assert plan.total_generations() == 3
        # Forward order: source -> transform -> sink
        assert plan.generations[0].pipe_ids == ["source"]
        assert plan.generations[1].pipe_ids == ["transform"]
        assert plan.generations[2].pipe_ids == ["sink"]

    def test_streaming_mode_reverse_order(self, strategy):
        """Test streaming mode uses reverse topological order."""
        graph = self.create_graph_with_modes(
            [
                ("source", [], "entity.a", "streaming", {}),
                ("transform", ["entity.a"], "entity.b", "streaming", {}),
                ("sink", ["entity.b"], "entity.c", "streaming", {}),
            ]
        )

        plan = strategy.plan(graph, ["source", "transform", "sink"])

        assert plan.strategy == "config_based"
        assert plan.metadata["detected_mode"] == "streaming"
        assert plan.total_generations() == 3
        # Reverse order: sink -> transform -> source
        assert plan.generations[0].pipe_ids == ["sink"]
        assert plan.generations[1].pipe_ids == ["transform"]
        assert plan.generations[2].pipe_ids == ["source"]

    def test_mixed_mode_uses_streaming(self, strategy):
        """Test if any pipe is streaming, entire plan uses streaming order."""
        graph = self.create_graph_with_modes(
            [
                ("source", [], "entity.a", "batch", {}),
                ("transform", ["entity.a"], "entity.b", "streaming", {}),
                ("sink", ["entity.b"], "entity.c", "batch", {}),
            ]
        )

        plan = strategy.plan(graph, ["source", "transform", "sink"])

        assert plan.strategy == "config_based"
        assert plan.metadata["detected_mode"] == "streaming"
        # Should use reverse order because one pipe is streaming
        assert plan.generations[0].pipe_ids == ["sink"]

    def test_untagged_defaults_to_batch(self, strategy):
        """Test untagged pipes default to batch mode."""
        graph = self.create_graph_with_modes(
            [
                ("source", [], "entity.a", None, {}),
                ("transform", ["entity.a"], "entity.b", None, {}),
                ("sink", ["entity.b"], "entity.c", None, {}),
            ]
        )

        plan = strategy.plan(graph, ["source", "transform", "sink"])

        assert plan.strategy == "config_based"
        assert plan.metadata["detected_mode"] == "batch"
        # Forward order (batch default)
        assert plan.generations[0].pipe_ids == ["source"]

    def test_metadata_includes_mode_info(self, strategy):
        """Test plan metadata includes detected mode information."""
        graph = self.create_graph_with_modes(
            [
                ("pipe1", [], "entity.a", "streaming", {}),
            ]
        )

        plan = strategy.plan(graph, ["pipe1"])

        assert "detected_mode" in plan.metadata
        assert plan.metadata["detected_mode"] == "streaming"
        assert "mode_source" in plan.metadata
        assert plan.metadata["mode_source"] == "pipe_tags"


class TestExecutionPlanGenerator:
    """Tests for ExecutionPlanGenerator facade."""

    @pytest.fixture
    def logger_provider(self):
        """Create mock logger provider."""
        provider = Mock()
        logger = Mock()
        logger.info = Mock()
        logger.debug = Mock()
        provider.get_logger = Mock(return_value=logger)
        return provider

    @pytest.fixture
    def pipe_registry(self, logger_provider):
        """Create real registry."""
        return DataPipesManager(logger_provider)

    @pytest.fixture
    def graph_builder(self, pipe_registry, logger_provider):
        """Create graph builder."""
        return PipeGraphBuilder(pipe_registry, logger_provider)

    @pytest.fixture
    def generator(self, pipe_registry, graph_builder, logger_provider):
        """Create plan generator."""
        return ExecutionPlanGenerator(pipe_registry, graph_builder, logger_provider)

    def register_pipe(self, registry, pipe_id, inputs, output):
        """Helper to register a pipe."""
        registry.register_pipe(
            pipe_id,
            name=pipe_id,
            execute=lambda: None,
            tags={},
            input_entity_ids=inputs,
            output_entity_id=output,
            output_type="delta",
        )

    def test_generate_batch_plan(self, generator, pipe_registry):
        """Test generating batch plan."""
        self.register_pipe(pipe_registry, "pipe1", [], "entity.a")
        self.register_pipe(pipe_registry, "pipe2", ["entity.a"], "entity.b")

        plan = generator.generate_batch_plan(["pipe1", "pipe2"])

        assert plan.strategy == "batch"
        assert plan.total_generations() == 2
        assert plan.generations[0].pipe_ids == ["pipe1"]

    def test_generate_streaming_plan(self, generator, pipe_registry):
        """Test generating streaming plan."""
        self.register_pipe(pipe_registry, "pipe1", [], "entity.a")
        self.register_pipe(pipe_registry, "pipe2", ["entity.a"], "entity.b")

        plan = generator.generate_streaming_plan(["pipe1", "pipe2"])

        assert plan.strategy == "streaming"
        assert plan.total_generations() == 2
        # Reverse order
        assert plan.generations[0].pipe_ids == ["pipe2"]
        assert plan.generations[1].pipe_ids == ["pipe1"]

    def test_generate_with_custom_strategy(self, generator, pipe_registry, logger_provider):
        """Test generating plan with custom strategy."""
        self.register_pipe(pipe_registry, "pipe1", [], "entity.a")

        custom_strategy = BatchExecutionStrategy(logger_provider)
        plan = generator.generate_plan(["pipe1"], custom_strategy)

        assert plan.strategy == "batch"

    def test_visualize_plan(self, generator, pipe_registry):
        """Test plan visualization."""
        self.register_pipe(pipe_registry, "pipe1", [], "entity.a")
        self.register_pipe(pipe_registry, "pipe2", ["entity.a"], "entity.b")

        plan = generator.generate_batch_plan(["pipe1", "pipe2"])
        visualization = generator.visualize_plan(plan)

        assert "Execution Plan" in visualization
        assert "Generation 0" in visualization
        assert "Generation 1" in visualization
        assert "pipe1" in visualization
        assert "pipe2" in visualization

    def test_complex_workflow(self, generator, pipe_registry):
        """Test generator with complex workflow."""
        # Create medallion architecture
        self.register_pipe(pipe_registry, "bronze_orders", [], "bronze.orders")
        self.register_pipe(pipe_registry, "bronze_customers", [], "bronze.customers")
        self.register_pipe(pipe_registry, "silver_orders", ["bronze.orders"], "silver.orders")
        self.register_pipe(
            pipe_registry, "silver_customers", ["bronze.customers"], "silver.customers"
        )
        self.register_pipe(
            pipe_registry, "gold_analytics", ["silver.orders", "silver.customers"], "gold.analytics"
        )

        pipe_ids = [
            "bronze_orders",
            "bronze_customers",
            "silver_orders",
            "silver_customers",
            "gold_analytics",
        ]

        # Test batch plan
        batch_plan = generator.generate_batch_plan(pipe_ids)
        assert batch_plan.total_generations() == 3
        assert batch_plan.max_parallelism() == 2

        # Test streaming plan
        streaming_plan = generator.generate_streaming_plan(pipe_ids)
        assert streaming_plan.total_generations() == 3
        # Verify reverse order
        assert streaming_plan.generations[0].pipe_ids == ["gold_analytics"]
        assert set(streaming_plan.generations[2].pipe_ids) == {"bronze_orders", "bronze_customers"}

    def test_generate_config_based_plan(self, generator, pipe_registry):
        """Test generating config-based plan using pipe processing_mode tags."""
        # Register dependent pipes tagged for batch mode
        pipe_registry.register_pipe(
            "source",
            name="source",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=[],
            output_entity_id="entity.a",
            output_type="delta",
        )
        pipe_registry.register_pipe(
            "consumer",
            name="consumer",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=["entity.a"],
            output_entity_id="entity.b",
            output_type="delta",
        )

        plan = generator.generate_config_based_plan(["source", "consumer"])

        assert plan.strategy == "config_based"
        assert plan.metadata["detected_mode"] == "batch"
        assert plan.total_generations() == 2
        assert plan.generations[0].pipe_ids == ["source"]
        assert plan.generations[1].pipe_ids == ["consumer"]
