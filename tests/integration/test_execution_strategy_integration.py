"""Integration tests for execution_strategy module with real DataPipesManager."""

import pytest
from unittest.mock import Mock

from kindling.data_pipes import DataPipesManager, PipeMetadata
from kindling.pipe_graph import PipeGraphBuilder
from kindling.execution_strategy import (
    ExecutionPlanGenerator,
    BatchExecutionStrategy,
    StreamingExecutionStrategy,
    ConfigBasedExecutionStrategy,
)


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
def pipes_manager(logger_provider):
    """Create real pipes manager."""
    return DataPipesManager(logger_provider)


@pytest.fixture
def graph_builder(pipes_manager, logger_provider):
    """Create graph builder."""
    return PipeGraphBuilder(pipes_manager, logger_provider)


@pytest.fixture
def plan_generator(pipes_manager, graph_builder, logger_provider):
    """Create execution plan generator."""
    return ExecutionPlanGenerator(pipes_manager, graph_builder, logger_provider)


def register_pipe(manager, pipe_id, inputs, output):
    """Helper to register a pipe."""
    manager.register_pipe(
        pipe_id,
        name=pipe_id,
        execute=lambda: None,
        tags={},
        input_entity_ids=inputs,
        output_entity_id=output,
        output_type="delta"
    )


class TestMedallionArchitectureExecution:
    """Test execution strategies on medallion architecture."""
    
    def setup_medallion_pipeline(self, manager):
        """Set up a medallion architecture pipeline."""
        # Bronze layer (ingestion from external sources)
        register_pipe(manager, "bronze_orders", [], "bronze.orders")
        register_pipe(manager, "bronze_customers", [], "bronze.customers")
        register_pipe(manager, "bronze_products", [], "bronze.products")
        
        # Silver layer (cleaning and joining)
        register_pipe(manager, "silver_orders", ["bronze.orders"], "silver.orders")
        register_pipe(manager, "silver_customers", ["bronze.customers"], "silver.customers")
        register_pipe(manager, "silver_products", ["bronze.products"], "silver.products")
        
        # Gold layer (aggregations and analytics)
        register_pipe(
            manager,
            "gold_customer_orders",
            ["silver.orders", "silver.customers"],
            "gold.customer_orders"
        )
        register_pipe(
            manager,
            "gold_product_analytics",
            ["silver.orders", "silver.products"],
            "gold.product_analytics"
        )
        register_pipe(
            manager,
            "gold_revenue_dashboard",
            ["gold.customer_orders", "gold.product_analytics"],
            "gold.revenue_dashboard"
        )
        
        return [
            "bronze_orders", "bronze_customers", "bronze_products",
            "silver_orders", "silver_customers", "silver_products",
            "gold_customer_orders", "gold_product_analytics",
            "gold_revenue_dashboard"
        ]
    
    def test_batch_execution_plan(self, plan_generator, pipes_manager):
        """Test batch execution plan for medallion architecture."""
        pipe_ids = self.setup_medallion_pipeline(pipes_manager)
        
        plan = plan_generator.generate_batch_plan(pipe_ids)
        
        # Should have 4 generations: bronze, silver, gold analytics, gold dashboard
        assert plan.total_generations() == 4
        
        # Generation 0: Bronze layer (all parallel)
        gen0 = plan.get_generation(0)
        assert set(gen0.pipe_ids) == {"bronze_orders", "bronze_customers", "bronze_products"}
        
        # Generation 1: Silver layer (all parallel)
        gen1 = plan.get_generation(1)
        assert set(gen1.pipe_ids) == {"silver_orders", "silver_customers", "silver_products"}
        
        # Generation 2: Gold analytics (parallel)
        gen2 = plan.get_generation(2)
        assert set(gen2.pipe_ids) == {"gold_customer_orders", "gold_product_analytics"}
        
        # Generation 3: Gold dashboard (depends on both analytics)
        gen3 = plan.get_generation(3)
        assert gen3.pipe_ids == ["gold_revenue_dashboard"]
        
        # Max parallelism should be 3 (bronze or silver layer)
        assert plan.max_parallelism() == 3
    
    def test_streaming_execution_plan(self, plan_generator, pipes_manager):
        """Test streaming execution plan for medallion architecture."""
        pipe_ids = self.setup_medallion_pipeline(pipes_manager)
        
        plan = plan_generator.generate_streaming_plan(pipe_ids)
        
        # Should have 4 generations but in reverse order
        assert plan.total_generations() == 4
        
        # Generation 0: Gold dashboard (sink starts first)
        gen0 = plan.get_generation(0)
        assert gen0.pipe_ids == ["gold_revenue_dashboard"]
        
        # Generation 1: Gold analytics (parallel)
        gen1 = plan.get_generation(1)
        assert set(gen1.pipe_ids) == {"gold_customer_orders", "gold_product_analytics"}
        
        # Generation 2: Silver layer (parallel)
        gen2 = plan.get_generation(2)
        assert set(gen2.pipe_ids) == {"silver_orders", "silver_customers", "silver_products"}
        
        # Generation 3: Bronze layer (sources start last)
        gen3 = plan.get_generation(3)
        assert set(gen3.pipe_ids) == {"bronze_orders", "bronze_customers", "bronze_products"}
    
    def test_plan_visualization(self, plan_generator, pipes_manager):
        """Test plan visualization output."""
        pipe_ids = self.setup_medallion_pipeline(pipes_manager)
        
        batch_plan = plan_generator.generate_batch_plan(pipe_ids)
        viz = plan_generator.visualize_plan(batch_plan)
        
        # Should contain key information
        assert "Execution Plan" in viz
        assert "batch" in viz  # Strategy shown in header
        assert "Total Pipes: 9" in viz
        assert "Total Generations: 4" in viz
        assert "Max Parallelism: 3" in viz
        
        # Should show generations
        assert "Generation 0" in viz
        assert "bronze_orders" in viz
        assert "gold_revenue_dashboard" in viz


class TestComplexWorkflows:
    """Test execution strategies on complex workflows."""
    
    def test_fan_out_fan_in(self, plan_generator, pipes_manager):
        """Test fan-out followed by fan-in pattern."""
        # Single source fans out to multiple processors, then fan in to single sink
        register_pipe(pipes_manager, "source", [], "entity.raw")
        register_pipe(pipes_manager, "process_a", ["entity.raw"], "entity.a")
        register_pipe(pipes_manager, "process_b", ["entity.raw"], "entity.b")
        register_pipe(pipes_manager, "process_c", ["entity.raw"], "entity.c")
        register_pipe(
            pipes_manager,
            "sink",
            ["entity.a", "entity.b", "entity.c"],
            "entity.final"
        )
        
        pipe_ids = ["source", "process_a", "process_b", "process_c", "sink"]
        
        # Batch: source -> processors (parallel) -> sink
        batch_plan = plan_generator.generate_batch_plan(pipe_ids)
        assert batch_plan.total_generations() == 3
        assert batch_plan.generations[0].pipe_ids == ["source"]
        assert set(batch_plan.generations[1].pipe_ids) == {"process_a", "process_b", "process_c"}
        assert batch_plan.generations[2].pipe_ids == ["sink"]
        assert batch_plan.max_parallelism() == 3
        
        # Streaming: sink -> processors (parallel) -> source
        streaming_plan = plan_generator.generate_streaming_plan(pipe_ids)
        assert streaming_plan.total_generations() == 3
        assert streaming_plan.generations[0].pipe_ids == ["sink"]
        assert set(streaming_plan.generations[1].pipe_ids) == {"process_a", "process_b", "process_c"}
        assert streaming_plan.generations[2].pipe_ids == ["source"]
    
    def test_multiple_independent_chains(self, plan_generator, pipes_manager):
        """Test multiple independent processing chains."""
        # Chain 1: A -> B -> C
        register_pipe(pipes_manager, "chain1_a", [], "entity.chain1.a")
        register_pipe(pipes_manager, "chain1_b", ["entity.chain1.a"], "entity.chain1.b")
        register_pipe(pipes_manager, "chain1_c", ["entity.chain1.b"], "entity.chain1.c")
        
        # Chain 2: X -> Y -> Z
        register_pipe(pipes_manager, "chain2_x", [], "entity.chain2.x")
        register_pipe(pipes_manager, "chain2_y", ["entity.chain2.x"], "entity.chain2.y")
        register_pipe(pipes_manager, "chain2_z", ["entity.chain2.y"], "entity.chain2.z")
        
        pipe_ids = ["chain1_a", "chain1_b", "chain1_c", "chain2_x", "chain2_y", "chain2_z"]
        
        # Batch plan should have 3 generations with 2 pipes each (parallel chains)
        batch_plan = plan_generator.generate_batch_plan(pipe_ids)
        assert batch_plan.total_generations() == 3
        assert set(batch_plan.generations[0].pipe_ids) == {"chain1_a", "chain2_x"}
        assert set(batch_plan.generations[1].pipe_ids) == {"chain1_b", "chain2_y"}
        assert set(batch_plan.generations[2].pipe_ids) == {"chain1_c", "chain2_z"}
        assert batch_plan.max_parallelism() == 2
    
    def test_shared_entity_optimization(self, plan_generator, pipes_manager):
        """Test planning with shared entity reads."""
        # Multiple pipes reading the same source entity
        register_pipe(pipes_manager, "source", [], "entity.shared")
        register_pipe(pipes_manager, "consumer1", ["entity.shared"], "entity.out1")
        register_pipe(pipes_manager, "consumer2", ["entity.shared"], "entity.out2")
        register_pipe(pipes_manager, "consumer3", ["entity.shared"], "entity.out3")
        
        pipe_ids = ["source", "consumer1", "consumer2", "consumer3"]
        
        batch_plan = plan_generator.generate_batch_plan(pipe_ids)
        
        # Should group consumers in same generation (can read shared entity in parallel)
        assert batch_plan.total_generations() == 2
        assert batch_plan.generations[0].pipe_ids == ["source"]
        assert set(batch_plan.generations[1].pipe_ids) == {"consumer1", "consumer2", "consumer3"}
        assert batch_plan.max_parallelism() == 3


class TestStreamingPatterns:
    """Test execution strategies on streaming-specific patterns."""
    
    def test_streaming_checkpoint_order(self, plan_generator, pipes_manager):
        """Test that streaming plan establishes correct checkpoint order."""
        # Linear pipeline: source -> transform -> sink
        register_pipe(pipes_manager, "source", [], "entity.raw")
        register_pipe(pipes_manager, "transform", ["entity.raw"], "entity.clean")
        register_pipe(pipes_manager, "sink", ["entity.clean"], "entity.final")
        
        pipe_ids = ["source", "transform", "sink"]
        
        streaming_plan = plan_generator.generate_streaming_plan(pipe_ids)
        
        # Streaming starts with sink, works backwards to source
        # This ensures downstream checkpoints are established first
        assert streaming_plan.generations[0].pipe_ids == ["sink"]
        assert streaming_plan.generations[1].pipe_ids == ["transform"]
        assert streaming_plan.generations[2].pipe_ids == ["source"]
        
        # Verify metadata
        assert streaming_plan.metadata["checkpoint_order"] == "downstream_first"
        assert streaming_plan.metadata["execution_order"] == "reverse"
    
    def test_streaming_multi_sink(self, plan_generator, pipes_manager):
        """Test streaming with multiple sinks (multiple outputs)."""
        # One source, multiple sinks
        register_pipe(pipes_manager, "source", [], "entity.raw")
        register_pipe(pipes_manager, "sink_db", ["entity.raw"], "entity.db")
        register_pipe(pipes_manager, "sink_warehouse", ["entity.raw"], "entity.warehouse")
        register_pipe(pipes_manager, "sink_lake", ["entity.raw"], "entity.lake")
        
        pipe_ids = ["source", "sink_db", "sink_warehouse", "sink_lake"]
        
        streaming_plan = plan_generator.generate_streaming_plan(pipe_ids)
        
        # All sinks should start in parallel (generation 0)
        assert streaming_plan.total_generations() == 2
        assert set(streaming_plan.generations[0].pipe_ids) == {
            "sink_db", "sink_warehouse", "sink_lake"
        }
        assert streaming_plan.generations[1].pipe_ids == ["source"]


class TestPlanValidation:
    """Test plan validation rules."""
    
    def test_batch_dependency_validation(self, plan_generator, pipes_manager):
        """Test that batch plans validate dependency order."""
        register_pipe(pipes_manager, "pipe1", [], "entity.a")
        register_pipe(pipes_manager, "pipe2", ["entity.a"], "entity.b")
        register_pipe(pipes_manager, "pipe3", ["entity.b"], "entity.c")
        
        pipe_ids = ["pipe1", "pipe2", "pipe3"]
        
        batch_plan = plan_generator.generate_batch_plan(pipe_ids)
        
        # Should pass validation (correct order)
        assert batch_plan.validate() is True
    
    def test_streaming_allows_reverse_order(self, plan_generator, pipes_manager):
        """Test that streaming plans allow consumers before producers."""
        register_pipe(pipes_manager, "source", [], "entity.a")
        register_pipe(pipes_manager, "sink", ["entity.a"], "entity.b")
        
        pipe_ids = ["source", "sink"]
        
        streaming_plan = plan_generator.generate_streaming_plan(pipe_ids)
        
        # Streaming intentionally puts sink before source
        assert streaming_plan.generations[0].pipe_ids == ["sink"]
        assert streaming_plan.generations[1].pipe_ids == ["source"]
        
        # Should still pass validation (streaming allows reverse order)
        assert streaming_plan.validate() is True
    
    def test_plan_summary_accuracy(self, plan_generator, pipes_manager):
        """Test that plan summary contains accurate information."""
        register_pipe(pipes_manager, "pipe1", [], "entity.a")
        register_pipe(pipes_manager, "pipe2", [], "entity.b")
        register_pipe(pipes_manager, "pipe3", ["entity.a", "entity.b"], "entity.c")
        
        pipe_ids = ["pipe1", "pipe2", "pipe3"]
        
        plan = plan_generator.generate_batch_plan(pipe_ids)
        summary = plan.get_summary()
        
        assert summary["strategy"] == "batch"
        assert summary["total_pipes"] == 3
        assert summary["total_generations"] == 2
        assert summary["max_parallelism"] == 2
        assert summary["generation_sizes"] == [2, 1]


class TestCustomStrategy:
    """Test using custom execution strategies."""
    
    def test_custom_batch_strategy(self, plan_generator, pipes_manager, logger_provider):
        """Test using custom batch strategy directly."""
        register_pipe(pipes_manager, "pipe1", [], "entity.a")
        register_pipe(pipes_manager, "pipe2", ["entity.a"], "entity.b")
        
        pipe_ids = ["pipe1", "pipe2"]
        
        # Create custom strategy instance
        custom_strategy = BatchExecutionStrategy(logger_provider)
        
        # Use it with plan generator
        plan = plan_generator.generate_plan(pipe_ids, custom_strategy)
        
        assert plan.strategy == "batch"
        assert plan.total_generations() == 2
    
    def test_custom_streaming_strategy(self, plan_generator, pipes_manager, logger_provider):
        """Test using custom streaming strategy directly."""
        register_pipe(pipes_manager, "pipe1", [], "entity.a")
        register_pipe(pipes_manager, "pipe2", ["entity.a"], "entity.b")
        
        pipe_ids = ["pipe1", "pipe2"]
        
        # Create custom strategy instance
        custom_strategy = StreamingExecutionStrategy(logger_provider)
        
        # Use it with plan generator
        plan = plan_generator.generate_plan(pipe_ids, custom_strategy)
        
        assert plan.strategy == "streaming"
        assert plan.total_generations() == 2
        # Verify reverse order
        assert plan.generations[0].pipe_ids == ["pipe2"]
        assert plan.generations[1].pipe_ids == ["pipe1"]


class TestConfigBasedStrategy:
    """Test config-based execution strategy with batch/streaming mode detection."""
    
    def test_batch_mode_forward_order(self, plan_generator, pipes_manager):
        """Test batch mode uses forward topological order."""
        # Register batch pipes
        pipes_manager.register_pipe(
            "source",
            name="source",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=[],
            output_entity_id="bronze.data",
            output_type="delta"
        )
        pipes_manager.register_pipe(
            "transform",
            name="transform",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=["bronze.data"],
            output_entity_id="silver.data",
            output_type="delta"
        )
        pipes_manager.register_pipe(
            "sink",
            name="sink",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=["silver.data"],
            output_entity_id="gold.data",
            output_type="delta"
        )
        
        pipe_ids = ["source", "transform", "sink"]
        plan = plan_generator.generate_config_based_plan(pipe_ids)
        
        # Batch mode uses forward order
        assert plan.strategy == "config_based"
        assert plan.metadata["detected_mode"] == "batch"
        assert plan.generations[0].pipe_ids == ["source"]
        assert plan.generations[1].pipe_ids == ["transform"]
        assert plan.generations[2].pipe_ids == ["sink"]
    
    def test_streaming_mode_reverse_order(self, plan_generator, pipes_manager):
        """Test streaming mode uses reverse topological order."""
        # Register streaming pipes
        pipes_manager.register_pipe(
            "source",
            name="source",
            execute=lambda: None,
            tags={"processing_mode": "streaming"},
            input_entity_ids=[],
            output_entity_id="stream.input",
            output_type="delta"
        )
        pipes_manager.register_pipe(
            "transform",
            name="transform",
            execute=lambda: None,
            tags={"processing_mode": "streaming"},
            input_entity_ids=["stream.input"],
            output_entity_id="stream.processed",
            output_type="delta"
        )
        pipes_manager.register_pipe(
            "sink",
            name="sink",
            execute=lambda: None,
            tags={"processing_mode": "streaming"},
            input_entity_ids=["stream.processed"],
            output_entity_id="stream.output",
            output_type="delta"
        )
        
        pipe_ids = ["source", "transform", "sink"]
        plan = plan_generator.generate_config_based_plan(pipe_ids)
        
        # Streaming mode uses reverse order
        assert plan.strategy == "config_based"
        assert plan.metadata["detected_mode"] == "streaming"
        assert plan.generations[0].pipe_ids == ["sink"]
        assert plan.generations[1].pipe_ids == ["transform"]
        assert plan.generations[2].pipe_ids == ["source"]
    
    def test_mixed_mode_uses_streaming(self, plan_generator, pipes_manager):
        """Test if any pipe is streaming, entire plan uses streaming order."""
        # Mix of batch and streaming - should detect streaming
        pipes_manager.register_pipe(
            "batch_source",
            name="batch_source",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=[],
            output_entity_id="data.input",
            output_type="delta"
        )
        pipes_manager.register_pipe(
            "streaming_transform",
            name="streaming_transform",
            execute=lambda: None,
            tags={"processing_mode": "streaming"},
            input_entity_ids=["data.input"],
            output_entity_id="data.processed",
            output_type="delta"
        )
        pipes_manager.register_pipe(
            "batch_sink",
            name="batch_sink",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=["data.processed"],
            output_entity_id="data.output",
            output_type="delta"
        )
        
        pipe_ids = ["batch_source", "streaming_transform", "batch_sink"]
        plan = plan_generator.generate_config_based_plan(pipe_ids)
        
        # Should use streaming order because one pipe is streaming
        assert plan.metadata["detected_mode"] == "streaming"
        assert plan.generations[0].pipe_ids == ["batch_sink"]
    
    def test_untagged_defaults_to_batch(self, plan_generator, pipes_manager):
        """Test pipes without processing_mode tags default to batch."""
        # Register pipes without processing_mode tags
        pipes_manager.register_pipe(
            "source",
            name="source",
            execute=lambda: None,
            tags={},  # No processing_mode tag
            input_entity_ids=[],
            output_entity_id="data.input",
            output_type="delta"
        )
        pipes_manager.register_pipe(
            "sink",
            name="sink",
            execute=lambda: None,
            tags={},  # No processing_mode tag
            input_entity_ids=["data.input"],
            output_entity_id="data.output",
            output_type="delta"
        )
        
        pipe_ids = ["source", "sink"]
        plan = plan_generator.generate_config_based_plan(pipe_ids)
        
        # Should default to batch (forward order)
        assert plan.metadata["detected_mode"] == "batch"
        assert plan.generations[0].pipe_ids == ["source"]
        assert plan.generations[1].pipe_ids == ["sink"]
    
    def test_parallel_pipes_batch_mode(self, plan_generator, pipes_manager):
        """Test parallel pipes work correctly in batch mode."""
        # Multiple sources, single sink
        pipes_manager.register_pipe(
            "source1",
            name="source1",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=[],
            output_entity_id="data.source1",
            output_type="delta"
        )
        pipes_manager.register_pipe(
            "source2",
            name="source2",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=[],
            output_entity_id="data.source2",
            output_type="delta"
        )
        pipes_manager.register_pipe(
            "join",
            name="join",
            execute=lambda: None,
            tags={"processing_mode": "batch"},
            input_entity_ids=["data.source1", "data.source2"],
            output_entity_id="data.joined",
            output_type="delta"
        )
        
        pipe_ids = ["source1", "source2", "join"]
        plan = plan_generator.generate_config_based_plan(pipe_ids)
        
        # Batch mode: sources in parallel, then join
        assert plan.metadata["detected_mode"] == "batch"
        assert set(plan.generations[0].pipe_ids) == {"source1", "source2"}
        assert plan.generations[1].pipe_ids == ["join"]
