"""Integration tests for pipe_graph module with real pipe definitions."""

from unittest.mock import Mock

import pytest
from kindling.data_pipes import DataPipesManager, PipeMetadata
from kindling.pipe_graph import GraphCycleError, GraphValidationError, PipeGraphBuilder


class TestPipeGraphIntegration:
    """Integration tests with real DataPipesManager."""

    @pytest.fixture
    def logger_provider(self):
        """Create a mock logger provider."""
        provider = Mock()
        logger = Mock()
        logger.debug = Mock()
        logger.info = Mock()
        logger.error = Mock()
        provider.get_logger = Mock(return_value=logger)
        return provider

    @pytest.fixture
    def pipe_registry(self, logger_provider):
        """Create a real DataPipesManager."""
        return DataPipesManager(logger_provider)

    @pytest.fixture
    def builder(self, pipe_registry, logger_provider):
        """Create a graph builder with real registry."""
        return PipeGraphBuilder(pipe_registry, logger_provider)

    def register_pipe(self, registry, pipe_id, input_entities, output_entity):
        """Helper to register a pipe."""
        registry.register_pipe(
            pipe_id,
            name=pipe_id,
            execute=lambda: None,
            tags={},
            input_entity_ids=input_entities,
            output_entity_id=output_entity,
            output_type="delta",
        )

    def test_medallion_architecture(self, builder, pipe_registry):
        """Test graph for typical medallion architecture (bronze -> silver -> gold)."""
        # Register pipes for medallion architecture
        self.register_pipe(pipe_registry, "ingest_orders", [], "bronze.orders")
        self.register_pipe(pipe_registry, "ingest_customers", [], "bronze.customers")
        self.register_pipe(pipe_registry, "ingest_products", [], "bronze.products")

        self.register_pipe(pipe_registry, "clean_orders", ["bronze.orders"], "silver.orders")
        self.register_pipe(
            pipe_registry, "clean_customers", ["bronze.customers"], "silver.customers"
        )
        self.register_pipe(pipe_registry, "clean_products", ["bronze.products"], "silver.products")

        self.register_pipe(
            pipe_registry,
            "enrich_orders",
            ["silver.orders", "silver.customers", "silver.products"],
            "gold.order_analytics",
        )

        # Build graph
        pipe_ids = [
            "ingest_orders",
            "ingest_customers",
            "ingest_products",
            "clean_orders",
            "clean_customers",
            "clean_products",
            "enrich_orders",
        ]
        graph = builder.build_graph(pipe_ids)

        # Verify graph structure
        assert len(graph.nodes) == 7
        assert len(graph.edges) == 6  # 3 bronze->silver + 3 silver->gold

        # Verify generations
        generations = builder.get_generations(graph)
        assert len(generations) == 3

        # Generation 1: All ingests can run in parallel
        assert set(generations[0]) == {"ingest_orders", "ingest_customers", "ingest_products"}

        # Generation 2: All cleaning can run in parallel
        assert set(generations[1]) == {"clean_orders", "clean_customers", "clean_products"}

        # Generation 3: Enrichment needs all silver tables
        assert generations[2] == ["enrich_orders"]

        # Verify cache opportunities
        shared = graph.get_shared_inputs()
        assert len(shared) == 0  # Each entity consumed once in this example

        # Verify stats
        stats = builder.get_graph_stats(graph)
        assert stats["max_parallelism"] == 3  # 3 pipes can run in parallel
        assert stats["generation_count"] == 3
        assert stats["root_count"] == 3
        assert stats["leaf_count"] == 1

    def test_fan_in_pattern(self, builder, pipe_registry):
        """Test graph with fan-in pattern (multiple sources, one sink)."""
        # Multiple sources flowing into one analytics table
        self.register_pipe(pipe_registry, "source_a", [], "data.a")
        self.register_pipe(pipe_registry, "source_b", [], "data.b")
        self.register_pipe(pipe_registry, "source_c", [], "data.c")
        self.register_pipe(
            pipe_registry, "combine", ["data.a", "data.b", "data.c"], "gold.combined"
        )

        graph = builder.build_graph(["source_a", "source_b", "source_c", "combine"])

        # All sources can run in parallel, then combine
        generations = builder.get_generations(graph)
        assert len(generations) == 2
        assert len(generations[0]) == 3  # All sources
        assert generations[1] == ["combine"]

        # Verify dependencies
        assert set(graph.get_dependencies("combine")) == {"source_a", "source_b", "source_c"}

    def test_fan_out_pattern(self, builder, pipe_registry):
        """Test graph with fan-out pattern (one source, multiple consumers)."""
        # One source feeding multiple downstream pipes
        self.register_pipe(pipe_registry, "source", [], "data.raw")
        self.register_pipe(pipe_registry, "transform_a", ["data.raw"], "data.view_a")
        self.register_pipe(pipe_registry, "transform_b", ["data.raw"], "data.view_b")
        self.register_pipe(pipe_registry, "transform_c", ["data.raw"], "data.view_c")

        graph = builder.build_graph(["source", "transform_a", "transform_b", "transform_c"])

        # Source first, then all transforms in parallel
        generations = builder.get_generations(graph)
        assert len(generations) == 2
        assert generations[0] == ["source"]
        assert len(generations[1]) == 3  # All transforms

        # Verify shared input (cache opportunity)
        shared = graph.get_shared_inputs()
        assert "data.raw" in shared
        assert len(shared["data.raw"]) == 3  # Read by 3 pipes

        stats = builder.get_graph_stats(graph)
        assert stats["shared_entities"] == 1
        assert stats["total_cache_opportunities"] == 3

    def test_complex_workflow(self, builder, pipe_registry):
        """Test a complex real-world workflow with multiple patterns."""
        # Simulate a complex ETL workflow
        # Layer 1: Multiple raw sources
        self.register_pipe(pipe_registry, "raw_sales", [], "bronze.sales")
        self.register_pipe(pipe_registry, "raw_inventory", [], "bronze.inventory")
        self.register_pipe(pipe_registry, "raw_customers", [], "bronze.customers")

        # Layer 2: Cleaning and standardization
        self.register_pipe(pipe_registry, "clean_sales", ["bronze.sales"], "silver.sales")
        self.register_pipe(
            pipe_registry, "clean_inventory", ["bronze.inventory"], "silver.inventory"
        )
        self.register_pipe(
            pipe_registry, "clean_customers", ["bronze.customers"], "silver.customers"
        )

        # Layer 3a: Derived metrics
        self.register_pipe(
            pipe_registry,
            "sales_metrics",
            ["silver.sales", "silver.customers"],
            "gold.sales_metrics",
        )

        # Layer 3b: Inventory analysis (depends on both inventory and sales)
        self.register_pipe(
            pipe_registry,
            "inventory_analysis",
            ["silver.inventory", "silver.sales"],
            "gold.inventory_analysis",
        )

        # Layer 4: Executive dashboard (depends on all gold tables)
        self.register_pipe(
            pipe_registry,
            "exec_dashboard",
            ["gold.sales_metrics", "gold.inventory_analysis"],
            "gold.executive_dashboard",
        )

        pipe_ids = [
            "raw_sales",
            "raw_inventory",
            "raw_customers",
            "clean_sales",
            "clean_inventory",
            "clean_customers",
            "sales_metrics",
            "inventory_analysis",
            "exec_dashboard",
        ]

        graph = builder.build_graph(pipe_ids)

        # Verify structure
        assert len(graph.nodes) == 9
        generations = builder.get_generations(graph)
        assert len(generations) == 4

        # Verify generation structure
        assert len(generations[0]) == 3  # 3 raw ingests
        assert len(generations[1]) == 3  # 3 cleanings
        assert len(generations[2]) == 2  # 2 analytics
        assert len(generations[3]) == 1  # 1 dashboard

        # Verify shared inputs (silver.sales used by both analytics pipes)
        shared = graph.get_shared_inputs()
        assert "silver.sales" in shared
        assert len(shared["silver.sales"]) == 2

        # Topological sort should respect dependencies
        topo_order = builder.topological_sort(graph)
        assert topo_order.index("raw_sales") < topo_order.index("clean_sales")
        assert topo_order.index("clean_sales") < topo_order.index("sales_metrics")
        assert topo_order.index("sales_metrics") < topo_order.index("exec_dashboard")

    def test_external_sources_handling(self, builder, pipe_registry):
        """Test that external data sources (not produced by any pipe) are handled correctly."""
        # Pipes that read from external sources
        self.register_pipe(pipe_registry, "read_external", ["external.data"], "processed.data")
        self.register_pipe(pipe_registry, "downstream", ["processed.data"], "final.output")

        graph = builder.build_graph(["read_external", "downstream"])

        # Should not error on external sources
        assert len(graph.nodes) == 2
        assert len(graph.edges) == 1

        # read_external is a root node (no dependencies in this graph)
        assert graph.get_root_nodes() == ["read_external"]

        generations = builder.get_generations(graph)
        assert len(generations) == 2

    def test_cycle_detection_in_real_workflow(self, builder, pipe_registry):
        """Test that cycles are detected in misconfigured real workflows."""
        # Accidental cycle: pipe A depends on B, B depends on C, C depends on A
        self.register_pipe(pipe_registry, "pipe_a", ["entity.c"], "entity.a")
        self.register_pipe(pipe_registry, "pipe_b", ["entity.a"], "entity.b")
        self.register_pipe(pipe_registry, "pipe_c", ["entity.b"], "entity.c")

        with pytest.raises(GraphCycleError) as exc_info:
            builder.build_graph(["pipe_a", "pipe_b", "pipe_c"])

        # Error message should be helpful
        error_msg = str(exc_info.value)
        assert "cycle" in error_msg.lower()
        assert any(pipe in error_msg for pipe in ["pipe_a", "pipe_b", "pipe_c"])

    def test_mermaid_visualization_with_real_pipes(self, builder, pipe_registry):
        """Test Mermaid diagram generation with real pipe names."""
        self.register_pipe(pipe_registry, "bronze_orders", [], "bronze.orders")
        self.register_pipe(pipe_registry, "silver_orders", ["bronze.orders"], "silver.orders")
        self.register_pipe(pipe_registry, "gold_analytics", ["silver.orders"], "gold.analytics")

        graph = builder.build_graph(["bronze_orders", "silver_orders", "gold_analytics"])
        mermaid = builder.visualize_mermaid(graph)

        # Verify Mermaid syntax
        assert mermaid.startswith("graph TD")
        assert "bronze_orders" in mermaid
        assert "silver_orders" in mermaid
        assert "gold_analytics" in mermaid
        assert "-->" in mermaid

        # Should have entity labels on edges
        assert "orders" in mermaid or "analytics" in mermaid

    def test_reverse_generations_for_streaming(self, builder, pipe_registry):
        """Test reverse generation ordering for streaming workloads."""
        # In streaming, we want to start sinks first, then work backwards
        self.register_pipe(pipe_registry, "source", [], "data.raw")
        self.register_pipe(pipe_registry, "transform", ["data.raw"], "data.clean")
        self.register_pipe(pipe_registry, "sink", ["data.clean"], "data.final")

        graph = builder.build_graph(["source", "transform", "sink"])

        # Forward generations (batch mode)
        forward_gens = builder.get_generations(graph)
        assert forward_gens == [["source"], ["transform"], ["sink"]]

        # Reverse generations (streaming mode)
        reverse_gens = builder.get_reverse_generations(graph)
        assert reverse_gens == [["sink"], ["transform"], ["source"]]

        # Reverse topological sort
        reverse_topo = builder.reverse_topological_sort(graph)
        assert reverse_topo == ["sink", "transform", "source"]
