"""Unit tests for pipe_graph module."""

from collections import defaultdict
from unittest.mock import MagicMock, Mock

import pytest
from kindling.data_pipes import PipeMetadata
from kindling.pipe_graph import (
    GraphCycleError,
    GraphValidationError,
    PipeEdge,
    PipeGraph,
    PipeGraphBuilder,
    PipeNode,
)


class TestPipeNode:
    """Tests for PipeNode dataclass."""

    def test_pipe_node_creation(self):
        """Test creating a pipe node."""
        node = PipeNode(
            pipe_id="pipe1", input_entities=["entity.a", "entity.b"], output_entity="entity.c"
        )

        assert node.pipe_id == "pipe1"
        assert node.input_entities == ["entity.a", "entity.b"]
        assert node.output_entity == "entity.c"
        assert node.metadata is None

    def test_pipe_node_equality(self):
        """Test pipe node equality based on pipe_id."""
        node1 = PipeNode("pipe1", ["a"], "b")
        node2 = PipeNode("pipe1", ["x"], "y")
        node3 = PipeNode("pipe2", ["a"], "b")

        assert node1 == node2  # Same ID
        assert node1 != node3  # Different ID

    def test_pipe_node_hashable(self):
        """Test pipe nodes can be used in sets/dicts."""
        node1 = PipeNode("pipe1", ["a"], "b")
        node2 = PipeNode("pipe1", ["a"], "b")

        node_set = {node1, node2}
        assert len(node_set) == 1  # Duplicates removed


class TestPipeEdge:
    """Tests for PipeEdge dataclass."""

    def test_pipe_edge_creation(self):
        """Test creating a pipe edge."""
        edge = PipeEdge(from_pipe="pipe1", to_pipe="pipe2", entity="entity.x")

        assert edge.from_pipe == "pipe1"
        assert edge.to_pipe == "pipe2"
        assert edge.entity == "entity.x"

    def test_pipe_edge_equality(self):
        """Test edge equality."""
        edge1 = PipeEdge("p1", "p2", "e1")
        edge2 = PipeEdge("p1", "p2", "e1")
        edge3 = PipeEdge("p1", "p2", "e2")

        assert edge1 == edge2
        assert edge1 != edge3

    def test_pipe_edge_hashable(self):
        """Test edges can be used in sets."""
        edge1 = PipeEdge("p1", "p2", "e1")
        edge2 = PipeEdge("p1", "p2", "e1")

        edge_set = {edge1, edge2}
        assert len(edge_set) == 1


class TestPipeGraph:
    """Tests for PipeGraph dataclass."""

    def test_empty_graph(self):
        """Test creating an empty graph."""
        graph = PipeGraph()

        assert len(graph.nodes) == 0
        assert len(graph.edges) == 0
        assert len(graph.entity_producers) == 0
        assert len(graph.entity_consumers) == 0

    def test_add_node(self):
        """Test adding nodes to graph."""
        graph = PipeGraph()
        node = PipeNode("pipe1", ["entity.a"], "entity.b")

        graph.add_node(node)

        assert "pipe1" in graph.nodes
        assert graph.entity_producers["entity.b"] == "pipe1"
        assert "entity.a" in graph.entity_consumers

    def test_add_edge(self):
        """Test adding edges to graph."""
        graph = PipeGraph()
        node1 = PipeNode("pipe1", [], "entity.a")
        node2 = PipeNode("pipe2", ["entity.a"], "entity.b")

        graph.add_node(node1)
        graph.add_node(node2)

        edge = PipeEdge("pipe1", "pipe2", "entity.a")
        graph.add_edge(edge)

        assert edge in graph.edges
        assert "pipe2" in graph.adjacency["pipe1"]
        assert "pipe1" in graph.reverse_adjacency["pipe2"]

    def test_get_dependencies(self):
        """Test getting pipe dependencies."""
        graph = PipeGraph()
        node1 = PipeNode("pipe1", [], "entity.a")
        node2 = PipeNode("pipe2", ["entity.a"], "entity.b")

        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_edge(PipeEdge("pipe1", "pipe2", "entity.a"))

        assert graph.get_dependencies("pipe1") == []
        assert graph.get_dependencies("pipe2") == ["pipe1"]

    def test_get_dependents(self):
        """Test getting pipe dependents."""
        graph = PipeGraph()
        node1 = PipeNode("pipe1", [], "entity.a")
        node2 = PipeNode("pipe2", ["entity.a"], "entity.b")

        graph.add_node(node1)
        graph.add_node(node2)
        graph.add_edge(PipeEdge("pipe1", "pipe2", "entity.a"))

        assert graph.get_dependents("pipe1") == ["pipe2"]
        assert graph.get_dependents("pipe2") == []

    def test_get_root_nodes(self):
        """Test identifying root nodes (no dependencies)."""
        graph = PipeGraph()
        graph.add_node(PipeNode("pipe1", [], "entity.a"))
        graph.add_node(PipeNode("pipe2", ["entity.a"], "entity.b"))
        graph.add_edge(PipeEdge("pipe1", "pipe2", "entity.a"))

        roots = graph.get_root_nodes()
        assert roots == ["pipe1"]

    def test_get_leaf_nodes(self):
        """Test identifying leaf nodes (no dependents)."""
        graph = PipeGraph()
        graph.add_node(PipeNode("pipe1", [], "entity.a"))
        graph.add_node(PipeNode("pipe2", ["entity.a"], "entity.b"))
        graph.add_edge(PipeEdge("pipe1", "pipe2", "entity.a"))

        leaves = graph.get_leaf_nodes()
        assert leaves == ["pipe2"]

    def test_get_shared_inputs(self):
        """Test identifying entities read by multiple pipes."""
        graph = PipeGraph()
        graph.add_node(PipeNode("pipe1", [], "entity.a"))
        graph.add_node(PipeNode("pipe2", ["entity.a"], "entity.b"))
        graph.add_node(PipeNode("pipe3", ["entity.a"], "entity.c"))

        shared = graph.get_shared_inputs()
        assert "entity.a" in shared
        assert set(shared["entity.a"]) == {"pipe2", "pipe3"}


class TestPipeGraphBuilder:
    """Tests for PipeGraphBuilder."""

    @pytest.fixture
    def mock_registry(self):
        """Create a mock pipe registry."""
        registry = Mock()
        registry.pipe_defs = {}

        def get_pipe(pipe_id):
            return registry.pipe_defs.get(pipe_id)

        registry.get_pipe_definition = Mock(side_effect=get_pipe)
        return registry

    @pytest.fixture
    def mock_logger_provider(self):
        """Create a mock logger provider."""
        provider = Mock()
        logger = Mock()
        logger.debug = Mock()
        logger.info = Mock()
        logger.error = Mock()
        provider.get_logger = Mock(return_value=logger)
        return provider

    @pytest.fixture
    def builder(self, mock_registry, mock_logger_provider):
        """Create a graph builder with mocks."""
        return PipeGraphBuilder(mock_registry, mock_logger_provider)

    def create_pipe_metadata(self, pipe_id, input_entities, output_entity):
        """Helper to create pipe metadata."""
        return PipeMetadata(
            pipeid=pipe_id,
            name=pipe_id,
            execute=lambda: None,
            tags={},
            input_entity_ids=input_entities,
            output_entity_id=output_entity,
            output_type="delta",
        )

    def test_build_simple_graph(self, builder, mock_registry):
        """Test building a simple linear graph."""
        # pipe1 -> pipe2 -> pipe3
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.b"], "entity.c"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3"])

        assert len(graph.nodes) == 3
        assert len(graph.edges) == 2
        assert graph.get_root_nodes() == ["pipe1"]
        assert graph.get_leaf_nodes() == ["pipe3"]

    def test_build_parallel_graph(self, builder, mock_registry):
        """Test building a graph with parallel pipes."""
        # pipe1 -> pipe2
        #       -> pipe3
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.a"], "entity.c"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3"])

        assert len(graph.nodes) == 3
        assert len(graph.edges) == 2
        assert graph.get_dependents("pipe1") == ["pipe2", "pipe3"]

    def test_build_diamond_graph(self, builder, mock_registry):
        """Test building a diamond-shaped graph."""
        # pipe1 -> pipe2 -> pipe4
        #       -> pipe3 ->
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.a"], "entity.c"),
            "pipe4": self.create_pipe_metadata("pipe4", ["entity.b", "entity.c"], "entity.d"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3", "pipe4"])

        assert len(graph.nodes) == 4
        assert len(graph.edges) == 4  # 1->2, 1->3, 2->4, 3->4
        assert graph.get_root_nodes() == ["pipe1"]
        assert graph.get_leaf_nodes() == ["pipe4"]
        assert set(graph.get_dependencies("pipe4")) == {"pipe2", "pipe3"}

    def test_detect_simple_cycle(self, builder, mock_registry):
        """Test detecting a simple 2-node cycle."""
        # pipe1 -> pipe2 -> pipe1 (cycle)
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", ["entity.b"], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
        }

        with pytest.raises(GraphCycleError) as exc_info:
            builder.build_graph(["pipe1", "pipe2"])

        assert "cycle" in str(exc_info.value).lower()
        assert "pipe1" in str(exc_info.value)
        assert "pipe2" in str(exc_info.value)

    def test_detect_self_cycle(self, builder, mock_registry):
        """Test detecting a self-referencing cycle."""
        # pipe1 -> pipe1 (self-loop via entity)
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", ["entity.a"], "entity.a"),
        }

        with pytest.raises(GraphCycleError) as exc_info:
            builder.build_graph(["pipe1"])

        assert "pipe1" in str(exc_info.value)

    def test_detect_complex_cycle(self, builder, mock_registry):
        """Test detecting a cycle in a complex graph."""
        # pipe1 -> pipe2 -> pipe3 -> pipe4 -> pipe2 (cycle)
        # Note: pipe4 produces entity.b which pipe2 consumes, creating the cycle
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a", "entity.d"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.b"], "entity.c"),
            "pipe4": self.create_pipe_metadata("pipe4", ["entity.c"], "entity.d"),
        }

        with pytest.raises(GraphCycleError) as exc_info:
            builder.build_graph(["pipe1", "pipe2", "pipe3", "pipe4"])

        # Cycle should be detected (pipe2 -> pipe3 -> pipe4 -> pipe2 via entity.d)
        error_msg = str(exc_info.value)
        assert "cycle" in error_msg.lower()
        # At least two pipes from the cycle should be mentioned
        pipes_in_error = sum([1 for p in ["pipe2", "pipe3", "pipe4"] if p in error_msg])
        assert pipes_in_error >= 2

    def test_topological_sort_linear(self, builder, mock_registry):
        """Test topological sort on linear graph."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.b"], "entity.c"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3"])
        sorted_pipes = builder.topological_sort(graph)

        assert sorted_pipes == ["pipe1", "pipe2", "pipe3"]

    def test_topological_sort_diamond(self, builder, mock_registry):
        """Test topological sort on diamond graph."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.a"], "entity.c"),
            "pipe4": self.create_pipe_metadata("pipe4", ["entity.b", "entity.c"], "entity.d"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3", "pipe4"])
        sorted_pipes = builder.topological_sort(graph)

        # pipe1 must be first, pipe4 must be last
        assert sorted_pipes[0] == "pipe1"
        assert sorted_pipes[3] == "pipe4"
        # pipe2 and pipe3 can be in any order but both before pipe4
        assert set(sorted_pipes[1:3]) == {"pipe2", "pipe3"}

    def test_get_generations_linear(self, builder, mock_registry):
        """Test generation grouping for linear graph."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.b"], "entity.c"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3"])
        generations = builder.get_generations(graph)

        assert len(generations) == 3
        assert generations[0] == ["pipe1"]
        assert generations[1] == ["pipe2"]
        assert generations[2] == ["pipe3"]

    def test_get_generations_parallel(self, builder, mock_registry):
        """Test generation grouping for parallel pipes."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.a"], "entity.c"),
            "pipe4": self.create_pipe_metadata("pipe4", ["entity.a"], "entity.d"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3", "pipe4"])
        generations = builder.get_generations(graph)

        assert len(generations) == 2
        assert generations[0] == ["pipe1"]
        # pipe2, pipe3, pipe4 can run in parallel
        assert set(generations[1]) == {"pipe2", "pipe3", "pipe4"}

    def test_get_generations_diamond(self, builder, mock_registry):
        """Test generation grouping for diamond graph."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.a"], "entity.c"),
            "pipe4": self.create_pipe_metadata("pipe4", ["entity.b", "entity.c"], "entity.d"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3", "pipe4"])
        generations = builder.get_generations(graph)

        assert len(generations) == 3
        assert generations[0] == ["pipe1"]
        assert set(generations[1]) == {"pipe2", "pipe3"}
        assert generations[2] == ["pipe4"]

    def test_reverse_topological_sort(self, builder, mock_registry):
        """Test reverse topological sort (sinks first)."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.b"], "entity.c"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3"])
        reversed_sort = builder.reverse_topological_sort(graph)

        assert reversed_sort == ["pipe3", "pipe2", "pipe1"]

    def test_reverse_generations(self, builder, mock_registry):
        """Test reverse generation grouping (for streaming)."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.b"], "entity.c"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3"])
        reverse_gens = builder.get_reverse_generations(graph)

        assert len(reverse_gens) == 3
        assert reverse_gens[0] == ["pipe3"]
        assert reverse_gens[1] == ["pipe2"]
        assert reverse_gens[2] == ["pipe1"]

    def test_visualize_mermaid(self, builder, mock_registry):
        """Test Mermaid diagram generation."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
        }

        graph = builder.build_graph(["pipe1", "pipe2"])
        mermaid = builder.visualize_mermaid(graph)

        assert "graph TD" in mermaid
        assert "pipe1" in mermaid
        assert "pipe2" in mermaid
        assert "-->" in mermaid

    def test_get_graph_stats(self, builder, mock_registry):
        """Test graph statistics calculation."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.a"], "entity.c"),
            "pipe4": self.create_pipe_metadata("pipe4", ["entity.b", "entity.c"], "entity.d"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3", "pipe4"])
        stats = builder.get_graph_stats(graph)

        assert stats["node_count"] == 4
        assert stats["edge_count"] == 4
        assert stats["root_count"] == 1
        assert stats["leaf_count"] == 1
        assert stats["generation_count"] == 3
        assert stats["max_parallelism"] == 2  # pipe2 and pipe3 together
        assert stats["shared_entities"] == 1  # entity.a read by 2 pipes

    def test_validation_missing_pipe(self, builder, mock_registry):
        """Test graph validation detects missing pipes."""
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
        }

        with pytest.raises(GraphValidationError) as exc_info:
            builder.build_graph(["pipe1", "pipe2"])

        assert "pipe2" in str(exc_info.value)
        assert "not found" in str(exc_info.value).lower()

    def test_external_entity_sources(self, builder, mock_registry):
        """Test handling pipes that read from external sources."""
        # pipe1 reads from external entity (not produced by any pipe)
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", ["external.source"], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
        }

        # Should not raise an error - external sources are OK
        graph = builder.build_graph(["pipe1", "pipe2"])

        assert len(graph.nodes) == 2
        assert len(graph.edges) == 1  # Only pipe1 -> pipe2
        assert graph.get_root_nodes() == ["pipe1"]

    def test_complex_dependency_graph(self, builder, mock_registry):
        """Test building a complex multi-path graph."""
        # Build a more realistic graph
        #     pipe1 --> pipe2 --> pipe4 --> pipe6
        #           --> pipe3 --> pipe5 -->
        mock_registry.pipe_defs = {
            "pipe1": self.create_pipe_metadata("pipe1", [], "entity.a"),
            "pipe2": self.create_pipe_metadata("pipe2", ["entity.a"], "entity.b"),
            "pipe3": self.create_pipe_metadata("pipe3", ["entity.a"], "entity.c"),
            "pipe4": self.create_pipe_metadata("pipe4", ["entity.b"], "entity.d"),
            "pipe5": self.create_pipe_metadata("pipe5", ["entity.c"], "entity.e"),
            "pipe6": self.create_pipe_metadata("pipe6", ["entity.d", "entity.e"], "entity.f"),
        }

        graph = builder.build_graph(["pipe1", "pipe2", "pipe3", "pipe4", "pipe5", "pipe6"])
        generations = builder.get_generations(graph)

        assert len(generations) == 4
        assert generations[0] == ["pipe1"]
        assert set(generations[1]) == {"pipe2", "pipe3"}
        assert set(generations[2]) == {"pipe4", "pipe5"}
        assert generations[3] == ["pipe6"]

        stats = builder.get_graph_stats(graph)
        assert stats["max_parallelism"] == 2
        assert stats["generation_count"] == 4
