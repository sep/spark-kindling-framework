"""Unit tests for cache_optimizer module."""

from kindling.cache_optimizer import CacheOptimizer
from kindling.pipe_graph import PipeGraph, PipeNode


def _build_shared_input_graph() -> PipeGraph:
    graph = PipeGraph()
    graph.add_node(PipeNode("pipe_source", [], "entity.shared"))
    graph.add_node(PipeNode("pipe_consumer_a", ["entity.shared"], "entity.a"))
    graph.add_node(PipeNode("pipe_consumer_b", ["entity.shared"], "entity.b"))
    graph.add_node(PipeNode("pipe_unique", ["entity.unique"], "entity.c"))
    return graph


class TestCacheOptimizer:
    """Tests for cache recommendation behavior."""

    def test_recommend_shared_entities(self):
        graph = _build_shared_input_graph()
        optimizer = CacheOptimizer()

        recommendations = optimizer.recommend(graph)

        assert set(recommendations.keys()) == {"entity.shared"}
        rec = recommendations["entity.shared"]
        assert rec.entity_id == "entity.shared"
        assert rec.consumer_pipes == ["pipe_consumer_a", "pipe_consumer_b"]
        assert rec.cache_level == "MEMORY_AND_DISK"
        assert rec.reason == "shared_input"

    def test_recommend_filters_to_pipe_scope(self):
        graph = _build_shared_input_graph()
        optimizer = CacheOptimizer()

        no_recommendations = optimizer.recommend(
            graph, pipe_ids=["pipe_source", "pipe_consumer_a"]
        )
        assert no_recommendations == {}

        scoped_recommendations = optimizer.recommend(
            graph, pipe_ids=["pipe_source", "pipe_consumer_a", "pipe_consumer_b"]
        )
        assert set(scoped_recommendations.keys()) == {"entity.shared"}

    def test_as_level_map(self):
        graph = _build_shared_input_graph()
        optimizer = CacheOptimizer(default_cache_level="DISK_ONLY")

        recommendations = optimizer.recommend(graph)
        levels = optimizer.as_level_map(recommendations)

        assert levels == {"entity.shared": "DISK_ONLY"}
