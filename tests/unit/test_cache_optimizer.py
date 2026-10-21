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

        no_recommendations = optimizer.recommend(graph, pipe_ids=["pipe_source", "pipe_consumer_a"])
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

    def test_entity_read_before_and_after_its_write_is_not_shared(self):
        """A producer that reads its own output sees the pre-write state;
        its downstream consumers see the post-write state. One cached frame
        would serve the stale pre-write data to the downstream reader."""
        graph = PipeGraph()
        graph.add_node(PipeNode("pipe_self", ["entity.src", "entity.a"], "entity.a"))
        graph.add_node(PipeNode("pipe_down", ["entity.a"], "entity.b"))

        assert CacheOptimizer().recommend(graph) == {}

    def test_scd2_companion_read_by_its_base_writer_is_not_shared(self):
        graph = PipeGraph()
        graph.add_node(
            PipeNode("detect_changes", ["entity.src", "silver.dim.current"], "silver.dim")
        )
        graph.add_node(PipeNode("build_gold", ["silver.dim.current"], "gold.dim"))
        graph.entity_aliases["silver.dim.current"] = "silver.dim"

        assert CacheOptimizer().recommend(graph) == {}

    def test_shared_companion_read_after_its_base_write_is_recommended(self):
        graph = PipeGraph()
        graph.add_node(PipeNode("load", ["entity.src"], "silver.dim"))
        graph.add_node(PipeNode("gold_a", ["silver.dim.current"], "gold.a"))
        graph.add_node(PipeNode("gold_b", ["silver.dim.current"], "gold.b"))
        graph.entity_aliases["silver.dim.current"] = "silver.dim"

        assert set(CacheOptimizer().recommend(graph)) == {"silver.dim.current"}
