"""Cache recommendation helpers for DAG execution plans.

This module identifies shared input entities in a pipe graph and produces
stable cache recommendations that can be consumed by execution planners and
executors.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

from kindling.pipe_graph import PipeGraph

DEFAULT_CACHE_LEVEL = "MEMORY_AND_DISK"


@dataclass(frozen=True)
class CacheRecommendation:
    """Cache recommendation for a shared input entity."""

    entity_id: str
    consumer_pipes: List[str]
    cache_level: str = DEFAULT_CACHE_LEVEL
    reason: str = "shared_input"


class CacheOptimizer:
    """Identify and normalize cache recommendations for DAG execution."""

    def __init__(self, default_cache_level: str = DEFAULT_CACHE_LEVEL):
        self.default_cache_level = default_cache_level

    def recommend(
        self,
        graph: PipeGraph,
        pipe_ids: Optional[List[str]] = None,
    ) -> Dict[str, CacheRecommendation]:
        """Return cache recommendations for shared input entities.

        Args:
            graph: Pipe dependency graph.
            pipe_ids: Optional subset of pipe IDs in scope for execution.

        Returns:
            Map of entity_id -> CacheRecommendation.
        """
        scoped_pipes = set(pipe_ids) if pipe_ids else None
        try:
            shared_inputs = graph.get_shared_inputs()
        except Exception:
            return {}

        if not isinstance(shared_inputs, dict):
            return {}
        recommendations: Dict[str, CacheRecommendation] = {}

        for entity_id, consumers in shared_inputs.items():
            scoped_consumers = sorted(
                consumer
                for consumer in consumers
                if scoped_pipes is None or consumer in scoped_pipes
            )
            if len(scoped_consumers) < 2:
                continue

            recommendations[entity_id] = CacheRecommendation(
                entity_id=entity_id,
                consumer_pipes=scoped_consumers,
                cache_level=self.default_cache_level,
            )

        return recommendations

    @staticmethod
    def as_level_map(
        recommendations: Dict[str, CacheRecommendation],
    ) -> Dict[str, str]:
        """Convert recommendations into `entity_id -> cache_level` map."""
        return {
            entity_id: recommendation.cache_level
            for entity_id, recommendation in recommendations.items()
        }
