"""Execution strategies for data pipe orchestration.

This module provides strategy patterns for different execution modes:
- BatchExecutionStrategy: Forward topological order (sources → sinks)
- StreamingExecutionStrategy: Reverse topological order (sinks → sources)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from kindling.cache_optimizer import CacheOptimizer
from injector import inject
from kindling.data_pipes import DataPipesRegistry
from kindling.pipe_graph import PipeGraph, PipeGraphBuilder
from kindling.spark_log_provider import PythonLoggerProvider


@dataclass
class Generation:
    """Represents a generation of pipes that can execute in parallel.

    Attributes:
        number: Generation number (0-indexed)
        pipe_ids: List of pipe IDs in this generation
        dependencies: Pipe IDs that must complete before this generation starts
    """

    number: int
    pipe_ids: List[str]
    dependencies: List[str] = field(default_factory=list)

    def __len__(self):
        return len(self.pipe_ids)

    def __iter__(self):
        return iter(self.pipe_ids)

    def __contains__(self, pipe_id: str):
        return pipe_id in self.pipe_ids


@dataclass
class ExecutionPlan:
    """Execution plan for a set of data pipes.

    Attributes:
        pipe_ids: Original list of pipe IDs to execute
        generations: Ordered list of generations
        graph: The dependency graph
        strategy: Name of the strategy used
        metadata: Additional metadata about the plan
    """

    pipe_ids: List[str]
    generations: List[Generation]
    graph: PipeGraph
    strategy: str
    metadata: Dict[str, any] = field(default_factory=dict)

    def total_pipes(self) -> int:
        """Get total number of pipes in the plan."""
        return len(self.pipe_ids)

    def total_generations(self) -> int:
        """Get total number of generations."""
        return len(self.generations)

    def max_parallelism(self) -> int:
        """Get maximum number of pipes that can run in parallel."""
        return max(len(gen) for gen in self.generations) if self.generations else 0

    def get_generation(self, number: int) -> Optional[Generation]:
        """Get generation by number."""
        if 0 <= number < len(self.generations):
            return self.generations[number]
        return None

    def find_pipe_generation(self, pipe_id: str) -> Optional[int]:
        """Find which generation a pipe belongs to."""
        for gen in self.generations:
            if pipe_id in gen:
                return gen.number
        return None

    def validate(self) -> bool:
        """Validate the execution plan.

        Returns:
            True if plan is valid

        Raises:
            ValueError: If plan is invalid
        """
        # Check all pipes are accounted for
        pipes_in_plan = set()
        for gen in self.generations:
            pipes_in_plan.update(gen.pipe_ids)

        if pipes_in_plan != set(self.pipe_ids):
            missing = set(self.pipe_ids) - pipes_in_plan
            extra = pipes_in_plan - set(self.pipe_ids)
            raise ValueError(f"Plan validation failed. Missing: {missing}, Extra: {extra}")

        # Check generations are ordered correctly (dependencies satisfied)
        # For batch: dependencies must execute BEFORE consumers
        # For streaming: consumers can execute BEFORE producers (reverse order)
        is_streaming = self.strategy == "streaming" or (
            self.strategy == "config_based" and self.metadata.get("detected_mode") == "streaming"
        )

        if not is_streaming:
            # Batch mode: validate forward dependencies
            executed = set()
            for gen in self.generations:
                # All dependencies should have been executed in previous generations
                for pipe_id in gen.pipe_ids:
                    deps = self.graph.get_dependencies(pipe_id)
                    unsatisfied = [d for d in deps if d not in executed and d in self.pipe_ids]
                    if unsatisfied:
                        raise ValueError(
                            f"Dependencies not satisfied for {pipe_id} in generation {gen.number}. "
                            f"Unsatisfied: {unsatisfied}"
                        )
                executed.update(gen.pipe_ids)
        else:
            # Streaming mode: validate reverse dependencies
            # In streaming, we start from sinks and work backwards to sources
            # So consumers can be scheduled before their producers
            # We just need to verify all pipes are included (already done above)
            pass

        return True

    def get_summary(self) -> Dict[str, any]:
        """Get summary statistics for the plan."""
        return {
            "strategy": self.strategy,
            "total_pipes": self.total_pipes(),
            "total_generations": self.total_generations(),
            "max_parallelism": self.max_parallelism(),
            "avg_pipes_per_generation": (
                round(self.total_pipes() / self.total_generations(), 2)
                if self.total_generations() > 0
                else 0
            ),
            "generation_sizes": [len(gen) for gen in self.generations],
            **self.metadata,
        }


class ExecutionStrategy(ABC):
    """Abstract base class for execution strategies.

    An execution strategy determines the order in which pipes should execute
    based on their dependencies, and groups them into generations for
    parallel execution.
    """

    def __init__(self, logger_provider: PythonLoggerProvider):
        """Initialize the strategy.

        Args:
            logger_provider: Logger provider for diagnostics
        """
        self.logger = logger_provider.get_logger(self.__class__.__name__)

    @abstractmethod
    def plan(self, graph: PipeGraph, pipe_ids: List[str]) -> ExecutionPlan:
        """Generate execution plan from graph.

        Args:
            graph: The dependency graph
            pipe_ids: List of pipe IDs to execute

        Returns:
            ExecutionPlan with ordered generations
        """
        pass

    @abstractmethod
    def get_strategy_name(self) -> str:
        """Get the name of this strategy."""
        pass


class BatchExecutionStrategy(ExecutionStrategy):
    """Forward topological sort strategy for batch processing.

    Executes pipes in forward topological order:
    - Sources (no dependencies) execute first
    - Sinks (no dependents) execute last
    - Pipes grouped into generations for parallel execution

    Use case: Batch ETL pipelines where data flows from sources to sinks.
    """

    def get_strategy_name(self) -> str:
        return "batch"

    def plan(self, graph: PipeGraph, pipe_ids: List[str]) -> ExecutionPlan:
        """Generate batch execution plan.

        Args:
            graph: The dependency graph
            pipe_ids: List of pipe IDs to execute

        Returns:
            ExecutionPlan with forward topological ordering
        """
        self.logger.info(f"Generating batch execution plan for {len(pipe_ids)} pipes")

        # Group pipes into generations
        generations_list = []
        processed = set()
        generation_num = 0

        while len(processed) < len(pipe_ids):
            # Find pipes whose dependencies are all satisfied
            current_gen_pipes = [
                pipe_id
                for pipe_id in pipe_ids
                if pipe_id not in processed
                and all(
                    dep in processed or dep not in pipe_ids
                    for dep in graph.get_dependencies(pipe_id)
                )
            ]

            if not current_gen_pipes:
                unprocessed = set(pipe_ids) - processed
                raise ValueError(
                    f"Cannot determine next generation. "
                    f"Unprocessed pipes: {unprocessed}. "
                    f"This may indicate a cycle or validation error."
                )

            # Collect dependencies for this generation
            gen_dependencies = set()
            for pipe_id in current_gen_pipes:
                deps = graph.get_dependencies(pipe_id)
                gen_dependencies.update(d for d in deps if d in pipe_ids)

            generation = Generation(
                number=generation_num,
                pipe_ids=current_gen_pipes,
                dependencies=list(gen_dependencies),
            )
            generations_list.append(generation)
            processed.update(current_gen_pipes)
            generation_num += 1

            self.logger.debug(
                f"Generation {generation_num - 1}: {len(current_gen_pipes)} pipes "
                f"({', '.join(current_gen_pipes[:3])}{'...' if len(current_gen_pipes) > 3 else ''})"
            )

        cache_optimizer = CacheOptimizer()
        recommendations = cache_optimizer.recommend(graph, pipe_ids)
        recommendation_levels = cache_optimizer.as_level_map(recommendations)

        plan = ExecutionPlan(
            pipe_ids=pipe_ids,
            generations=generations_list,
            graph=graph,
            strategy=self.get_strategy_name(),
            metadata={
                "execution_order": "forward",
                "description": "Sources → Sinks (batch processing)",
                "cache_recommendations": recommendation_levels,
                "cache_candidate_count": len(recommendation_levels),
                "cache_candidate_entities": sorted(recommendation_levels.keys()),
            },
        )

        # Validate the plan
        plan.validate()

        self.logger.info(
            f"Batch plan generated: {len(generations_list)} generations, "
            f"max parallelism: {plan.max_parallelism()}"
        )

        return plan


class StreamingExecutionStrategy(ExecutionStrategy):
    """Reverse topological sort strategy for streaming processing.

    Executes pipes in reverse topological order:
    - Sinks (consumers) start first and wait for data
    - Sources (producers) start last and begin producing data
    - Pipes grouped into generations for parallel startup

    Use case: Streaming pipelines where consumers should be ready before
    producers start emitting data. This ensures no data is lost and
    checkpoints are properly configured.
    """

    def get_strategy_name(self) -> str:
        return "streaming"

    def plan(self, graph: PipeGraph, pipe_ids: List[str]) -> ExecutionPlan:
        """Generate streaming execution plan.

        Args:
            graph: The dependency graph
            pipe_ids: List of pipe IDs to execute

        Returns:
            ExecutionPlan with reverse topological ordering
        """
        self.logger.info(f"Generating streaming execution plan for {len(pipe_ids)} pipes")

        # Group pipes into generations (reverse order)
        generations_list = []
        processed = set()
        generation_num = 0

        while len(processed) < len(pipe_ids):
            # Find pipes whose dependents are all satisfied
            # In reverse order, we process sinks first (pipes with no dependents in the remaining set)
            current_gen_pipes = [
                pipe_id
                for pipe_id in pipe_ids
                if pipe_id not in processed
                and all(
                    dep in processed or dep not in pipe_ids for dep in graph.get_dependents(pipe_id)
                )
            ]

            if not current_gen_pipes:
                unprocessed = set(pipe_ids) - processed
                raise ValueError(
                    f"Cannot determine next generation. "
                    f"Unprocessed pipes: {unprocessed}. "
                    f"This may indicate a cycle or validation error."
                )

            # For streaming, dependencies are the pipes that will produce data for this generation
            # These are the pipes this generation depends on (will be started later)
            gen_dependencies = set()
            for pipe_id in current_gen_pipes:
                deps = graph.get_dependencies(pipe_id)
                gen_dependencies.update(d for d in deps if d in pipe_ids and d not in processed)

            generation = Generation(
                number=generation_num,
                pipe_ids=current_gen_pipes,
                dependencies=list(gen_dependencies),
            )
            generations_list.append(generation)
            processed.update(current_gen_pipes)
            generation_num += 1

            self.logger.debug(
                f"Generation {generation_num - 1} (streaming): {len(current_gen_pipes)} pipes "
                f"({', '.join(current_gen_pipes[:3])}{'...' if len(current_gen_pipes) > 3 else ''})"
            )

        plan = ExecutionPlan(
            pipe_ids=pipe_ids,
            generations=generations_list,
            graph=graph,
            strategy=self.get_strategy_name(),
            metadata={
                "execution_order": "reverse",
                "description": "Sinks → Sources (streaming processing)",
                "checkpoint_order": "downstream_first",
                "cache_recommendations": {},
                "cache_candidate_count": 0,
                "cache_candidate_entities": [],
            },
        )

        # Validate the plan
        # Note: For streaming, validation logic is different - we need to check
        # that dependents are processed before dependencies
        self._validate_streaming_plan(plan)

        self.logger.info(
            f"Streaming plan generated: {len(generations_list)} generations, "
            f"max parallelism: {plan.max_parallelism()}"
        )

        return plan

    def _validate_streaming_plan(self, plan: ExecutionPlan) -> bool:
        """Validate streaming execution plan.

        For streaming, we validate that consumers are started before producers.

        Args:
            plan: The execution plan to validate

        Returns:
            True if valid

        Raises:
            ValueError: If plan is invalid
        """
        # Check all pipes are accounted for
        pipes_in_plan = set()
        for gen in plan.generations:
            pipes_in_plan.update(gen.pipe_ids)

        if pipes_in_plan != set(plan.pipe_ids):
            missing = set(plan.pipe_ids) - pipes_in_plan
            extra = pipes_in_plan - set(plan.pipe_ids)
            raise ValueError(f"Plan validation failed. Missing: {missing}, Extra: {extra}")

        # Check streaming order: dependents should be started before dependencies
        started = set()
        for gen in plan.generations:
            for pipe_id in gen.pipe_ids:
                # Get all dependents (consumers) of this pipe
                dependents = plan.graph.get_dependents(pipe_id)
                # Check that relevant dependents have been started
                relevant_dependents = [d for d in dependents if d in plan.pipe_ids]
                for dependent in relevant_dependents:
                    if dependent not in started:
                        # This is an error - we're starting a producer before its consumer
                        raise ValueError(
                            f"Streaming order violated: pipe '{pipe_id}' in generation {gen.number} "
                            f"should start after its consumer '{dependent}'"
                        )
            started.update(gen.pipe_ids)

        return True


class ConfigBasedExecutionStrategy(ExecutionStrategy):
    """Config-based execution strategy using pipe processing mode tags.

    Reads 'processing_mode' tag from pipe metadata to determine whether
    to use batch (forward) or streaming (reverse) execution order:
    - Batch mode: Forward topological order (sources → sinks)
    - Streaming mode: Reverse topological order (sinks → sources)

    If any pipe is tagged as streaming, the entire plan uses streaming order.
    Otherwise, defaults to batch order.

    Use case: Automatically apply correct execution strategy based on
    pipe characteristics without manual strategy selection.

    Example tags:
        {"processing_mode": "batch"}      # Use forward order
        {"processing_mode": "streaming"}  # Use reverse order
    """

    MODE_TAG = "processing_mode"
    BATCH_MODE = "batch"
    STREAMING_MODE = "streaming"

    def __init__(self, logger_provider: PythonLoggerProvider):
        """Initialize strategy and store logger provider for delegation."""
        super().__init__(logger_provider)
        self.logger_provider = logger_provider

    def get_strategy_name(self) -> str:
        return "config_based"

    def plan(self, graph: PipeGraph, pipe_ids: List[str]) -> ExecutionPlan:
        """Generate config-based execution plan.

        Args:
            graph: The dependency graph
            pipe_ids: List of pipe IDs to execute

        Returns:
            ExecutionPlan with mode-based ordering (batch or streaming)
        """
        self.logger.info(f"Generating config-based execution plan for {len(pipe_ids)} pipes")

        # Detect processing mode from pipe tags
        detected_mode = self._detect_processing_mode(graph, pipe_ids)

        self.logger.info(f"Detected processing mode: {detected_mode}")

        # Delegate to appropriate strategy based on detected mode
        if detected_mode == self.STREAMING_MODE:
            # Use streaming strategy (reverse order)
            strategy = StreamingExecutionStrategy(self.logger_provider)
            plan = strategy.plan(graph, pipe_ids)
        else:
            # Use batch strategy (forward order)
            strategy = BatchExecutionStrategy(self.logger_provider)
            plan = strategy.plan(graph, pipe_ids)

        # Override strategy name and add metadata
        plan.strategy = self.get_strategy_name()
        plan.metadata.update(
            {
                "detected_mode": detected_mode,
                "description": f"Config-based {detected_mode} execution",
                "mode_source": "pipe_tags",
            }
        )

        self.logger.info(
            f"Config-based plan generated: {len(plan.generations)} generations, "
            f"mode: {detected_mode}, max parallelism: {plan.max_parallelism()}"
        )

        return plan

    def _detect_processing_mode(self, graph: PipeGraph, pipe_ids: List[str]) -> str:
        """Detect processing mode from pipe tags.

        If any pipe is tagged as streaming, return streaming mode.
        Otherwise, return batch mode.

        Args:
            graph: The dependency graph
            pipe_ids: List of pipe IDs to check

        Returns:
            Processing mode ('batch' or 'streaming')
        """
        streaming_pipes = []
        batch_pipes = []
        untagged_pipes = []

        for pipe_id in pipe_ids:
            node = graph.nodes.get(pipe_id)
            if node and node.metadata and node.metadata.tags:
                mode = node.metadata.tags.get(self.MODE_TAG, "").lower()
                if mode == self.STREAMING_MODE:
                    streaming_pipes.append(pipe_id)
                elif mode == self.BATCH_MODE:
                    batch_pipes.append(pipe_id)
                else:
                    untagged_pipes.append(pipe_id)
            else:
                untagged_pipes.append(pipe_id)

        # Log detection results
        if streaming_pipes:
            self.logger.debug(f"Found {len(streaming_pipes)} streaming pipe(s): {streaming_pipes}")
        if batch_pipes:
            self.logger.debug(f"Found {len(batch_pipes)} batch pipe(s): {batch_pipes}")
        if untagged_pipes:
            self.logger.debug(f"Found {len(untagged_pipes)} untagged pipe(s), defaulting to batch")

        # If any pipe is streaming, use streaming mode
        if streaming_pipes:
            return self.STREAMING_MODE

        # Default to batch mode
        return self.BATCH_MODE


class ExecutionPlanGenerator:
    """Facade for generating execution plans.

    This class provides a simple interface for generating execution plans
    using different strategies, with automatic graph building.
    """

    @inject
    def __init__(
        self,
        registry: DataPipesRegistry,
        graph_builder: PipeGraphBuilder,
        logger_provider: PythonLoggerProvider,
    ):
        """Initialize the plan generator.

        Args:
            registry: Pipe registry
            graph_builder: Graph builder for creating dependency graphs
            logger_provider: Logger provider
        """
        self.registry = registry
        self.graph_builder = graph_builder
        self.logger = logger_provider.get_logger("execution_plan_generator")

        # Initialize strategies
        self.batch_strategy = BatchExecutionStrategy(logger_provider)
        self.streaming_strategy = StreamingExecutionStrategy(logger_provider)
        self.config_strategy = ConfigBasedExecutionStrategy(logger_provider)

    def generate_plan(
        self, pipe_ids: List[str], strategy: Optional[ExecutionStrategy] = None
    ) -> ExecutionPlan:
        """Generate execution plan for given pipes.

        Args:
            pipe_ids: List of pipe IDs to execute
            strategy: Execution strategy (defaults to batch)

        Returns:
            ExecutionPlan
        """
        if strategy is None:
            strategy = self.batch_strategy

        self.logger.info(
            f"Generating {strategy.get_strategy_name()} execution plan "
            f"for {len(pipe_ids)} pipes"
        )

        # Build dependency graph
        graph = self.graph_builder.build_graph(pipe_ids)

        # Generate plan using strategy
        plan = strategy.plan(graph, pipe_ids)

        # Log summary
        summary = plan.get_summary()
        self.logger.info(f"Plan summary: {summary}")

        return plan

    def generate_batch_plan(self, pipe_ids: List[str]) -> ExecutionPlan:
        """Generate batch execution plan (forward order).

        Args:
            pipe_ids: List of pipe IDs to execute

        Returns:
            ExecutionPlan with forward topological ordering
        """
        return self.generate_plan(pipe_ids, self.batch_strategy)

    def generate_streaming_plan(self, pipe_ids: List[str]) -> ExecutionPlan:
        """Generate streaming execution plan (reverse order).

        Args:
            pipe_ids: List of pipe IDs to execute

        Returns:
            ExecutionPlan with reverse topological ordering
        """
        return self.generate_plan(pipe_ids, self.streaming_strategy)

    def generate_config_based_plan(self, pipe_ids: List[str]) -> ExecutionPlan:
        """Generate config-based execution plan (priority ordering).

        Args:
            pipe_ids: List of pipe IDs to execute

        Returns:
            ExecutionPlan with priority-based ordering from pipe tags
        """
        return self.generate_plan(pipe_ids, self.config_strategy)

    def visualize_plan(self, plan: ExecutionPlan) -> str:
        """Generate text visualization of execution plan.

        Args:
            plan: Execution plan to visualize

        Returns:
            Multi-line string visualization
        """
        lines = [
            f"Execution Plan ({plan.strategy})",
            f"=" * 50,
            f"Total Pipes: {plan.total_pipes()}",
            f"Total Generations: {plan.total_generations()}",
            f"Max Parallelism: {plan.max_parallelism()}",
            "",
            "Execution Order:",
            "-" * 50,
        ]

        for gen in plan.generations:
            lines.append(f"\nGeneration {gen.number}: ({len(gen)} pipes)")
            for pipe_id in gen.pipe_ids:
                deps = plan.graph.get_dependencies(pipe_id)
                lines.append(f"  - {pipe_id}" + (f" (deps: {deps})" if deps else ""))

        return "\n".join(lines)
