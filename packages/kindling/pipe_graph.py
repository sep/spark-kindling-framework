"""Dependency graph builder for data pipes.

This module provides tools for building and analyzing dependency graphs of data pipes,
including cycle detection, topological sorting, and generation-based execution planning.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict, deque
from injector import inject

from kindling.spark_log_provider import PythonLoggerProvider
from kindling.data_pipes import DataPipesRegistry, PipeMetadata


@dataclass
class PipeNode:
    """Represents a pipe in the dependency graph.
    
    Attributes:
        pipe_id: Unique identifier for the pipe
        input_entities: List of entity IDs this pipe reads from
        output_entity: Entity ID this pipe writes to
        metadata: Full pipe metadata (optional)
    """
    pipe_id: str
    input_entities: List[str]
    output_entity: str
    metadata: Optional[PipeMetadata] = None
    
    def __hash__(self):
        return hash(self.pipe_id)
    
    def __eq__(self, other):
        if not isinstance(other, PipeNode):
            return False
        return self.pipe_id == other.pipe_id


@dataclass
class PipeEdge:
    """Represents a dependency edge between two pipes.
    
    An edge from pipe A to pipe B means:
    - A produces an entity that B consumes
    - B depends on A (A must execute before B)
    
    Attributes:
        from_pipe: Source pipe ID (producer)
        to_pipe: Target pipe ID (consumer)
        entity: The entity linking these pipes
    """
    from_pipe: str
    to_pipe: str
    entity: str
    
    def __hash__(self):
        return hash((self.from_pipe, self.to_pipe, self.entity))
    
    def __eq__(self, other):
        if not isinstance(other, PipeEdge):
            return False
        return (self.from_pipe == other.from_pipe and 
                self.to_pipe == other.to_pipe and
                self.entity == other.entity)


@dataclass
class PipeGraph:
    """Represents the complete dependency graph of pipes.
    
    Attributes:
        nodes: Map of pipe_id to PipeNode
        edges: Set of dependency edges
        adjacency: Map of pipe_id to list of dependent pipe_ids
        reverse_adjacency: Map of pipe_id to list of pipes it depends on
        entity_producers: Map of entity_id to pipe_id that produces it
        entity_consumers: Map of entity_id to list of pipe_ids that consume it
    """
    nodes: Dict[str, PipeNode] = field(default_factory=dict)
    edges: Set[PipeEdge] = field(default_factory=set)
    adjacency: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))
    reverse_adjacency: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))
    entity_producers: Dict[str, str] = field(default_factory=dict)
    entity_consumers: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))
    
    def add_node(self, node: PipeNode):
        """Add a pipe node to the graph."""
        self.nodes[node.pipe_id] = node
        
        # Track entity producers and consumers
        self.entity_producers[node.output_entity] = node.pipe_id
        for entity in node.input_entities:
            self.entity_consumers[entity].append(node.pipe_id)
    
    def add_edge(self, edge: PipeEdge):
        """Add a dependency edge to the graph."""
        self.edges.add(edge)
        
        # Update adjacency lists
        if edge.from_pipe not in self.adjacency:
            self.adjacency[edge.from_pipe] = []
        if edge.to_pipe not in self.adjacency[edge.from_pipe]:
            self.adjacency[edge.from_pipe].append(edge.to_pipe)
        
        if edge.to_pipe not in self.reverse_adjacency:
            self.reverse_adjacency[edge.to_pipe] = []
        if edge.from_pipe not in self.reverse_adjacency[edge.to_pipe]:
            self.reverse_adjacency[edge.to_pipe].append(edge.from_pipe)
    
    def get_dependencies(self, pipe_id: str) -> List[str]:
        """Get list of pipes that must execute before this pipe."""
        return self.reverse_adjacency.get(pipe_id, [])
    
    def get_dependents(self, pipe_id: str) -> List[str]:
        """Get list of pipes that depend on this pipe."""
        return self.adjacency.get(pipe_id, [])
    
    def get_root_nodes(self) -> List[str]:
        """Get pipes with no dependencies (sources)."""
        return [pipe_id for pipe_id in self.nodes.keys() 
                if not self.get_dependencies(pipe_id)]
    
    def get_leaf_nodes(self) -> List[str]:
        """Get pipes with no dependents (sinks)."""
        return [pipe_id for pipe_id in self.nodes.keys() 
                if not self.get_dependents(pipe_id)]
    
    def get_shared_inputs(self) -> Dict[str, List[str]]:
        """Identify entities consumed by multiple pipes (cache candidates).
        
        Returns:
            Map of entity_id to list of pipe_ids that read it
        """
        return {entity: consumers for entity, consumers in self.entity_consumers.items() 
                if len(consumers) > 1}


class GraphCycleError(Exception):
    """Raised when a cycle is detected in the pipe dependency graph."""
    
    def __init__(self, cycle: List[str]):
        self.cycle = cycle
        cycle_str = " → ".join(cycle + [cycle[0]])
        super().__init__(
            f"Cycle detected in pipe dependencies: {cycle_str}\n"
            f"Pipes in cycle: {', '.join(cycle)}"
        )


class GraphValidationError(Exception):
    """Raised when the dependency graph fails validation."""
    pass


class PipeGraphBuilder:
    """Builds and analyzes dependency graphs from pipe definitions.
    
    This class constructs a directed acyclic graph (DAG) from pipe metadata,
    validates the graph structure, detects cycles, and provides topological
    sorting and generation-based execution planning.
    """
    
    @inject
    def __init__(self, registry: DataPipesRegistry, logger_provider: PythonLoggerProvider):
        """Initialize the graph builder.
        
        Args:
            registry: Registry containing pipe definitions
            logger_provider: Logger provider for diagnostics
        """
        self.registry = registry
        self.logger = logger_provider.get_logger("pipe_graph_builder")
    
    def build_graph(self, pipe_ids: List[str]) -> PipeGraph:
        """Build dependency graph from pipe definitions.
        
        Args:
            pipe_ids: List of pipe IDs to include in the graph
            
        Returns:
            PipeGraph with nodes, edges, and adjacency information
            
        Raises:
            GraphValidationError: If graph validation fails
            GraphCycleError: If cycles are detected
        """
        self.logger.debug(f"Building graph for {len(pipe_ids)} pipes")
        graph = PipeGraph()
        
        # Step 1: Create nodes from pipe definitions
        for pipe_id in pipe_ids:
            pipe_def = self.registry.get_pipe_definition(pipe_id)
            if pipe_def is None:
                raise GraphValidationError(f"Pipe not found in registry: {pipe_id}")
            
            node = PipeNode(
                pipe_id=pipe_id,
                input_entities=pipe_def.input_entity_ids,
                output_entity=pipe_def.output_entity_id,
                metadata=pipe_def
            )
            graph.add_node(node)
        
        # Step 2: Create edges based on entity dependencies
        for pipe_id, node in graph.nodes.items():
            for input_entity in node.input_entities:
                # Find which pipe produces this input entity
                producer_id = graph.entity_producers.get(input_entity)
                if producer_id and producer_id in graph.nodes:
                    # Create edge from producer to consumer
                    edge = PipeEdge(
                        from_pipe=producer_id,
                        to_pipe=pipe_id,
                        entity=input_entity
                    )
                    graph.add_edge(edge)
                    self.logger.debug(
                        f"Added edge: {producer_id} → {pipe_id} (via {input_entity})"
                    )
        
        # Step 3: Validate the graph
        self._validate_graph(graph, pipe_ids)
        
        # Step 4: Detect cycles
        cycles = self.detect_cycles(graph)
        if cycles:
            self.logger.error(f"Detected {len(cycles)} cycle(s) in graph")
            # Raise error for first cycle found
            raise GraphCycleError(cycles[0])
        
        self.logger.info(
            f"Graph built successfully: {len(graph.nodes)} nodes, "
            f"{len(graph.edges)} edges, {len(graph.get_root_nodes())} roots, "
            f"{len(graph.get_leaf_nodes())} leaves"
        )
        
        return graph
    
    def _validate_graph(self, graph: PipeGraph, requested_pipes: List[str]):
        """Validate graph structure and dependencies.
        
        Args:
            graph: The graph to validate
            requested_pipes: Original list of requested pipe IDs
            
        Raises:
            GraphValidationError: If validation fails
        """
        errors = []
        warnings = []
        
        # Check for missing pipes
        missing_pipes = set(requested_pipes) - set(graph.nodes.keys())
        if missing_pipes:
            errors.append(f"Pipes not found in registry: {', '.join(missing_pipes)}")
        
        # Check for orphan nodes (no inputs and no outputs in the current graph)
        for pipe_id, node in graph.nodes.items():
            deps = graph.get_dependencies(pipe_id)
            dependents = graph.get_dependents(pipe_id)
            
            # Check for missing input entities (not produced by any pipe in graph)
            for input_entity in node.input_entities:
                producer = graph.entity_producers.get(input_entity)
                if producer is None or producer not in graph.nodes:
                    # This is OK - entity might be external (source table)
                    warnings.append(
                        f"Pipe '{pipe_id}' reads entity '{input_entity}' "
                        f"not produced by any pipe in this graph (external source)"
                    )
        
        # Log warnings
        for warning in warnings:
            self.logger.debug(warning)
        
        # Raise errors
        if errors:
            error_msg = "Graph validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            raise GraphValidationError(error_msg)
    
    def detect_cycles(self, graph: PipeGraph) -> List[List[str]]:
        """Detect cycles in the graph using DFS.
        
        Args:
            graph: The graph to check
            
        Returns:
            List of cycles, where each cycle is a list of pipe IDs
        """
        cycles = []
        visited = set()
        rec_stack = set()
        path = []
        
        def dfs(node_id: str) -> bool:
            """DFS helper that returns True if cycle found."""
            visited.add(node_id)
            rec_stack.add(node_id)
            path.append(node_id)
            
            for neighbor in graph.get_dependents(node_id):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    # Found cycle - extract it from path
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    cycles.append(cycle)
                    return True
            
            path.pop()
            rec_stack.remove(node_id)
            return False
        
        # Run DFS from each unvisited node
        for node_id in graph.nodes.keys():
            if node_id not in visited:
                dfs(node_id)
        
        return cycles
    
    def topological_sort(self, graph: PipeGraph) -> List[str]:
        """Perform topological sort using Kahn's algorithm.
        
        Args:
            graph: The graph to sort
            
        Returns:
            List of pipe IDs in topological order (dependencies first)
            
        Raises:
            GraphCycleError: If graph contains cycles
        """
        # Calculate in-degrees
        in_degree = {pipe_id: len(graph.get_dependencies(pipe_id)) 
                     for pipe_id in graph.nodes.keys()}
        
        # Queue of nodes with no dependencies
        queue = deque([pipe_id for pipe_id, degree in in_degree.items() if degree == 0])
        result = []
        
        while queue:
            # Process node with no dependencies
            node_id = queue.popleft()
            result.append(node_id)
            
            # Reduce in-degree for dependents
            for dependent in graph.get_dependents(node_id):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)
        
        # If we haven't processed all nodes, there's a cycle
        if len(result) != len(graph.nodes):
            unprocessed = set(graph.nodes.keys()) - set(result)
            self.logger.error(f"Topological sort failed - unprocessed nodes: {unprocessed}")
            # Try to find the cycle
            cycles = self.detect_cycles(graph)
            if cycles:
                raise GraphCycleError(cycles[0])
            else:
                raise GraphValidationError("Topological sort failed but no cycle detected")
        
        return result
    
    def get_generations(self, graph: PipeGraph) -> List[List[str]]:
        """Group pipes into generations for parallel execution.
        
        A generation is a set of pipes that:
        1. Have no dependencies on each other
        2. All their dependencies are satisfied by previous generations
        
        Args:
            graph: The graph to analyze
            
        Returns:
            List of generations, where each generation is a list of pipe IDs
            that can execute in parallel
        """
        generations = []
        processed = set()
        
        # Calculate in-degrees relative to unprocessed nodes
        in_degree = {pipe_id: len(graph.get_dependencies(pipe_id)) 
                     for pipe_id in graph.nodes.keys()}
        
        while len(processed) < len(graph.nodes):
            # Find all nodes whose dependencies are satisfied
            current_generation = [
                pipe_id for pipe_id in graph.nodes.keys()
                if pipe_id not in processed and
                all(dep in processed for dep in graph.get_dependencies(pipe_id))
            ]
            
            if not current_generation:
                # This shouldn't happen if graph is acyclic and validated
                unprocessed = set(graph.nodes.keys()) - processed
                raise GraphValidationError(
                    f"Cannot determine next generation. Unprocessed nodes: {unprocessed}"
                )
            
            generations.append(current_generation)
            processed.update(current_generation)
            
            self.logger.debug(
                f"Generation {len(generations)}: {len(current_generation)} pipes"
            )
        
        return generations
    
    def reverse_topological_sort(self, graph: PipeGraph) -> List[str]:
        """Perform reverse topological sort (sinks first, sources last).
        
        Useful for streaming execution where consumers should start before producers.
        
        Args:
            graph: The graph to sort
            
        Returns:
            List of pipe IDs in reverse topological order (sinks first)
        """
        forward_order = self.topological_sort(graph)
        return list(reversed(forward_order))
    
    def get_reverse_generations(self, graph: PipeGraph) -> List[List[str]]:
        """Group pipes into generations in reverse order (for streaming).
        
        Args:
            graph: The graph to analyze
            
        Returns:
            List of generations in reverse order (sinks first)
        """
        forward_generations = self.get_generations(graph)
        return list(reversed(forward_generations))
    
    def visualize_mermaid(self, graph: PipeGraph) -> str:
        """Generate Mermaid diagram markup for graph visualization.
        
        Args:
            graph: The graph to visualize
            
        Returns:
            Mermaid markdown string
        """
        lines = ["graph TD"]
        
        # Add nodes with labels
        for pipe_id, node in graph.nodes.items():
            safe_id = pipe_id.replace("-", "_").replace(".", "_")
            label = f"{pipe_id}"
            lines.append(f'    {safe_id}["{label}"]')
        
        # Add edges
        for edge in graph.edges:
            from_id = edge.from_pipe.replace("-", "_").replace(".", "_")
            to_id = edge.to_pipe.replace("-", "_").replace(".", "_")
            entity_label = edge.entity.split(".")[-1]  # Use short name
            lines.append(f'    {from_id} -->|{entity_label}| {to_id}')
        
        return "\n".join(lines)
    
    def get_graph_stats(self, graph: PipeGraph) -> Dict[str, any]:
        """Get statistical information about the graph.
        
        Args:
            graph: The graph to analyze
            
        Returns:
            Dictionary with graph statistics
        """
        generations = self.get_generations(graph)
        shared_inputs = graph.get_shared_inputs()
        
        # Calculate max parallelism (largest generation)
        max_parallelism = max(len(gen) for gen in generations) if generations else 0
        
        # Calculate average dependencies per pipe
        total_deps = sum(len(graph.get_dependencies(p)) for p in graph.nodes.keys())
        avg_deps = total_deps / len(graph.nodes) if graph.nodes else 0
        
        return {
            "node_count": len(graph.nodes),
            "edge_count": len(graph.edges),
            "root_count": len(graph.get_root_nodes()),
            "leaf_count": len(graph.get_leaf_nodes()),
            "generation_count": len(generations),
            "max_parallelism": max_parallelism,
            "avg_dependencies": round(avg_deps, 2),
            "shared_entities": len(shared_inputs),
            "total_cache_opportunities": sum(len(consumers) for consumers in shared_inputs.values()),
        }
