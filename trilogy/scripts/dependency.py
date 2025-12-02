"""
Dependency resolution for Trilogy script execution.

This module provides a pluggable dependency resolution system for determining
execution order of Trilogy scripts. The default strategy uses folder depth
(deeper folders run before shallower ones), but this can be easily swapped
for more sophisticated logic (e.g., explicit imports, content analysis).
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import networkx as nx


@dataclass(frozen=True)
class ScriptNode:
    """Represents a script file with its path and associated data."""

    path: Path
    content: str

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if not isinstance(other, ScriptNode):
            return False
        return self.path == other.path

    def __repr__(self):
        return f"ScriptNode({self.path.name})"


class DependencyStrategy(Protocol):
    """Protocol for dependency resolution strategies."""

    def build_graph(self, nodes: list[ScriptNode]) -> nx.DiGraph:
        """
        Given a list of script nodes, return a directed dependency graph.

        The graph should have edges pointing from dependencies TO dependents.
        i.e., if A depends on B, there should be an edge B -> A.
        This means: B must run before A.

        Returns:
            A networkx DiGraph where nodes are ScriptNode instances and
            edges point from dependencies to their dependents.
        """
        ...


class FolderDepthStrategy:
    """
    Dependency strategy based on folder depth.

    Scripts deeper in the folder hierarchy run before scripts at shallower levels.
    For example, abc/def/script.preql runs before abc/script.preql.

    File-level dependencies are created: each file at depth N depends on ALL files
    at depth N+1 that are in subdirectories of that file's parent directory.

    This is a placeholder strategy that can be replaced with more sophisticated
    logic (e.g., explicit dependency declarations, import analysis, etc.).
    """

    def build_graph(self, nodes: list[ScriptNode]) -> nx.DiGraph:
        """
        Build dependency graph based on folder depth.

        Creates file-level edges where deeper scripts must complete before
        shallower scripts that contain them in their directory tree.

        Returns:
            DiGraph with edges pointing from dependencies to dependents
            (i.e., edge A -> B means A must run before B).
        """
        graph = nx.DiGraph()

        # Add all nodes to the graph
        for node in nodes:
            graph.add_node(node)

        # Create file-level dependencies based on folder depth
        for node in nodes:
            node_depth = len(node.path.parts)
            node_parent = node.path.parent

            for other in nodes:
                if other == node:
                    continue

                other_depth = len(other.path.parts)

                # Check if 'other' is deeper and in a subdirectory of node's parent
                if other_depth > node_depth:
                    try:
                        other.path.relative_to(node_parent)
                        # 'other' is deeper and in node's parent tree
                        # 'other' must run before 'node'
                        # Edge direction: other -> node (dependency -> dependent)
                        graph.add_edge(other, node)
                    except ValueError:
                        # other is not in node's parent tree
                        pass

        return graph


class NoDependencyStrategy:
    """
    Strategy with no dependencies - all scripts can run in parallel.

    Useful for testing or when scripts are known to be independent.
    """

    def build_graph(self, nodes: list[ScriptNode]) -> nx.DiGraph:
        """Build a graph with no edges (all nodes independent)."""
        graph = nx.DiGraph()
        for node in nodes:
            graph.add_node(node)
        return graph


class DependencyResolver:
    """
    Resolves execution order for scripts based on a pluggable dependency strategy.

    Uses networkx for graph operations and provides utilities for both
    level-based and eager BFS execution patterns.
    """

    def __init__(self, strategy: DependencyStrategy | None = None):
        """
        Initialize the resolver with a dependency strategy.

        Args:
            strategy: The dependency resolution strategy to use.
                      Defaults to FolderDepthStrategy.
        """
        self.strategy = strategy or FolderDepthStrategy()

    def build_graph(self, nodes: list[ScriptNode]) -> nx.DiGraph:
        """
        Build the dependency graph for the given nodes.

        Args:
            nodes: List of script nodes.

        Returns:
            A networkx DiGraph representing dependencies.

        Raises:
            ValueError: If the graph contains cycles.
        """
        graph = self.strategy.build_graph(nodes)

        # Validate no cycles
        if not nx.is_directed_acyclic_graph(graph):
            cycles = list(nx.simple_cycles(graph))
            cycle_info = "; ".join(
                [" -> ".join(str(n.path.name) for n in cycle) for cycle in cycles[:3]]
            )
            raise ValueError(f"Circular dependencies detected: {cycle_info}")

        return graph

    def get_root_nodes(self, graph: nx.DiGraph) -> list[ScriptNode]:
        """
        Get nodes with no dependencies (in-degree 0).

        These are the nodes that can be executed immediately.

        Args:
            graph: The dependency graph.

        Returns:
            List of nodes with no incoming edges.
        """
        return [node for node in graph.nodes() if graph.in_degree(node) == 0]

    def get_dependents(self, graph: nx.DiGraph, node: ScriptNode) -> list[ScriptNode]:
        """
        Get nodes that directly depend on the given node.

        Args:
            graph: The dependency graph.
            node: The node whose dependents to find.

        Returns:
            List of nodes that have 'node' as a dependency.
        """
        return list(graph.successors(node))

    def get_dependencies(self, graph: nx.DiGraph, node: ScriptNode) -> list[ScriptNode]:
        """
        Get nodes that the given node depends on.

        Args:
            graph: The dependency graph.
            node: The node whose dependencies to find.

        Returns:
            List of nodes that must run before 'node'.
        """
        return list(graph.predecessors(node))

    def resolve_levels(self, nodes: list[ScriptNode]) -> list[list[ScriptNode]]:
        """
        Resolve execution order and return parallelizable batches (levels).

        This is the legacy interface for level-based execution.
        Uses topological generations from networkx.

        Args:
            nodes: List of script nodes to order.

        Returns:
            A list of levels, where each level is a list of nodes that can
            be executed in parallel. Levels must be executed in order.

        Raises:
            ValueError: If there's a circular dependency.
        """
        if not nodes:
            return []

        graph = self.build_graph(nodes)

        # Use networkx topological_generations for level-based ordering
        return [list(gen) for gen in nx.topological_generations(graph)]

    # Legacy alias
    def resolve(self, nodes: list[ScriptNode]) -> list[list[ScriptNode]]:
        """Legacy alias for resolve_levels."""
        return self.resolve_levels(nodes)

    def get_dependency_graph(
        self, nodes: list[ScriptNode]
    ) -> dict[ScriptNode, set[ScriptNode]]:
        """
        Get the raw dependency graph as a dict for inspection/debugging.

        Returns:
            Dict mapping each node to its dependencies (predecessors).
        """
        graph = self.build_graph(nodes)
        return {node: set(graph.predecessors(node)) for node in graph.nodes()}


def create_script_nodes(files: list[Path]) -> list[ScriptNode]:
    """
    Create ScriptNode instances from file paths.

    Args:
        files: List of paths to script files.

    Returns:
        List of ScriptNode instances with file contents loaded.
    """
    nodes = []
    for file in files:
        with open(file, "r") as f:
            content = f.read()
        nodes.append(ScriptNode(path=file, content=content))
    return nodes
