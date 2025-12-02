"""
Dependency resolution for Trilogy script execution.

This module provides a pluggable dependency resolution system for determining
execution order of Trilogy scripts. The default strategy uses folder depth
(deeper folders run before shallower ones), but this can be easily swapped
for more sophisticated logic.
"""

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


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


class DependencyStrategy(Protocol):
    """Protocol for dependency resolution strategies."""

    def get_dependencies(
        self, nodes: list[ScriptNode]
    ) -> dict[ScriptNode, set[ScriptNode]]:
        """
        Given a list of script nodes, return a dependency graph.

        Returns:
            A dict mapping each node to the set of nodes it depends on
            (i.e., nodes that must run BEFORE this node).
        """
        ...


class FolderDepthStrategy:
    """
    Dependency strategy based on folder depth.

    Scripts deeper in the folder hierarchy run before scripts at shallower levels.
    For example, abc/def/script.preql runs before abc/script.preql.

    This is a placeholder strategy that can be replaced with more sophisticated
    logic (e.g., explicit dependency declarations, import analysis, etc.).
    """

    def get_dependencies(
        self, nodes: list[ScriptNode]
    ) -> dict[ScriptNode, set[ScriptNode]]:
        """
        Build dependency graph based on folder depth.

        Deeper scripts are dependencies of shallower scripts within the same
        directory tree.
        """
        dependencies: dict[ScriptNode, set[ScriptNode]] = {
            node: set() for node in nodes
        }

        # Group nodes by their parent directories at each level
        # A script depends on all scripts that are in subdirectories of its parent
        for node in nodes:
            node_depth = len(node.path.parts)
            node_parent = node.path.parent

            for other in nodes:
                if other == node:
                    continue

                other_depth = len(other.path.parts)
                other_parent = other.path.parent

                # Check if 'other' is deeper and in a subdirectory of node's parent
                # or in a sibling subdirectory
                if other_depth > node_depth:
                    # Check if other is in a subdirectory relative to node's directory
                    try:
                        other.path.relative_to(node_parent)
                        # other is in a subdirectory of node's parent, so other runs first
                        dependencies[node].add(other)
                    except ValueError:
                        # other is not in node's parent tree
                        pass

        return dependencies


class DependencyResolver:
    """
    Resolves execution order for scripts based on a pluggable dependency strategy.

    Uses topological sorting to determine valid execution order while respecting
    dependencies. Returns execution levels for parallel execution.
    """

    def __init__(self, strategy: DependencyStrategy | None = None):
        """
        Initialize the resolver with a dependency strategy.

        Args:
            strategy: The dependency resolution strategy to use.
                      Defaults to FolderDepthStrategy.
        """
        self.strategy = strategy or FolderDepthStrategy()

    def resolve(self, nodes: list[ScriptNode]) -> list[list[ScriptNode]]:
        """
        Resolve execution order and return parallelizable batches.

        Uses Kahn's algorithm (BFS-based topological sort) to produce
        execution levels where all scripts in a level can run in parallel.

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

        # Get dependency edges from strategy
        dependencies = self.strategy.get_dependencies(nodes)

        # Build reverse graph (dependents) and in-degree counts
        dependents: dict[ScriptNode, set[ScriptNode]] = defaultdict(set)
        in_degree: dict[ScriptNode, int] = {node: 0 for node in nodes}

        for node, deps in dependencies.items():
            in_degree[node] = len(deps)
            for dep in deps:
                dependents[dep].add(node)

        # BFS-based topological sort producing levels
        levels: list[list[ScriptNode]] = []
        current_level = [node for node in nodes if in_degree[node] == 0]

        processed = 0
        while current_level:
            levels.append(current_level)
            processed += len(current_level)

            next_level = []
            for node in current_level:
                for dependent in dependents[node]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_level.append(dependent)

            current_level = next_level

        if processed != len(nodes):
            raise ValueError(
                f"Circular dependency detected. Processed {processed} of {len(nodes)} scripts."
            )

        return levels

    def get_dependency_graph(
        self, nodes: list[ScriptNode]
    ) -> dict[ScriptNode, set[ScriptNode]]:
        """
        Get the raw dependency graph for inspection/debugging.

        Returns:
            Dict mapping each node to its dependencies.
        """
        return self.strategy.get_dependencies(nodes)


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
