from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import networkx as nx


def normalize_path_variants(path: str) -> Path:
    """
    On Windows, paths from Rust may include UNC prefixes like \\?\C:\path.
    This function returns the path without the prefix.
    """
    # Handle Windows UNC prefix (\\?\)
    if str(path).startswith("\\\\?\\"):
        # Strip the UNC prefix
        normal_path = str(path)[4:]
        return Path(normal_path)
    return Path(path)


@dataclass(frozen=True)
class ScriptNode:
    """Represents a script file with its path and associated data."""

    path: Path

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

    def build_folder_graph(self, folder: Path) -> nx.DiGraph: ...


class ETLDependencyStrategy:
    """
    Dependency strategy using the Rust-based ETL logic parser.

    Uses the preql-import-resolver Rust library to parse imports, datasources,
    and persist statements, building a dependency graph that respects:
    1. Import dependencies (imported files run before importing files)
    2. Persist-before-declare dependencies (files that persist to a datasource run before files that declare it)
    3. Declare-before-use dependencies (files that declare datasources run before files that import them)
    """

    def build_folder_graph(self, folder: Path) -> nx.DiGraph:
        """
        Build dependency graph for all script files in a folder.

        Args:
            folder: The folder containing script files.

        Returns:
            A networkx DiGraph representing dependencies.
        """
        try:
            from _preql_import_resolver import PyImportResolver
        except ImportError:
            raise ImportError(
                "The dependency resolution script could not be found. If this error occured in production, please open an issue on https://github.com/trilogy-data/pytrilogy. "
                "If developing, please build it by running: maturin develop"
            )
        resolver = PyImportResolver()

        result = resolver.resolve_directory(str(folder), False)
        nodes = result.get("files", [])
        graph = nx.DiGraph()
        path_to_node = {}
        edges = result.get("edges", [])
        # Build the graph
        for node in nodes:
            normal_path = normalize_path_variants(node)
            node = ScriptNode(path=normal_path)
            path_to_node[normal_path] = node
            graph.add_node(node)

        # Build edges from the result
        for edge in edges:
            from_path = normalize_path_variants(edge["from"])
            to_path = normalize_path_variants(edge["to"])

            # Only add edges for files we're managing
            if from_path in path_to_node and to_path in path_to_node:
                graph.add_edge(path_to_node[from_path], path_to_node[to_path])

        return graph

    def build_graph(self, nodes: list[ScriptNode]) -> nx.DiGraph:
        """
        Build dependency graph based on ETL semantics using Rust resolver.

        This strategy requires all nodes to be in the same directory.
        It uses the Rust directory resolver to analyze all files together.

        Returns:
            DiGraph with edges pointing from dependencies to dependents
            (i.e., edge A -> B means A must run before B).
        """
        try:
            from _preql_import_resolver import PyImportResolver
        except ImportError:
            raise ImportError(
                "The preql-import-resolver resolver could not be found.  If this is production, please open an issue."
                "If developing, please build it with maturin: cd trilogy/scripts/dependency && maturin develop"
            )

        graph = nx.DiGraph()

        # Add all nodes
        for node in nodes:
            graph.add_node(node)

        # If we only have one node, return early
        if len(nodes) <= 1:
            return graph

        # Check that all nodes are in the same directory
        directories = {node.path.parent.resolve() for node in nodes}
        if len(directories) > 1:
            raise ValueError(
                "ETLDependencyStrategy requires all script files to be in the same directory. "
                f"Found files in {len(directories)} different directories. {directories}"
            )

        # Build a mapping from absolute path to node
        # We need to handle both regular paths and UNC paths from Rust
        path_to_node = {}
        for node in nodes:
            resolved_path = str(node.path.resolve())
            # Map all path variants to the same node
            path_to_node[normalize_path_variants(resolved_path)] = node

        # Use directory resolver to get all edges at once
        directory = nodes[0].path.parent
        resolver = PyImportResolver()

        result = resolver.resolve_directory(str(directory.resolve()), False)
        edges = result.get("edges", [])

        # Build edges from the result
        for edge in edges:
            from_path = normalize_path_variants(edge["from"])
            to_path = normalize_path_variants(edge["to"])

            # Only add edges for files we're managing
            if from_path in path_to_node and to_path in path_to_node:
                from_node = path_to_node[from_path]
                to_node = path_to_node[to_path]
                graph.add_edge(from_node, to_node)

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
                      Defaults to ETLDependencyStrategy if None.
        """
        self.strategy = strategy or ETLDependencyStrategy()

    def build_folder_graph(self, folder: Path) -> nx.DiGraph:
        """
        Build the dependency graph for all script files in a folder.

        Args:
            folder: The folder containing script files.

        Returns:
            A networkx DiGraph representing dependencies.
        """
        graph = self.strategy.build_folder_graph(folder)

        # Validate no cycles
        if not nx.is_directed_acyclic_graph(graph):
            cycles = list(nx.simple_cycles(graph))
            cycle_info = "; ".join(
                [" -> ".join(str(n.path.name) for n in cycle) for cycle in cycles[:3]]
            )
            raise ValueError(f"Circular dependencies detected: {cycle_info}")

        return graph

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
        nodes.append(ScriptNode(path=file))
    return nodes
