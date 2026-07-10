from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Union

from typing_extensions import TypeAlias

from trilogy.core import graph as nx
from trilogy.execution.state import StaleAsset
from trilogy.parsing.exceptions import ParseError


def normalize_path_variants(path: str | Path) -> Path:
    r"""
    Canonicalize a path into the form used as a dependency-graph key.

    On Windows, paths from Rust may include UNC prefixes like \\?\C:\path, and
    callers may supply a different case than the one on disk (VS Code, for
    example, passes a lowercased drive letter). Both spellings must collapse to
    the same key or edge lookups silently miss.

    Only absolute paths are resolved; a relative path would otherwise be
    anchored to the current working directory.
    """
    # Strip Windows UNC prefix (\\?\)
    if str(path).startswith("\\\\?\\"):
        path = str(path)[4:]
    normal_path = Path(path)
    if normal_path.is_absolute():
        return normal_path.resolve()
    return normal_path


@dataclass(frozen=True)
class ScriptNode:
    """Represents a script file with its path and associated data."""

    path: Path

    def __post_init__(self):
        object.__setattr__(self, "path", normalize_path_variants(self.path))

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if not isinstance(other, ScriptNode):
            return False
        return self.path == other.path

    def __repr__(self):
        return f"ScriptNode({self.path.name})"


@dataclass(frozen=True)
class ManagedRefreshNode:
    """Represents a managed data address to be refreshed.

    `assets`: pre-classified stale assets discovered at preview time. May be
    empty if the address was added to phys_graph as an "unknown" node (downstream
    of a refreshable-root script whose effect on this address can't be known
    until the script runs). The node's executor re-evaluates staleness at
    execute time.

    `datasource_ids`: every datasource at this physical address. Used to drive
    per-node deferred staleness evaluation when `assets` is empty.
    """

    address: str
    owner_script: ScriptNode
    assets: list[StaleAsset]
    datasource_ids: tuple[str, ...] = ()

    def __hash__(self):
        return hash(self.address)

    def __eq__(self, other):
        if not isinstance(other, ManagedRefreshNode):
            return False
        return self.address == other.address

    def __repr__(self):
        return f"ManagedRefreshNode({self.address})"


ExecutionNode: TypeAlias = Union[ScriptNode, ManagedRefreshNode]


class DependencyStrategy(Protocol):
    """Protocol for dependency resolution strategies."""

    def build_graph(self, nodes: list[ScriptNode]) -> nx.DiGraph:
        """
        Given a list of script nodes, return a directed dependency graph.

        The graph should have edges pointing from dependencies TO dependents.
        i.e., if A depends on B, there should be an edge B -> A.
        This means: B must run before A.

        Returns:
            A DiGraph whose nodes are script path strings (``str(node.path)``)
            and whose edges point from dependencies to their dependents.
        """

    def build_folder_graph(self, folder: Path) -> nx.DiGraph: ...


def resolve_with_errors(folder: Path) -> dict:
    try:
        from _preql_import_resolver import PyImportResolver
    except ImportError:
        raise ImportError(
            "The dependency resolution script could not be found. If this error occured in production, please open an issue on https://github.com/trilogy-data/pytrilogy. "
            "If developing, please build it by running: maturin develop"
        )
    resolver = PyImportResolver()

    result = resolver.resolve_directory(str(folder), False)

    # Check for parse errors in warnings and raise ParseError
    warnings = result.get("warnings", [])
    parse_errors = [w for w in warnings if "Failed to parse" in w]
    if parse_errors:
        raise ParseError("\n".join(parse_errors))
    return result


def resolve_script_with_errors(path: Path) -> dict:
    try:
        from _preql_import_resolver import PyImportResolver
    except ImportError:
        raise ImportError(
            "The dependency resolution script could not be found. If this error occured in production, please open an issue on https://github.com/trilogy-data/pytrilogy. "
            "If developing, please build it by running: maturin develop"
        )
    resolver = PyImportResolver()

    result = resolver.resolve(str(path))

    warnings = result.get("warnings", [])
    parse_errors = [w for w in warnings if "Parse error" in w]
    if parse_errors:
        raise ParseError("\n".join(parse_errors))
    return result


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
            A DiGraph (string-keyed by script path) representing dependencies.
        """

        result = resolve_with_errors(folder)
        files = result.get("files", [])
        graph = nx.DiGraph()
        # path -> graph key (str). Keys are the canonical string of each path.
        path_to_key: dict[Path, str] = {}
        edges = result.get("edges", [])
        for file in files:
            normal_path = normalize_path_variants(file)
            key = str(normal_path)
            path_to_key[normal_path] = key
            graph.add_node(key)

        # Build edges from the result
        for edge in edges:
            from_path = normalize_path_variants(edge["from"])
            to_path = normalize_path_variants(edge["to"])

            # Only add edges for files we're managing
            if from_path in path_to_key and to_path in path_to_key:
                graph.add_edge(path_to_key[from_path], path_to_key[to_path])

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

        graph = nx.DiGraph()

        # Add all nodes (graph keys are the canonical string of each path)
        for node in nodes:
            graph.add_node(str(node.path))

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

        # Node paths are already canonical, so each one is its own graph key.
        path_to_key: dict[Path, str] = {node.path: str(node.path) for node in nodes}

        # Use directory resolver to get all edges at once
        directory = nodes[0].path.parent

        result = resolve_with_errors(directory.resolve())

        edges = result.get("edges", [])

        # Build edges from the result
        for edge in edges:
            from_path = normalize_path_variants(edge["from"])
            to_path = normalize_path_variants(edge["to"])

            # Only add edges for files we're managing
            if from_path in path_to_key and to_path in path_to_key:
                graph.add_edge(path_to_key[from_path], path_to_key[to_path])

        return graph


def _validate_acyclic(graph: nx.DiGraph) -> None:
    """Raise ValueError if the (string-keyed) dependency graph has a cycle."""
    if nx.is_directed_acyclic_graph(graph):
        return
    cycles = list(nx.simple_cycles(graph))
    cycle_info = "; ".join(
        " -> ".join(Path(key).name for key in cycle) for cycle in cycles[:3]
    )
    raise ValueError(f"Circular dependencies detected: {cycle_info}")


class NoDependencyStrategy:
    """
    Strategy with no dependencies - all scripts can run in parallel.

    Useful for testing or when scripts are known to be independent.
    """

    def build_graph(self, nodes: list[ScriptNode]) -> nx.DiGraph:
        """Build a graph with no edges (all nodes independent)."""
        graph = nx.DiGraph()
        for node in nodes:
            graph.add_node(str(node.path))
        return graph


class DependencyResolver:
    """
    Resolves execution order for scripts based on a pluggable dependency strategy.

    Uses the Rust-backed graph facade for graph operations and provides
    utilities for both level-based and eager BFS execution patterns.
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
            A DiGraph (string-keyed by script path) representing dependencies.
        """
        graph = self.strategy.build_folder_graph(folder)
        _validate_acyclic(graph)
        return graph

    def build_graph(self, nodes: list[ScriptNode]) -> nx.DiGraph:
        """
        Build the dependency graph for the given nodes.

        Args:
            nodes: List of script nodes.

        Returns:
            A DiGraph (string-keyed by script path) representing dependencies.

        Raises:
            ValueError: If the graph contains cycles.
        """
        graph = self.strategy.build_graph(nodes)
        _validate_acyclic(graph)
        return graph


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
