"""Plan command for Trilogy CLI - shows execution order without running."""

import json
from pathlib import Path as PathlibPath
from typing import Any

import networkx as nx
from click import Path, argument, option, pass_context
from click.exceptions import Exit

from trilogy.scripts.common import (
    handle_execution_exception,
    resolve_input_information,
)
from trilogy.scripts.dependency import ETLDependencyStrategy, ScriptNode
from trilogy.scripts.display import print_error, print_info, show_execution_plan
from trilogy.scripts.parallel_execution import ParallelExecutor


def safe_relative_path(path: PathlibPath, root: PathlibPath) -> str:
    """Get path relative to root, falling back to absolute if not a subpath.

    This handles the case where imports pull in files from outside the root
    directory (e.g., `import ../shared/utils.preql`).
    """
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def graph_to_json(graph: nx.DiGraph, root: PathlibPath) -> dict[str, Any]:
    """Convert dependency graph to JSON-serializable dict."""
    nodes = [
        {"id": safe_relative_path(node.path, root), "path": str(node.path)}
        for node in graph.nodes()
    ]
    edges = [
        {
            "from": safe_relative_path(from_node.path, root),
            "to": safe_relative_path(to_node.path, root),
        }
        for from_node, to_node in graph.edges()
    ]
    return {"nodes": nodes, "edges": edges}


def get_execution_levels(graph: nx.DiGraph) -> list[list[ScriptNode]]:
    """Get execution levels (BFS layers) from dependency graph."""
    if not graph.nodes():
        return []

    levels: list[list[ScriptNode]] = []
    remaining_deps = {node: graph.in_degree(node) for node in graph.nodes()}
    completed: set[ScriptNode] = set()

    while len(completed) < len(graph.nodes()):
        ready = [
            node
            for node in graph.nodes()
            if remaining_deps[node] == 0 and node not in completed
        ]
        if not ready:
            break
        levels.append(ready)
        for node in ready:
            completed.add(node)
            for dependent in graph.successors(node):
                remaining_deps[dependent] -= 1

    return levels


def format_plan_text(
    graph: nx.DiGraph, root: PathlibPath
) -> tuple[list[str], list[tuple[str, str]], list[list[str]]]:
    """Format plan for text display."""
    nodes = [safe_relative_path(node.path, root) for node in graph.nodes()]
    edges = [
        (
            safe_relative_path(from_node.path, root),
            safe_relative_path(to_node.path, root),
        )
        for from_node, to_node in graph.edges()
    ]
    levels = get_execution_levels(graph)
    execution_order = [
        [safe_relative_path(node.path, root) for node in level] for level in levels
    ]
    return nodes, edges, execution_order


@argument("input", type=Path())
@option(
    "--output",
    "-o",
    type=Path(),
    default=None,
    help="Output file path for the plan",
)
@option(
    "--json",
    "json_format",
    is_flag=True,
    default=False,
    help="Output plan as JSON graph with nodes and edges",
)
@option(
    "--config",
    type=Path(exists=True),
    help="Path to trilogy.toml configuration file",
)
@pass_context
def plan(ctx, input: str, output: str | None, json_format: bool, config: str | None):
    """Show execution plan without running scripts."""
    try:
        pathlib_input = PathlibPath(input)
        config_path = PathlibPath(config) if config else None

        # Resolve input to validate it exists
        _ = list(resolve_input_information(input, config_path)[0])

        if not pathlib_input.exists():
            print_error(f"Input path '{input}' does not exist.")
            raise Exit(1)

        # Set up parallel executor with ETL strategy
        parallel_exec = ParallelExecutor(
            max_workers=1,
            dependency_strategy=ETLDependencyStrategy(),
        )

        # Get execution plan
        if pathlib_input.is_dir():
            root = pathlib_input
            graph = parallel_exec.get_folder_execution_plan(pathlib_input)
        elif pathlib_input.is_file():
            root = pathlib_input.parent
            graph = parallel_exec.get_execution_plan([pathlib_input])
        else:
            print_error(f"Input path '{input}' is not a file or directory.")
            raise Exit(1)

        if json_format:
            plan_data = graph_to_json(graph, root)
            levels = get_execution_levels(graph)
            plan_data["execution_order"] = [
                [safe_relative_path(node.path, root) for node in level]
                for level in levels
            ]
            json_output = json.dumps(plan_data, indent=2)

            if output:
                output_path = PathlibPath(output)
                output_path.write_text(json_output)
                print_info(f"Plan written to {output_path}")
            else:
                print(json_output)
        else:
            nodes, edges, execution_order = format_plan_text(graph, root)

            if output:
                output_path = PathlibPath(output)
                lines = []
                lines.append("Execution Plan")
                lines.append(f"Scripts: {len(nodes)}")
                lines.append(f"Dependencies: {len(edges)}")
                lines.append(f"Execution Levels: {len(execution_order)}")
                lines.append("")
                if execution_order:
                    lines.append("Execution Order:")
                    for level_idx, level_scripts in enumerate(execution_order):
                        lines.append(
                            f"  Level {level_idx + 1}: {', '.join(level_scripts)}"
                        )
                    lines.append("")
                if edges:
                    lines.append("Dependencies:")
                    for from_node, to_node in edges:
                        lines.append(f"  {from_node} -> {to_node}")
                output_path.write_text("\n".join(lines))
                print_info(f"Plan written to {output_path}")
            else:
                show_execution_plan(nodes, edges, execution_order)

    except Exit:
        raise
    except Exception as e:
        handle_execution_exception(e, debug=ctx.obj.get("DEBUG", False))
