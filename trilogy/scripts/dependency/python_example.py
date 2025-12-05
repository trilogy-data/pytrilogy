#!/usr/bin/env python3
"""
Python integration example for preql-import-resolver.

This module demonstrates how to call the Rust CLI from Python and
work with the structured JSON output.
"""

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# Use non-interactive backend to avoid Tk dependency
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import networkx as nx


@dataclass
class ImportInfo:
    """Information about a single import statement."""

    raw_path: str
    alias: Optional[str]
    is_stdlib: bool
    parent_dirs: int
    resolved_path: Optional[Path] = None


@dataclass
class Edge:
    """An edge in the dependency graph."""

    from_path: str
    to_path: str
    reason_type: str
    reason_detail: Optional[str] = None


@dataclass
class DependencyGraph:
    """Complete dependency graph with resolution order."""

    root: Path
    files: list[str]
    edges: list[Edge]
    warnings: list[str] = field(default_factory=list)

    def visualize(self, output_path: Optional[str] = None, max_label_length: int = 15):
        """
        Visualize the dependency graph as a DAG.

        Args:
            output_path: If provided, save the figure to this path. Otherwise, display it.
            max_label_length: Maximum characters for node labels before truncating.
        """
        G = nx.DiGraph()

        # Add all nodes
        for file_path in self.files:
            G.add_node(file_path)

        # Add edges: 'from' depends on 'to', so arrow goes from -> to
        # (the dependency must be processed before the dependent)
        for edge in self.edges:
            G.add_edge(edge.from_path, edge.to_path)

        if len(G.nodes()) == 0:
            print("No nodes to visualize.")
            return

        # Create figure with appropriate size
        node_count = len(G.nodes())
        fig_width = max(14, min(24, node_count * 0.6))
        fig_height = max(10, min(18, node_count * 0.4))
        fig, ax = plt.subplots(figsize=(fig_width, fig_height))

        # Use hierarchical layout for DAG
        try:
            # Attempt topological generations for layered layout
            generations = list(nx.topological_generations(G))
            pos = {}
            max_width = max(len(gen) for gen in generations) if generations else 1
            for gen_idx, generation in enumerate(generations):
                y = -gen_idx * 1.5  # Negative to put root at top
                width = len(generation)
                for node_idx, node in enumerate(sorted(generation)):
                    # Center each generation
                    x = (node_idx - (width - 1) / 2) * (max_width / max(width, 1)) * 1.5
                    pos[node] = (x, y)
        except nx.NetworkXUnfeasible:
            # Graph has cycles, use spring layout
            pos = nx.spring_layout(G, k=3, iterations=50, seed=42)

        # Create shortened labels for readability
        def shorten_label(path_str: str) -> str:
            """Shorten path to just filename stem, truncate if needed."""
            # Handle Windows extended path prefix
            clean_path = path_str.replace("\\\\?\\", "").replace("\\?\\", "")
            name = Path(clean_path).stem
            if len(name) > max_label_length:
                return name[: max_label_length - 2] + ".."
            return name

        labels = {node: shorten_label(node) for node in G.nodes()}

        # Color nodes: root is highlighted, identify by checking in-degree
        # Nodes with no incoming edges are roots/entry points
        root_nodes = [n for n in G.nodes() if G.in_degree(n) == 0]
        leaf_nodes = [n for n in G.nodes() if G.out_degree(n) == 0]

        def get_node_color(node):
            if node in root_nodes:
                return "#FF6B6B"  # Red for roots
            elif node in leaf_nodes:
                return "#95E879"  # Green for leaves
            else:
                return "#4ECDC4"  # Teal for intermediate

        node_colors = [get_node_color(n) for n in G.nodes()]

        # Draw the graph
        nx.draw_networkx_nodes(
            G,
            pos,
            ax=ax,
            node_color=node_colors,
            node_size=1800,
            alpha=0.9,
        )

        nx.draw_networkx_edges(
            G,
            pos,
            ax=ax,
            edge_color="#888888",
            arrows=True,
            arrowsize=15,
            arrowstyle="-|>",
            connectionstyle="arc3,rad=0.05",
            alpha=0.6,
            min_source_margin=20,
            min_target_margin=20,
        )

        nx.draw_networkx_labels(
            G,
            pos,
            labels,
            ax=ax,
            font_size=7,
            font_weight="bold",
        )

        # Add title
        root_name = shorten_label(str(self.root))
        ax.set_title(
            f"Dependency Graph: {root_name}\n({len(self.files)} files, {len(self.edges)} edges)",
            fontsize=12,
            fontweight="bold",
            pad=20,
        )

        # Create legend
        from matplotlib.patches import Patch

        legend_elements = [
            Patch(facecolor="#FF6B6B", label="Entry points (no deps)"),
            Patch(facecolor="#4ECDC4", label="Intermediate"),
            Patch(facecolor="#95E879", label="Leaf files (no dependents)"),
        ]
        ax.legend(handles=legend_elements, loc="upper left", fontsize=9)

        ax.axis("off")
        plt.tight_layout()

        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
            print(f"Graph saved to: {output_path}")
        else:
            plt.show()

        plt.close()

    def print_summary(self):
        """Print a text summary of the dependency graph."""
        print(f"\n{'=' * 60}")
        print("Dependency Graph Summary")
        print(f"{'=' * 60}")
        print(f"Root: {self.root}")
        print(f"Total files: {len(self.files)}")
        print(f"Total edges: {len(self.edges)}")

        # Group edges by reason type
        by_reason: dict[str, list[Edge]] = {}
        for edge in self.edges:
            by_reason.setdefault(edge.reason_type, []).append(edge)

        print("\n--- Edge reasons ---")
        for reason, edges in sorted(by_reason.items()):
            print(f"  {reason}: {len(edges)} edges")

        print("\n--- Sample edges ---")
        for edge in self.edges[:10]:
            from_name = Path(edge.from_path).stem
            to_name = Path(edge.to_path).stem
            detail = f" ({edge.reason_detail})" if edge.reason_detail else ""
            print(f"  {from_name} → {to_name} [{edge.reason_type}{detail}]")

        if len(self.edges) > 10:
            print(f"  ... and {len(self.edges) - 10} more edges")

        if self.warnings:
            print("\n--- Warnings ---")
            for warning in self.warnings:
                print(f"  ⚠ {warning}")

        print(f"{'=' * 60}\n")


class PreqlImportResolver:
    """
    Python wrapper for the preql-import-resolver Rust CLI.

    Example usage:
        resolver = PreqlImportResolver()

        # Parse a single file
        imports = resolver.parse_file("models/customer.preql")

        # Resolve all dependencies
        graph = resolver.resolve_dependencies("main.preql")

        # Get just the dependency order
        order = resolver.get_dependency_order("main.preql")
    """

    def __init__(self, binary_path: str = "preql-import-resolver"):
        """
        Initialize the resolver.

        Args:
            binary_path: Path to the preql-import-resolver binary.
                         Defaults to assuming it's in PATH.
        """
        self.binary_path = binary_path

    def _run(self, *args: str) -> dict:
        """Run the CLI and return parsed JSON output."""
        print(f"Running command: {self.binary_path} {' '.join(args)}")
        cmd = [self.binary_path, *args, "--format", "json"]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            # Try to parse error as JSON
            try:
                error_data = json.loads(result.stderr or result.stdout)
                raise RuntimeError(error_data.get("error", "Unknown error"))
            except json.JSONDecodeError:
                raise RuntimeError(result.stderr or result.stdout or "Unknown error")

        return json.loads(result.stdout)

    def parse_file(self, file_path: str | Path) -> list[ImportInfo]:
        """
        Parse a single file and return its imports.

        Args:
            file_path: Path to the PreQL file to parse.

        Returns:
            List of ImportInfo objects for each import in the file.
        """
        data = self._run("parse", str(file_path))

        return [
            ImportInfo(
                raw_path=imp["raw_path"],
                alias=imp.get("alias"),
                is_stdlib=imp["is_stdlib"],
                parent_dirs=imp["parent_dirs"],
            )
            for imp in data["imports"]
        ]

    def resolve_dependencies(self, root_file: str | Path) -> DependencyGraph:
        """
        Resolve all dependencies starting from a root file.

        Args:
            root_file: Path to the root PreQL file.

        Returns:
            DependencyGraph with full resolution information.
        """
        data = self._run("resolve", str(root_file))

        # Files is just a list of path strings
        files = data.get("files", [])
        for x in files:
            print(f"File: {x}")

        # Parse edges
        edges = []
        for edge_data in data.get("edges", []):
            print(edge_data)
            reason = edge_data.get("reason", {})
            reason_type = reason.get("type", "unknown")
            # Extract detail (e.g., datasource name)
            reason_detail = reason.get("datasource") or reason.get("concept")

            edges.append(
                Edge(
                    from_path=edge_data["from"],
                    to_path=edge_data["to"],
                    reason_type=reason_type,
                    reason_detail=reason_detail,
                )
            )

        return DependencyGraph(
            root=Path(root_file),
            files=files,
            edges=edges,
            warnings=data.get("warnings", []),
        )

    def get_dependency_order(self, root_file: str | Path) -> list[Path]:
        """
        Get the dependency order for a root file.

        This is a convenience method that returns just the ordered list
        of file paths, where dependencies come before dependents.

        Args:
            root_file: Path to the root PreQL file.

        Returns:
            List of file paths in dependency order.
        """
        data = self._run("resolve", str(root_file), "--order-only")
        return [Path(p) for p in data]


def main():
    # Use the resolver
    # Note: This assumes the binary is built and in PATH or current directory
    binary = Path(__file__).parent / "target/release/preql-import-resolver.exe"
    print(str(binary))
    if not binary.exists():
        raise ValueError(
            "preql-import-resolver binary not found. Please build it first."
        )

    resolver = PreqlImportResolver(str(binary))

    # Example: Get full dependency graph
    print("\n=== Full dependency graph ===")
    graph = resolver.resolve_dependencies(
        "C:\\Users\\ethan\\coding_projects\\pytrilogy\\tests\\modeling\\tpc_ds_duckdb"
    )

    # Print text summary
    graph.print_summary()

    # Visualize as a DAG
    graph.visualize(output_path="dependency_graph.png")


if __name__ == "__main__":
    main()
