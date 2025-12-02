#!/usr/bin/env python3
"""
Python integration example for preql-import-resolver.

This module demonstrates how to call the Rust CLI from Python and
work with the structured JSON output.
"""

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ImportInfo:
    """Information about a single import statement."""

    raw_path: str
    alias: Optional[str]
    is_stdlib: bool
    parent_dirs: int
    resolved_path: Optional[Path] = None


@dataclass
class FileNode:
    """Information about a parsed file and its dependencies."""

    path: Path
    relative_path: Path
    imports: list[ImportInfo]
    dependencies: list[Path]


@dataclass
class DependencyGraph:
    """Complete dependency graph with resolution order."""

    root: Path
    order: list[Path]
    files: dict[Path, FileNode]
    warnings: list[str]


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

        files = {}
        for path_str, node_data in data["files"].items():
            path = Path(path_str)
            files[path] = FileNode(
                path=path,
                relative_path=Path(node_data["relative_path"]),
                imports=[
                    ImportInfo(
                        raw_path=imp["raw_path"],
                        alias=imp.get("alias"),
                        is_stdlib=imp["is_stdlib"],
                        parent_dirs=imp.get("parent_dirs", 0),
                        resolved_path=(
                            Path(imp["resolved_path"])
                            if imp.get("resolved_path")
                            else None
                        ),
                    )
                    for imp in node_data["imports"]
                ],
                dependencies=[Path(d) for d in node_data["dependencies"]],
            )

        return DependencyGraph(
            root=Path(data["root"]),
            order=[Path(p) for p in data["order"]],
            files=files,
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
    """Example usage demonstrating the Python integration."""
    import tempfile

    # Create a temporary directory with test files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test PreQL files
        models_dir = Path(tmpdir) / "models"
        models_dir.mkdir()

        # shared/utils.preql - no dependencies
        shared_dir = Path(tmpdir) / "shared"
        shared_dir.mkdir()
        (shared_dir / "utils.preql").write_text(
            """
// Utility functions - no imports
const MAX_LENGTH <- 100;
        """
        )

        # models/base.preql - depends on shared/utils
        (models_dir / "base.preql").write_text(
            """
import ..shared.utils;
// Base model definitions
        """
        )

        # models/customer.preql - depends on base
        (models_dir / "customer.preql").write_text(
            """
import base;
import std.aggregates;  // stdlib - will be skipped
// Customer model
key customer_id int;
        """
        )

        # main.preql - depends on customer
        main_file = Path(tmpdir) / "main.preql"
        main_file.write_text(
            """
import models.customer as cust;
// Main entry point
select customer_id;
        """
        )

        # Use the resolver
        # Note: This assumes the binary is built and in PATH or current directory
        binary = "./target/release/preql-import-resolver"
        if not Path(binary).exists():
            binary = "./target/debug/preql-import-resolver"
        if not Path(binary).exists():
            print("Binary not found. Build with: cargo build --release")
            print("\nShowing what the output would look like:\n")

            # Simulate output for demonstration
            print("=== Parsing single file ===")
            print(
                json.dumps(
                    {
                        "file": str(main_file),
                        "imports": [
                            {
                                "raw_path": "models.customer",
                                "alias": "cust",
                                "is_stdlib": False,
                                "parent_dirs": 0,
                            }
                        ],
                    },
                    indent=2,
                )
            )

            print("\n=== Dependency order ===")
            print(
                json.dumps(
                    [
                        str(shared_dir / "utils.preql"),
                        str(models_dir / "base.preql"),
                        str(models_dir / "customer.preql"),
                        str(main_file),
                    ],
                    indent=2,
                )
            )
            return

        resolver = PreqlImportResolver(binary)

        # Example 1: Parse a single file
        print("=== Parsing single file ===")
        imports = resolver.parse_file(main_file)
        for imp in imports:
            print(f"  - {imp.raw_path}" + (f" as {imp.alias}" if imp.alias else ""))
            if imp.is_stdlib:
                print("    (stdlib - skipped)")

        # Example 2: Get full dependency graph
        print("\n=== Full dependency graph ===")
        graph = resolver.resolve_dependencies(main_file)
        print(f"Root: {graph.root}")
        print(f"Total files: {len(graph.files)}")

        if graph.warnings:
            print("Warnings:")
            for w in graph.warnings:
                print(f"  - {w}")

        # Example 3: Get just the dependency order
        print("\n=== Dependency order ===")
        order = resolver.get_dependency_order(main_file)
        for i, path in enumerate(order, 1):
            print(f"  {i}. {path.name}")

        # Example 4: Process files in dependency order
        print("\n=== Processing files in order ===")
        for path in order:
            # In a real application, you would process each file here
            # knowing that all its dependencies have been processed
            print(f"  Processing: {path.name}")


if __name__ == "__main__":
    main()
