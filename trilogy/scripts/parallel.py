"""
Parallel execution support for Trilogy scripts.

This module provides parallel execution of Trilogy scripts while respecting
dependency ordering determined by the dependency resolver.
"""

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from trilogy.scripts.dependency import (
    DependencyResolver,
    DependencyStrategy,
    ScriptNode,
    create_script_nodes,
)


@dataclass
class ExecutionResult:
    """Result of executing a single script."""

    node: ScriptNode
    success: bool
    error: Exception | None = None
    duration: float = 0.0  # seconds


@dataclass
class ParallelExecutionSummary:
    """Summary of a parallel execution run."""

    total_scripts: int
    successful: int
    failed: int
    total_duration: float
    results: list[ExecutionResult]

    @property
    def all_succeeded(self) -> bool:
        return self.failed == 0


class ParallelExecutor:
    """
    Executes scripts in parallel while respecting dependencies.

    Scripts are organized into levels by the dependency resolver. All scripts
    in a level can run in parallel, but levels must be executed sequentially.
    """

    def __init__(
        self,
        max_workers: int = 5,
        dependency_strategy: DependencyStrategy | None = None,
    ):
        """
        Initialize the parallel executor.

        Args:
            max_workers: Maximum number of parallel workers. Defaults to 5.
            dependency_strategy: Strategy for resolving dependencies.
                                Defaults to FolderDepthStrategy.
        """
        self.max_workers = max_workers
        self.resolver = DependencyResolver(strategy=dependency_strategy)

    def execute(
        self,
        files: list[Path],
        executor_factory: Callable[[ScriptNode], Any],
        execution_fn: Callable[[Any, ScriptNode], None],
        on_script_start: Callable[[ScriptNode], None] | None = None,
        on_script_complete: Callable[[ExecutionResult], None] | None = None,
        on_level_start: Callable[[int, list[ScriptNode]], None] | None = None,
        on_level_complete: Callable[[int, list[ExecutionResult]], None] | None = None,
    ) -> ParallelExecutionSummary:
        """
        Execute scripts in parallel respecting dependencies.

        Args:
            files: List of script file paths to execute.
            executor_factory: Factory function that creates an executor for a script.
                             Called once per script with the ScriptNode.
            execution_fn: Function that executes a script given (executor, node).
            on_script_start: Optional callback when a script starts.
            on_script_complete: Optional callback when a script completes.
            on_level_start: Optional callback when a level starts (level_index, nodes).
            on_level_complete: Optional callback when a level completes.

        Returns:
            ParallelExecutionSummary with all results.
        """
        start_time = datetime.now()

        # Create script nodes
        nodes = create_script_nodes(files)

        if not nodes:
            return ParallelExecutionSummary(
                total_scripts=0,
                successful=0,
                failed=0,
                total_duration=0.0,
                results=[],
            )

        # Resolve execution levels
        levels = self.resolver.resolve(nodes)

        all_results: list[ExecutionResult] = []
        failed_scripts: set[ScriptNode] = set()

        # Execute level by level
        for level_idx, level_nodes in enumerate(levels):
            if on_level_start:
                on_level_start(level_idx, level_nodes)

            # Skip scripts whose dependencies failed
            runnable = [
                n
                for n in level_nodes
                if not self._has_failed_dependency(n, failed_scripts, nodes)
            ]
            skipped = [n for n in level_nodes if n not in runnable]

            # Mark skipped scripts as failed
            for node in skipped:
                result = ExecutionResult(
                    node=node,
                    success=False,
                    error=RuntimeError("Skipped due to failed dependency"),
                    duration=0.0,
                )
                all_results.append(result)
                failed_scripts.add(node)
                if on_script_complete:
                    on_script_complete(result)

            # Execute runnable scripts in parallel
            level_results = self._execute_level(
                runnable,
                executor_factory,
                execution_fn,
                on_script_start,
                on_script_complete,
            )

            all_results.extend(level_results)

            # Track failures for dependency checking in next level
            for result in level_results:
                if not result.success:
                    failed_scripts.add(result.node)

            if on_level_complete:
                # Include skipped results in level completion
                all_level_results = [r for r in all_results if r.node in level_nodes]
                on_level_complete(level_idx, all_level_results)

        total_duration = (datetime.now() - start_time).total_seconds()
        successful = sum(1 for r in all_results if r.success)

        return ParallelExecutionSummary(
            total_scripts=len(nodes),
            successful=successful,
            failed=len(nodes) - successful,
            total_duration=total_duration,
            results=all_results,
        )

    def _has_failed_dependency(
        self,
        node: ScriptNode,
        failed: set[ScriptNode],
        all_nodes: list[ScriptNode],
    ) -> bool:
        """Check if any of node's dependencies have failed."""
        deps = self.resolver.get_dependency_graph(all_nodes).get(node, set())
        return bool(deps & failed)

    def _execute_level(
        self,
        nodes: list[ScriptNode],
        executor_factory: Callable[[ScriptNode], Any],
        execution_fn: Callable[[Any, ScriptNode], None],
        on_script_start: Callable[[ScriptNode], None] | None,
        on_script_complete: Callable[[ExecutionResult], None] | None,
    ) -> list[ExecutionResult]:
        """Execute all scripts in a level in parallel."""
        if not nodes:
            return []

        results: list[ExecutionResult] = []

        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(nodes))) as pool:
            future_to_node: dict[Future, ScriptNode] = {}

            for node in nodes:
                if on_script_start:
                    on_script_start(node)

                future = pool.submit(
                    self._execute_single,
                    node,
                    executor_factory,
                    execution_fn,
                )
                future_to_node[future] = node

            for future in as_completed(future_to_node):
                result = future.result()
                results.append(result)
                if on_script_complete:
                    on_script_complete(result)

        return results

    def _execute_single(
        self,
        node: ScriptNode,
        executor_factory: Callable[[ScriptNode], Any],
        execution_fn: Callable[[Any, ScriptNode], None],
    ) -> ExecutionResult:
        """Execute a single script and return the result."""
        start_time = datetime.now()

        try:
            executor = executor_factory(node)
            execution_fn(executor, node)
            duration = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                node=node,
                success=True,
                error=None,
                duration=duration,
            )
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            return ExecutionResult(
                node=node,
                success=False,
                error=e,
                duration=duration,
            )
