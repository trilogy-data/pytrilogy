import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Protocol

import networkx as nx

from trilogy import Executor
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


class ExecutionStrategy(Protocol):
    """Protocol for execution traversal strategies."""

    def execute(
        self,
        graph: nx.DiGraph,
        resolver: DependencyResolver,
        max_workers: int,
        executor_factory: Callable[[ScriptNode], Any],
        execution_fn: Callable[[Any, ScriptNode], None],
        on_script_start: Callable[[ScriptNode], None] | None = None,
        on_script_complete: Callable[[ExecutionResult], None] | None = None,
    ) -> list[ExecutionResult]:
        """
        Execute scripts according to the strategy.

        Args:
            graph: The dependency graph (edges point from deps to dependents).
            resolver: The dependency resolver for graph queries.
            max_workers: Maximum parallel workers.
            executor_factory: Factory to create executor for each script.
            execution_fn: Function to execute a script.
            on_script_start: Optional callback when script starts.
            on_script_complete: Optional callback when script completes.

        Returns:
            List of ExecutionResult for all scripts.
        """
        ...


# Type aliases for cleaner signatures
CompletedSet = set[ScriptNode]
FailedSet = set[ScriptNode]
InProgressSet = set[ScriptNode]
ResultsList = list[ExecutionResult]
RemainingDepsDict = dict[ScriptNode, int]
ReadyList = list[ScriptNode]
OnCompleteCallback = Callable[[ExecutionResult], None] | None


def _propagate_failure(
    failed_node: ScriptNode,
    graph: nx.DiGraph,
    completed: CompletedSet,
    in_progress: InProgressSet,
    results: ResultsList,
    failed: FailedSet,
    on_script_complete: OnCompleteCallback,
) -> None:
    """
    Recursively mark all dependents of a failed node as failed.

    Args:
        failed_node: The node that failed.
        graph: The dependency graph.
        completed: Set of completed nodes (modified in place).
        in_progress: Set of nodes currently being executed.
        results: List of execution results (modified in place).
        failed: Set of failed nodes (modified in place).
        on_script_complete: Optional callback when script completes.
    """
    for dependent in graph.successors(failed_node):
        if dependent not in completed and dependent not in in_progress:
            skip_result = ExecutionResult(
                node=dependent,
                success=False,
                error=RuntimeError("Skipped due to failed dependency"),
                duration=0.0,
            )
            results.append(skip_result)
            completed.add(dependent)
            failed.add(dependent)
            if on_script_complete:
                on_script_complete(skip_result)
            _propagate_failure(
                dependent,
                graph,
                completed,
                in_progress,
                results,
                failed,
                on_script_complete,
            )


def _get_next_ready(ready: ReadyList) -> ScriptNode | None:
    """
    Get next ready node from the queue.

    Args:
        ready: List of nodes ready for execution.

    Returns:
        The next ready node, or None if the queue is empty.
    """
    if ready:
        return ready.pop(0)
    return None


def _mark_node_complete(
    node: ScriptNode,
    success: bool,
    graph: nx.DiGraph,
    completed: CompletedSet,
    failed: FailedSet,
    in_progress: InProgressSet,
    remaining_deps: RemainingDepsDict,
    ready: ReadyList,
    results: ResultsList,
    on_script_complete: OnCompleteCallback,
) -> None:
    """
    Mark a node as complete and update dependent nodes.

    This function handles:
    - Removing the node from in_progress
    - Adding it to completed (and failed if unsuccessful)
    - Decrementing dependency counts for successors
    - Adding newly ready nodes to the ready queue
    - Propagating failures to dependent nodes

    Args:
        node: The node that completed.
        success: Whether execution was successful.
        graph: The dependency graph.
        completed: Set of completed nodes (modified in place).
        failed: Set of failed nodes (modified in place).
        in_progress: Set of nodes in progress (modified in place).
        remaining_deps: Dict of remaining dependency counts (modified in place).
        ready: List of ready nodes (modified in place).
        results: List of execution results (modified in place).
        on_script_complete: Optional callback when script completes.
    """
    in_progress.discard(node)
    completed.add(node)
    if not success:
        failed.add(node)

    # Update dependents
    for dependent in graph.successors(node):
        if dependent in completed or dependent in in_progress:
            continue

        if success:
            remaining_deps[dependent] -= 1
            if remaining_deps[dependent] == 0:
                # Check if any dependency failed
                deps = set(graph.predecessors(dependent))
                if deps & failed:
                    # Skip this node - dependency failed
                    skip_result = ExecutionResult(
                        node=dependent,
                        success=False,
                        error=RuntimeError("Skipped due to failed dependency"),
                        duration=0.0,
                    )
                    results.append(skip_result)
                    completed.add(dependent)
                    failed.add(dependent)
                    if on_script_complete:
                        on_script_complete(skip_result)
                    # Recursively mark dependents as failed
                    _propagate_failure(
                        dependent,
                        graph,
                        completed,
                        in_progress,
                        results,
                        failed,
                        on_script_complete,
                    )
                else:
                    ready.append(dependent)
        else:
            # Dependency failed - mark this as failed too
            if dependent not in failed:
                skip_result = ExecutionResult(
                    node=dependent,
                    success=False,
                    error=RuntimeError("Skipped due to failed dependency"),
                    duration=0.0,
                )
                results.append(skip_result)
                completed.add(dependent)
                failed.add(dependent)
                if on_script_complete:
                    on_script_complete(skip_result)
                # Recursively mark dependents as failed
                _propagate_failure(
                    dependent,
                    graph,
                    completed,
                    in_progress,
                    results,
                    failed,
                    on_script_complete,
                )


def _is_execution_done(completed: CompletedSet, total_count: int) -> bool:
    """
    Check if all nodes have been processed.

    Args:
        completed: Set of completed nodes.
        total_count: Total number of nodes.

    Returns:
        True if all nodes are complete, False otherwise.
    """
    return len(completed) >= total_count


def _execute_single(
    node: ScriptNode,
    executor_factory: Callable[[ScriptNode], Executor],
    execution_fn: Callable[[Any, ScriptNode], None],
) -> ExecutionResult:
    """
    Execute a single script and return the result.

    Args:
        node: The script node to execute.
        executor_factory: Factory function to create an executor.
        execution_fn: Function to execute the script.

    Returns:
        ExecutionResult with success status, error (if any), and duration.
    """
    start_time = datetime.now()
    executor = None
    try:
        executor = executor_factory(node)
        execution_fn(executor, node)
        duration = (datetime.now() - start_time).total_seconds()
        if executor:
            executor.close()
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
    finally:
        if executor:
            try:
                executor.close()
            except Exception:
                pass


def _create_worker(
    graph: nx.DiGraph,
    lock: threading.Lock,
    work_available: threading.Condition,
    completed: CompletedSet,
    failed: FailedSet,
    in_progress: InProgressSet,
    remaining_deps: RemainingDepsDict,
    ready: ReadyList,
    results: ResultsList,
    total_count: int,
    executor_factory: Callable[[ScriptNode], Any],
    execution_fn: Callable[[Any, ScriptNode], None],
    on_script_start: Callable[[ScriptNode], None] | None,
    on_script_complete: OnCompleteCallback,
) -> Callable[[], None]:
    """
    Create a worker function for thread execution.

    Args:
        graph: The dependency graph.
        lock: Threading lock for synchronization.
        work_available: Condition variable for signaling work availability.
        completed: Set of completed nodes.
        failed: Set of failed nodes.
        in_progress: Set of nodes in progress.
        remaining_deps: Dict of remaining dependency counts.
        ready: List of ready nodes.
        results: List of execution results.
        total_count: Total number of nodes.
        executor_factory: Factory function to create an executor.
        execution_fn: Function to execute the script.
        on_script_start: Optional callback when script starts.
        on_script_complete: Optional callback when script completes.

    Returns:
        A worker function suitable for threading.
    """

    def worker() -> None:
        while True:
            node = None

            with work_available:
                # Wait for work or completion
                while not ready and not _is_execution_done(completed, total_count):
                    work_available.wait()

                if _is_execution_done(completed, total_count):
                    return

                node = _get_next_ready(ready)
                if node is None:
                    continue

                in_progress.add(node)

            # Execute outside the lock
            if node is not None:
                if on_script_start:
                    on_script_start(node)

                result = _execute_single(node, executor_factory, execution_fn)

                with lock:
                    results.append(result)

                if on_script_complete:
                    on_script_complete(result)

                with lock:
                    _mark_node_complete(
                        node,
                        result.success,
                        graph,
                        completed,
                        failed,
                        in_progress,
                        remaining_deps,
                        ready,
                        results,
                        on_script_complete,
                    )
                    work_available.notify_all()

    return worker


class EagerBFSStrategy:
    """
    Eager BFS execution strategy.

    Uses a work queue and condition variables to coordinate workers.
    """

    def execute(
        self,
        graph: nx.DiGraph,
        resolver: DependencyResolver,
        max_workers: int,
        executor_factory: Callable[[ScriptNode], Any],
        execution_fn: Callable[[Any, ScriptNode], None],
        on_script_start: Callable[[ScriptNode], None] | None = None,
        on_script_complete: Callable[[ExecutionResult], None] | None = None,
    ) -> list[ExecutionResult]:
        """Execute scripts eagerly as dependencies complete."""
        if not graph.nodes():
            return []

        lock = threading.Lock()
        work_available = threading.Condition(lock)

        # Track state
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = set()
        results: ResultsList = []

        # Calculate in-degrees (number of incomplete dependencies)
        remaining_deps: RemainingDepsDict = {
            node: graph.in_degree(node) for node in graph.nodes()
        }

        # Ready queue - nodes with all dependencies satisfied
        ready: ReadyList = [node for node in graph.nodes() if remaining_deps[node] == 0]

        total_count = len(graph.nodes())

        # Create the worker function
        worker = _create_worker(
            graph=graph,
            lock=lock,
            work_available=work_available,
            completed=completed,
            failed=failed,
            in_progress=in_progress,
            remaining_deps=remaining_deps,
            ready=ready,
            results=results,
            total_count=total_count,
            executor_factory=executor_factory,
            execution_fn=execution_fn,
            on_script_start=on_script_start,
            on_script_complete=on_script_complete,
        )

        # Start worker threads
        workers = min(max_workers, total_count)
        threads: list[threading.Thread] = []
        for _ in range(workers):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)

        # Wait for all threads to complete
        for t in threads:
            t.join()

        return results


class ParallelExecutor:
    """
    Executes scripts in parallel while respecting dependencies.

    By default uses eager BFS traversal - scripts execute as soon as their
    dependencies complete. Can be configured to use level-based execution
    for backwards compatibility.
    """

    def __init__(
        self,
        max_workers: int = 5,
        dependency_strategy: DependencyStrategy | None = None,
        execution_strategy: ExecutionStrategy | None = None,
    ):
        """
        Initialize the parallel executor.

        Args:
            max_workers: Maximum number of parallel workers. Defaults to 5.
            dependency_strategy: Strategy for resolving dependencies.
                                Defaults to FolderDepthStrategy.
            execution_strategy: Strategy for traversing the graph during execution.
                               Defaults to EagerBFSStrategy.
        """
        self.max_workers = max_workers
        self.resolver = DependencyResolver(strategy=dependency_strategy)
        self.execution_strategy = execution_strategy or EagerBFSStrategy()

    def execute(
        self,
        files: list[Path],
        executor_factory: Callable[[ScriptNode], Any],
        execution_fn: Callable[[Any, ScriptNode], None],
        on_script_start: Callable[[ScriptNode], None] | None = None,
        on_script_complete: Callable[[ExecutionResult], None] | None = None,
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

        # Build dependency graph
        graph = self.resolver.build_graph(nodes)

        # Execute using the configured strategy
        results = self.execution_strategy.execute(
            graph=graph,
            resolver=self.resolver,
            max_workers=self.max_workers,
            executor_factory=executor_factory,
            execution_fn=execution_fn,
            on_script_start=on_script_start,
            on_script_complete=on_script_complete,
        )

        total_duration = (datetime.now() - start_time).total_seconds()
        successful = sum(1 for r in results if r.success)

        return ParallelExecutionSummary(
            total_scripts=len(nodes),
            successful=successful,
            failed=len(nodes) - successful,
            total_duration=total_duration,
            results=results,
        )

    def get_execution_plan(self, files: list[Path]) -> nx.DiGraph:
        """
        Get the execution plan (dependency graph) without executing.

        Useful for debugging or visualization.

        Args:
            files: List of script file paths.

        Returns:
            The dependency graph that would be used for execution.
        """
        nodes = create_script_nodes(files)
        return self.resolver.build_graph(nodes)
