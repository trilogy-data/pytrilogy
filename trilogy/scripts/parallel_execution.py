import threading
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Protocol

import networkx as nx
from click.exceptions import Exit

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
            max_workers: Maximum parallel workers.
            executor_factory: Factory to create executor for each script.
            execution_fn: Function to execute a script.

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
    Recursively mark all *unstarted* dependents of a failed node as failed and skipped.
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
    """Get next ready node from the queue."""
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
    Mark a node as complete, update dependent counts, and add newly ready/skipped nodes.
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
                # Check if any dependency failed before running
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
            # Current node failed - mark this dependent as skipped
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
    """Check if all nodes have been processed."""
    return len(completed) >= total_count


def _execute_single(
    node: ScriptNode,
    executor_factory: Callable[[ScriptNode], Executor],
    execution_fn: Callable[[Any, ScriptNode], None],
) -> ExecutionResult:
    """Execute a single script and return the result."""
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
        if executor:
            executor.close()  # Ensure executor is closed even on failure
        return ExecutionResult(
            node=node,
            success=False,
            error=e,
            duration=duration,
        )


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
    Create a worker function for thread execution to process the dependency graph.
    """

    def worker() -> None:
        while True:
            node = None

            with work_available:
                # Wait for work or global completion
                while not ready and not _is_execution_done(completed, total_count):
                    work_available.wait()

                if _is_execution_done(completed, total_count):
                    return

                node = _get_next_ready(ready)
                if node is None:
                    # Should be impossible if total_count check is correct, but handles race condition safety
                    continue

                in_progress.add(node)

            # Execute outside the lock
            if node is not None:
                if on_script_start:
                    on_script_start(node)
                result = _execute_single(node, executor_factory, execution_fn)

                # Use the lock for state updates and notification
                with lock:
                    results.append(result)

                    if on_script_complete:
                        on_script_complete(result)

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
                    work_available.notify_all()  # Notify other workers of new ready/completed state

    return worker


class EagerBFSStrategy:
    """
    Eager Breadth-First Search (BFS) execution strategy.

    Scripts execute as soon as all their dependencies complete, maximizing parallelism.
    Uses a thread pool coordinated by locks and condition variables.
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

        # Ready queue - nodes with all dependencies satisfied initially (in-degree 0)
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

        # Wake up any waiting workers if we have initial work
        with work_available:
            work_available.notify_all()

        # Wait for all threads to complete
        for t in threads:
            t.join()

        return results


class ParallelExecutor:
    """
    Executes scripts in parallel while respecting dependencies.

    Uses an Eager BFS traversal by default, running scripts as soon as their
    dependencies complete.
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
            max_workers: Maximum number of parallel workers.
            dependency_strategy: Strategy for resolving dependencies.
            execution_strategy: Strategy for traversing the graph during execution.
        """
        self.max_workers = max_workers
        # Resolver finds dependencies and builds the graph
        self.resolver = DependencyResolver(strategy=dependency_strategy)
        # Execution strategy determines how the graph is traversed and executed
        self.execution_strategy = execution_strategy or EagerBFSStrategy()

    def execute(
        self,
        root: Path,
        executor_factory: Callable[[ScriptNode], Any],
        execution_fn: Callable[[Any, ScriptNode], None],
        on_script_start: Callable[[ScriptNode], None] | None = None,
        on_script_complete: Callable[[ExecutionResult], None] | None = None,
    ) -> ParallelExecutionSummary:
        """
        Execute scripts in parallel respecting dependencies.

        Args:
            root: Root path (folder or single file) to find scripts.
            executor_factory: Factory function to create an executor for a script.
            execution_fn: Function that executes a script given (executor, node).

        Returns:
            ParallelExecutionSummary with all results.
        """
        start_time = datetime.now()

        # Build dependency graph
        if root.is_dir():
            graph = self.resolver.build_folder_graph(root)
            nodes = list(graph.nodes())
        else:
            nodes = create_script_nodes([root])
            graph = self.resolver.build_graph(nodes)

        # Total count of nodes for summary/completion check
        total_scripts = len(nodes)

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
            total_scripts=total_scripts,
            successful=successful,
            failed=total_scripts - successful,
            total_duration=total_duration,
            results=results,
        )

    def get_folder_execution_plan(self, folder: Path) -> nx.DiGraph:
        """
        Get the execution plan (dependency graph) for all scripts in a folder.
        """
        return self.resolver.build_folder_graph(folder)

    def get_execution_plan(self, files: list[Path]) -> nx.DiGraph:
        """
        Get the execution plan (dependency graph) without executing.
        """
        nodes = create_script_nodes(files)
        return self.resolver.build_graph(nodes)


def run_single_script_execution(
    files: list[StringIO | Path],
    directory: Path,
    input_type: str,
    input_name: str,
    edialect,
    param: tuple[str, ...],
    conn_args,
    debug: bool,
    execution_mode: str,
    config,
) -> None:
    """
    Run single script execution with polished multi-statement progress display.

    Args:
        text: List of script contents
        directory: Working directory
        input_type: Type of input (file, query, etc.)
        input_name: Name of the input
        edialect: Dialect to use
        param: Environment parameters
        conn_args: Connection arguments
        debug: Debug mode flag
        execution_mode: One of 'run', 'integration', or 'unit'
    """
    from trilogy.scripts.common import (
        create_executor,
        handle_execution_exception,
        validate_datasources,
    )
    from trilogy.scripts.display import (
        RICH_AVAILABLE,
        create_progress_context,
        print_success,
        show_execution_info,
        show_execution_start,
        show_execution_summary,
    )
    from trilogy.scripts.single_execution import (
        execute_queries_simple,
        execute_queries_with_progress,
    )

    show_execution_info(input_type, input_name, edialect.value, debug)

    exec = create_executor(param, directory, conn_args, edialect, debug, config)
    base = files[0]
    if isinstance(base, StringIO):
        text = [base.getvalue()]
    else:
        with open(base, "r") as raw:
            text = [raw.read()]

    if execution_mode == "run":
        # Parse all scripts and collect queries
        queries = []
        try:
            for script in text:
                queries += exec.parse_text(script)
        except Exception as e:
            handle_execution_exception(e, debug=debug)

        start = datetime.now()
        show_execution_start(len(queries))

        # Execute with progress tracking for multiple statements
        if len(queries) > 1 and RICH_AVAILABLE:
            progress = create_progress_context()
        else:
            progress = None

        try:
            if progress:
                exception = execute_queries_with_progress(exec, queries)
            else:
                exception = execute_queries_simple(exec, queries)

            total_duration = datetime.now() - start
            show_execution_summary(len(queries), total_duration, exception is None)

            if exception:
                raise Exit(1) from exception
        except Exit:
            raise
        except Exception as e:
            handle_execution_exception(e, debug=debug)

    elif execution_mode == "integration":
        for script in text:
            exec.parse_text(script)
        validate_datasources(exec, mock=False, quiet=False)
        print_success("Integration tests passed successfully!")

    elif execution_mode == "unit":
        for script in text:
            exec.parse_text(script)
        validate_datasources(exec, mock=True, quiet=False)
        print_success("Unit tests passed successfully!")


def get_execution_strategy(strategy_name: str):
    """Get execution strategy by name."""
    strategies = {
        "eager_bfs": EagerBFSStrategy,
    }
    if strategy_name not in strategies:
        raise ValueError(
            f"Unknown execution strategy: {strategy_name}. "
            f"Available: {', '.join(strategies.keys())}"
        )
    return strategies[strategy_name]()


def run_parallel_execution(
    cli_params,
    execution_fn,
    execution_mode: str = "run",
) -> None:
    """
    Run parallel execution for directory inputs, or single-script execution
    with polished progress display for single files/inline queries.

    Args:
        cli_params: CLI runtime parameters containing all execution settings
        execution_fn: Function to execute each script (exec, node, quiet) -> None
        execution_mode: One of 'run', 'integration', or 'unit'
    """
    from trilogy.scripts.common import (
        create_executor_for_script,
        merge_runtime_config,
        resolve_input_information,
    )
    from trilogy.scripts.dependency import ETLDependencyStrategy
    from trilogy.scripts.display import (
        print_error,
        print_success,
        show_execution_info,
        show_parallel_execution_start,
        show_parallel_execution_summary,
        show_script_result,
    )

    # Check if input is a directory (parallel execution)
    pathlib_input = Path(cli_params.input)
    files_iter, directory, input_type, input_name, config = resolve_input_information(
        cli_params.input, cli_params.config_path
    )
    files = list(files_iter)

    # Merge CLI params with config file
    edialect, parallelism = merge_runtime_config(cli_params, config)
    if not pathlib_input.exists() or len(files) == 1:
        # Inline query - use polished single-script execution

        run_single_script_execution(
            files=files,
            directory=directory,
            input_type=input_type,
            input_name=input_name,
            edialect=edialect,
            param=cli_params.param,
            conn_args=cli_params.conn_args,
            debug=cli_params.debug,
            execution_mode=execution_mode,
            config=config,
        )
        return
    # Multiple files - use parallel execution
    show_execution_info(input_type, input_name, edialect.value, cli_params.debug)

    # Get execution strategy
    strategy = get_execution_strategy(cli_params.execution_strategy)

    # Set up parallel executor
    parallel_exec = ParallelExecutor(
        max_workers=parallelism,
        dependency_strategy=ETLDependencyStrategy(),
        execution_strategy=strategy,
    )

    # Get execution plan for display
    if pathlib_input.is_dir():
        execution_plan = parallel_exec.get_folder_execution_plan(pathlib_input)
    elif pathlib_input.is_file():
        execution_plan = parallel_exec.get_execution_plan([pathlib_input])
    else:
        raise FileNotFoundError(f"Input path '{pathlib_input}' does not exist.")

    num_edges = execution_plan.number_of_edges()
    num_nodes = execution_plan.number_of_nodes()

    show_parallel_execution_start(
        num_nodes, num_edges, parallelism, cli_params.execution_strategy
    )

    # Factory to create executor for each script
    def executor_factory(node: ScriptNode) -> Executor:
        return create_executor_for_script(
            node,
            cli_params.param,
            cli_params.conn_args,
            edialect,
            cli_params.debug,
            config,
        )

    # Wrap execution_fn to pass quiet=True for parallel execution
    def quiet_execution_fn(exec: Executor, node: ScriptNode) -> None:
        execution_fn(exec, node, quiet=True)

    # Run parallel execution
    summary = parallel_exec.execute(
        root=pathlib_input,
        executor_factory=executor_factory,
        execution_fn=quiet_execution_fn,
        on_script_complete=show_script_result,
    )

    show_parallel_execution_summary(summary)

    if not summary.all_succeeded:
        print_error("Some scripts failed during execution.")
        raise Exit(1)

    print_success("All scripts executed successfully!")
