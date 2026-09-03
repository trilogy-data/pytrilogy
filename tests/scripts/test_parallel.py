from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from trilogy.core import graph as nx
from trilogy.scripts.dependency import ScriptNode
from trilogy.scripts.parallel_execution import (
    OUTCOME_FAILED,
    ExecutionResult,
    RunState,
    _get_next_ready,
    _is_execution_done,
    _mark_node_complete,
    _settle_dependents,
)

# The scheduler keys its state on the graph's string node keys.
CompletedSet = set[str]
ReadyList = list[str]


# ============================================================================
# Fixtures
# ============================================================================
# Graph nodes are string keys (script paths); node_map recovers the ScriptNode.


@pytest.fixture
def node_a() -> str:
    return str(Path("/scripts/a.sql"))


@pytest.fixture
def node_b() -> str:
    return str(Path("/scripts/b.sql"))


@pytest.fixture
def node_c() -> str:
    return str(Path("/scripts/c.sql"))


@pytest.fixture
def node_d() -> str:
    return str(Path("/scripts/d.sql"))


@pytest.fixture
def linear_graph(node_a: str, node_b: str, node_c: str) -> nx.DiGraph:
    """Create a linear dependency graph: A -> B -> C"""
    graph = nx.DiGraph()
    graph.add_nodes_from([node_a, node_b, node_c])
    graph.add_edge(node_a, node_b)  # B depends on A
    graph.add_edge(node_b, node_c)  # C depends on B
    return graph


@pytest.fixture
def diamond_graph(
    node_a: str,
    node_b: str,
    node_c: str,
    node_d: str,
) -> nx.DiGraph:
    """
    Create a diamond dependency graph:
        A
       / \\
      B   C
       \\ /
        D
    """
    graph = nx.DiGraph()
    graph.add_nodes_from([node_a, node_b, node_c, node_d])
    graph.add_edge(node_a, node_b)  # B depends on A
    graph.add_edge(node_a, node_c)  # C depends on A
    graph.add_edge(node_b, node_d)  # D depends on B
    graph.add_edge(node_c, node_d)  # D depends on C
    return graph


# ============================================================================
# Tests for _get_next_ready
# ============================================================================


class TestGetNextReady:
    def test_returns_first_item_from_non_empty_list(
        self, node_a: str, node_b: str
    ) -> None:
        ready = [node_a, node_b]
        result = _get_next_ready(ready)
        assert result == node_a
        assert ready == [node_b]

    def test_returns_none_for_empty_list(self) -> None:
        ready: ReadyList = []
        result = _get_next_ready(ready)
        assert result is None
        assert ready == []

    def test_removes_item_from_list(self, node_a: str) -> None:
        ready = [node_a]
        _get_next_ready(ready)
        assert ready == []

    def test_fifo_order(self, node_a: str, node_b: str, node_c: str) -> None:
        ready = [node_a, node_b, node_c]
        assert _get_next_ready(ready) == node_a
        assert _get_next_ready(ready) == node_b
        assert _get_next_ready(ready) == node_c
        assert _get_next_ready(ready) is None


# ============================================================================
# Tests for _is_execution_done
# ============================================================================


class TestIsExecutionDone:
    def test_returns_true_when_all_complete(self, node_a: str) -> None:
        completed = {node_a}
        assert _is_execution_done(completed, 1) is True

    def test_returns_false_when_incomplete(self, node_a: str) -> None:
        completed = {node_a}
        assert _is_execution_done(completed, 3) is False

    def test_returns_true_when_empty_and_zero_total(self) -> None:
        completed: CompletedSet = set()
        assert _is_execution_done(completed, 0) is True

    def test_returns_true_when_more_than_total(self, node_a: str, node_b: str) -> None:
        # Edge case: shouldn't happen, but function should handle it
        completed = {node_a, node_b}
        assert _is_execution_done(completed, 1) is True


# ============================================================================
# Tests for _settle_dependents
# ============================================================================


class TestSettleDependents:
    """A node that did not succeed skips its unstarted dependents, recursively."""

    def test_marks_direct_dependent_as_failed(
        self, linear_graph: nx.DiGraph, node_a: str, node_b: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.completed.add(node_a)
        state.failed.add(node_a)

        _settle_dependents(state, node_a, OUTCOME_FAILED, None)

        assert node_b in state.completed
        assert node_b in state.failed
        assert len(state.results) == 2  # B and C both failed

    def test_recursively_marks_all_dependents(
        self, linear_graph: nx.DiGraph, node_a: str, node_b: str, node_c: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.completed.add(node_a)
        state.failed.add(node_a)

        _settle_dependents(state, node_a, OUTCOME_FAILED, None)

        assert node_b in state.failed
        assert node_c in state.failed
        assert len(state.results) == 2

    def test_skips_already_completed_nodes(
        self, linear_graph: nx.DiGraph, node_a: str, node_b: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.completed.update({node_a, node_b})  # B already done
        state.failed.add(node_a)

        _settle_dependents(state, node_a, OUTCOME_FAILED, None)

        # C is never reached: recursion stops at the completed B.
        assert len(state.results) == 0

    def test_skips_in_progress_nodes(
        self, linear_graph: nx.DiGraph, node_a: str, node_b: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.completed.add(node_a)
        state.failed.add(node_a)
        state.in_progress.add(node_b)  # B is running

        _settle_dependents(state, node_a, OUTCOME_FAILED, None)

        assert node_b not in state.failed
        assert len(state.results) == 0

    def test_calls_callback_for_each_skipped_node(
        self, linear_graph: nx.DiGraph, node_a: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.completed.add(node_a)
        state.failed.add(node_a)
        callback = Mock()

        _settle_dependents(state, node_a, OUTCOME_FAILED, callback)

        assert callback.call_count == 2  # Called for B and C

    def test_sets_correct_error_message(
        self, linear_graph: nx.DiGraph, node_a: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.completed.add(node_a)
        state.failed.add(node_a)

        _settle_dependents(state, node_a, OUTCOME_FAILED, None)

        for result in state.results:
            assert result.success is False
            assert isinstance(result.error, RuntimeError)
            assert "Skipped due to failed dependency" in str(result.error)
            assert result.duration == 0.0

    def test_handles_node_with_no_dependents(self, node_a: str) -> None:
        graph = nx.DiGraph()
        graph.add_node(node_a)
        state = RunState.for_graph(graph)
        state.completed.add(node_a)
        state.failed.add(node_a)

        _settle_dependents(state, node_a, OUTCOME_FAILED, None)

        assert len(state.results) == 0


# ============================================================================
# Tests for _mark_node_complete
# ============================================================================


class TestMarkNodeComplete:
    def test_adds_node_to_completed(
        self, linear_graph: nx.DiGraph, node_a: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.in_progress.add(node_a)

        _mark_node_complete(state, node_a, True, None)

        assert node_a in state.completed

    def test_removes_node_from_in_progress(
        self, linear_graph: nx.DiGraph, node_a: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.in_progress.add(node_a)

        _mark_node_complete(state, node_a, True, None)

        assert node_a not in state.in_progress

    def test_adds_to_failed_on_failure(
        self, linear_graph: nx.DiGraph, node_a: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.in_progress.add(node_a)

        _mark_node_complete(state, node_a, False, None)

        assert node_a in state.failed

    def test_decrements_dependent_remaining_deps(
        self, linear_graph: nx.DiGraph, node_a: str, node_b: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.in_progress.add(node_a)

        _mark_node_complete(state, node_a, True, None)

        assert state.remaining_deps[node_b] == 0

    def test_adds_to_ready_when_deps_satisfied(
        self, linear_graph: nx.DiGraph, node_a: str, node_b: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.in_progress.add(node_a)

        _mark_node_complete(state, node_a, True, None)

        assert node_b in state.ready

    def test_propagates_failure_to_dependents(
        self, linear_graph: nx.DiGraph, node_a: str, node_b: str, node_c: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.in_progress.add(node_a)

        _mark_node_complete(state, node_a, False, None)

        assert node_b in state.failed
        assert node_c in state.failed
        assert len(state.results) == 2

    def test_skips_already_completed_dependents(
        self, linear_graph: nx.DiGraph, node_a: str, node_b: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.in_progress.add(node_a)
        state.completed.add(node_b)  # B already done

        _mark_node_complete(state, node_a, True, None)

        assert node_b not in state.ready

    def test_diamond_both_deps_must_complete(
        self,
        diamond_graph: nx.DiGraph,
        node_a: str,
        node_b: str,
        node_c: str,
        node_d: str,
    ) -> None:
        """D requires both B and C to complete."""
        state = RunState.for_graph(diamond_graph)
        state.in_progress.add(node_a)
        _mark_node_complete(state, node_a, True, None)

        state.in_progress.add(node_b)
        _mark_node_complete(state, node_b, True, None)

        # D shouldn't be ready yet (still waiting on C)
        assert node_d not in state.ready
        assert state.remaining_deps[node_d] == 1

        state.in_progress.add(node_c)
        _mark_node_complete(state, node_c, True, None)

        assert node_d in state.ready
        assert state.remaining_deps[node_d] == 0

    def test_diamond_one_path_fails(
        self,
        diamond_graph: nx.DiGraph,
        node_a: str,
        node_b: str,
        node_d: str,
    ) -> None:
        """If B fails in diamond, D should fail even though C succeeds."""
        state = RunState.for_graph(diamond_graph)
        state.in_progress.add(node_a)
        _mark_node_complete(state, node_a, True, None)

        state.in_progress.add(node_b)
        _mark_node_complete(state, node_b, False, None)

        assert node_d in state.failed

    def test_calls_callback_for_propagated_failures(
        self, linear_graph: nx.DiGraph, node_a: str
    ) -> None:
        state = RunState.for_graph(linear_graph)
        state.in_progress.add(node_a)
        callback = Mock()

        _mark_node_complete(state, node_a, False, callback)

        # Callback should be called for B and C
        assert callback.call_count == 2


# ============================================================================
# Tests for ExecutionResult dataclass
# ============================================================================


class TestExecutionResult:
    def test_default_values(self) -> None:
        result = ExecutionResult(node=ScriptNode(path=Path("/a.sql")), success=True)
        assert result.error is None
        assert result.duration == 0.0

    def test_with_error(self) -> None:
        error = RuntimeError("Test error")
        result = ExecutionResult(
            node=ScriptNode(path=Path("/a.sql")),
            success=False,
            error=error,
            duration=1.5,
        )
        assert result.error == error
        assert result.duration == 1.5


# ============================================================================
# Integration tests
# ============================================================================


class TestIntegration:
    def test_get_execution_plan_single_file(self, tmp_path: Path) -> None:
        """Test get_execution_plan with a single file."""
        from trilogy.scripts.parallel_execution import ParallelExecutor

        test_file = tmp_path / "test.sql"
        test_file.write_text("SELECT 1;")

        parallel_exec = ParallelExecutor()
        execution_plan = parallel_exec.get_execution_plan([test_file])

        assert execution_plan.number_of_nodes() == 1
        assert execution_plan.number_of_edges() == 0

    def test_full_linear_execution_success(
        self,
        linear_graph: nx.DiGraph,
        node_a: str,
        node_b: str,
        node_c: str,
    ) -> None:
        """Simulate successful execution of A -> B -> C."""
        state = RunState.for_graph(linear_graph)

        for expected in (node_a, node_b, node_c):
            node = _get_next_ready(state.ready)
            assert node == expected
            state.in_progress.add(node)
            _mark_node_complete(state, node, True, None)

        assert _is_execution_done(state.completed, 3) is True
        assert len(state.failed) == 0
        assert len(state.results) == 0  # No failures recorded

    def test_full_linear_execution_early_failure(
        self,
        linear_graph: nx.DiGraph,
        node_a: str,
        node_b: str,
        node_c: str,
    ) -> None:
        """Simulate A fails, B and C should be skipped."""
        state = RunState.for_graph(linear_graph)

        node = _get_next_ready(state.ready)
        state.in_progress.add(node)
        _mark_node_complete(state, node, False, None)

        # All nodes should be marked as completed (A executed, B & C skipped)
        assert _is_execution_done(state.completed, 3) is True
        assert node_a in state.failed
        assert node_b in state.failed
        assert node_c in state.failed
        assert len(state.results) == 2  # B and C recorded as failed


# ── EagerBFSStrategy callback exception resilience ────────────────────────────


class TestEagerBFSCallbackException:
    """on_script_complete raising must not kill the worker thread."""

    def test_callback_exception_does_not_abort_execution(self, node_a: str) -> None:
        from trilogy.scripts.dependency import DependencyResolver
        from trilogy.scripts.parallel_execution import (
            EagerBFSStrategy,
            ExecutionResult,
        )

        graph = nx.DiGraph()
        graph.add_node(node_a)

        def executor_factory(node: ScriptNode) -> None:
            return None

        def execution_fn(executor: None, node: ScriptNode) -> Any:
            return None  # success; _execute_single wraps the result

        def bad_callback(result: ExecutionResult) -> None:
            raise RuntimeError("callback exploded")

        # _execute_single is the real wrapper; mock it to return a successful result
        # without needing a real executor or file system.
        from unittest import mock

        successful_result = ExecutionResult(
            node=ScriptNode(path=Path(node_a)), success=True
        )
        resolver = Mock(spec=DependencyResolver)

        strategy = EagerBFSStrategy()
        with mock.patch(
            "trilogy.scripts.parallel_execution._execute_single",
            return_value=successful_result,
        ):
            results = strategy.execute(
                graph=graph,
                resolver=resolver,
                max_workers=1,
                executor_factory=executor_factory,
                execution_fn=execution_fn,
                on_script_complete=bad_callback,
            )

        # Execution must still return the result despite the callback exception.
        assert len(results) == 1
        assert results[0].success is True
