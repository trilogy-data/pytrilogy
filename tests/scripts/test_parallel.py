from pathlib import Path
from typing import Any
from unittest.mock import Mock

import networkx as nx
import pytest

from trilogy.scripts.parallel_execution import (
    ExecutionResult,
    ScriptNode,
    _get_next_ready,
    _is_execution_done,
    _mark_node_complete,
    _propagate_failure,
)

# Type aliases
CompletedSet = set[ScriptNode]
FailedSet = set[ScriptNode]
InProgressSet = set[ScriptNode]
ResultsList = list[ExecutionResult]
RemainingDepsDict = dict[ScriptNode, int]
ReadyList = list[ScriptNode]
OnCompleteCallback = Any  # Callable[[ExecutionResult], None] | None


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def node_a() -> ScriptNode:
    return ScriptNode(path=Path("/scripts/a.sql"))


@pytest.fixture
def node_b() -> ScriptNode:
    return ScriptNode(path=Path("/scripts/b.sql"))


@pytest.fixture
def node_c() -> ScriptNode:
    return ScriptNode(path=Path("/scripts/c.sql"))


@pytest.fixture
def node_d() -> ScriptNode:
    return ScriptNode(path=Path("/scripts/d.sql"))


@pytest.fixture
def linear_graph(
    node_a: ScriptNode, node_b: ScriptNode, node_c: ScriptNode
) -> nx.DiGraph:
    """Create a linear dependency graph: A -> B -> C"""
    graph = nx.DiGraph()
    graph.add_nodes_from([node_a, node_b, node_c])
    graph.add_edge(node_a, node_b)  # B depends on A
    graph.add_edge(node_b, node_c)  # C depends on B
    return graph


@pytest.fixture
def diamond_graph(
    node_a: ScriptNode,
    node_b: ScriptNode,
    node_c: ScriptNode,
    node_d: ScriptNode,
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
        self, node_a: ScriptNode, node_b: ScriptNode
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

    def test_removes_item_from_list(self, node_a: ScriptNode) -> None:
        ready = [node_a]
        _get_next_ready(ready)
        assert ready == []

    def test_fifo_order(
        self, node_a: ScriptNode, node_b: ScriptNode, node_c: ScriptNode
    ) -> None:
        ready = [node_a, node_b, node_c]
        assert _get_next_ready(ready) == node_a
        assert _get_next_ready(ready) == node_b
        assert _get_next_ready(ready) == node_c
        assert _get_next_ready(ready) is None


# ============================================================================
# Tests for _is_execution_done
# ============================================================================


class TestIsExecutionDone:
    def test_returns_true_when_all_complete(self, node_a: ScriptNode) -> None:
        completed = {node_a}
        assert _is_execution_done(completed, 1) is True

    def test_returns_false_when_incomplete(self, node_a: ScriptNode) -> None:
        completed = {node_a}
        assert _is_execution_done(completed, 3) is False

    def test_returns_true_when_empty_and_zero_total(self) -> None:
        completed: CompletedSet = set()
        assert _is_execution_done(completed, 0) is True

    def test_returns_true_when_more_than_total(
        self, node_a: ScriptNode, node_b: ScriptNode
    ) -> None:
        # Edge case: shouldn't happen, but function should handle it
        completed = {node_a, node_b}
        assert _is_execution_done(completed, 1) is True


# ============================================================================
# Tests for _propagate_failure
# ============================================================================


class TestPropagateFailure:
    def test_marks_direct_dependent_as_failed(
        self, linear_graph: nx.DiGraph, node_a: ScriptNode, node_b: ScriptNode
    ) -> None:
        completed: CompletedSet = {node_a}
        failed: FailedSet = {node_a}
        in_progress: InProgressSet = set()
        results: ResultsList = []

        _propagate_failure(
            node_a, linear_graph, completed, in_progress, results, failed, None
        )

        assert node_b in completed
        assert node_b in failed
        assert len(results) == 2  # B and C both failed

    def test_recursively_marks_all_dependents(
        self,
        linear_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
    ) -> None:
        completed: CompletedSet = {node_a}
        failed: FailedSet = {node_a}
        in_progress: InProgressSet = set()
        results: ResultsList = []

        _propagate_failure(
            node_a, linear_graph, completed, in_progress, results, failed, None
        )

        assert node_b in failed
        assert node_c in failed
        assert len(results) == 2

    def test_skips_already_completed_nodes(
        self,
        linear_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
    ) -> None:
        completed: CompletedSet = {node_a, node_b}  # B already done
        failed: FailedSet = {node_a}
        in_progress: InProgressSet = set()
        results: ResultsList = []

        _propagate_failure(
            node_a, linear_graph, completed, in_progress, results, failed, None
        )

        # B is skipped because it's already completed
        # C is NOT marked because propagate_failure only looks at direct successors
        # and B (the successor of A) is already completed, so recursion stops there
        assert node_b not in results
        assert len(results) == 0  # No new failures - B was already done

    def test_skips_in_progress_nodes(
        self, linear_graph: nx.DiGraph, node_a: ScriptNode, node_b: ScriptNode
    ) -> None:
        completed: CompletedSet = {node_a}
        failed: FailedSet = {node_a}
        in_progress: InProgressSet = {node_b}  # B is running
        results: ResultsList = []

        _propagate_failure(
            node_a, linear_graph, completed, in_progress, results, failed, None
        )

        # B should be skipped since it's in progress
        assert node_b not in failed
        # C is NOT marked because B (A's successor) is in_progress,
        # so propagation stops there and doesn't recurse
        assert len(results) == 0

    def test_calls_callback_for_each_failed_node(
        self, linear_graph: nx.DiGraph, node_a: ScriptNode
    ) -> None:
        completed: CompletedSet = {node_a}
        failed: FailedSet = {node_a}
        in_progress: InProgressSet = set()
        results: ResultsList = []
        callback = Mock()

        _propagate_failure(
            node_a, linear_graph, completed, in_progress, results, failed, callback
        )

        assert callback.call_count == 2  # Called for B and C

    def test_sets_correct_error_message(
        self, linear_graph: nx.DiGraph, node_a: ScriptNode
    ) -> None:
        completed: CompletedSet = {node_a}
        failed: FailedSet = {node_a}
        in_progress: InProgressSet = set()
        results: ResultsList = []

        _propagate_failure(
            node_a, linear_graph, completed, in_progress, results, failed, None
        )

        for result in results:
            assert result.success is False
            assert isinstance(result.error, RuntimeError)
            assert "Skipped due to failed dependency" in str(result.error)
            assert result.duration == 0.0

    def test_handles_empty_graph(self, node_a: ScriptNode) -> None:
        graph = nx.DiGraph()
        graph.add_node(node_a)  # No edges
        completed: CompletedSet = {node_a}
        failed: FailedSet = {node_a}
        in_progress: InProgressSet = set()
        results: ResultsList = []

        _propagate_failure(node_a, graph, completed, in_progress, results, failed, None)

        assert len(results) == 0  # No dependents to mark


# ============================================================================
# Tests for _mark_node_complete
# ============================================================================


class TestMarkNodeComplete:
    def test_adds_node_to_completed(
        self,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
        linear_graph: nx.DiGraph,
    ) -> None:
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_a}
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 1, node_c: 1}
        ready: ReadyList = []
        results: ResultsList = []

        _mark_node_complete(
            node_a,
            True,
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        assert node_a in completed

    def test_removes_node_from_in_progress(
        self,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
        linear_graph: nx.DiGraph,
    ) -> None:
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_a}
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 1, node_c: 1}
        ready: ReadyList = []
        results: ResultsList = []

        _mark_node_complete(
            node_a,
            True,
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        assert node_a not in in_progress

    def test_adds_to_failed_on_failure(
        self, node_a: ScriptNode, linear_graph: nx.DiGraph
    ) -> None:
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_a}
        remaining_deps: RemainingDepsDict = {node_a: 0}
        ready: ReadyList = []
        results: ResultsList = []

        _mark_node_complete(
            node_a,
            False,  # Failed
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        assert node_a in failed

    def test_decrements_dependent_remaining_deps(
        self,
        linear_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
    ) -> None:
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_a}
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 1, node_c: 1}
        ready: ReadyList = []
        results: ResultsList = []

        _mark_node_complete(
            node_a,
            True,
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        assert remaining_deps[node_b] == 0

    def test_adds_to_ready_when_deps_satisfied(
        self,
        linear_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
    ) -> None:
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_a}
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 1, node_c: 1}
        ready: ReadyList = []
        results: ResultsList = []

        _mark_node_complete(
            node_a,
            True,
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        assert node_b in ready

    def test_propagates_failure_to_dependents(
        self,
        linear_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
    ) -> None:
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_a}
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 1, node_c: 1}
        ready: ReadyList = []
        results: ResultsList = []

        _mark_node_complete(
            node_a,
            False,  # Failed
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        assert node_b in failed
        assert node_c in failed
        assert len(results) == 2

    def test_skips_already_completed_dependents(
        self,
        linear_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
    ) -> None:
        completed: CompletedSet = {node_b}  # B already done
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_a}
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 0, node_c: 1}
        ready: ReadyList = []
        results: ResultsList = []

        _mark_node_complete(
            node_a,
            True,
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        # B was already completed, shouldn't be added to ready
        assert node_b not in ready

    def test_diamond_both_deps_must_complete(
        self,
        diamond_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
        node_d: ScriptNode,
    ) -> None:
        """D requires both B and C to complete."""
        completed: CompletedSet = {node_a}
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_b}
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 0, node_c: 0, node_d: 2}
        ready: ReadyList = []
        results: ResultsList = []

        # Complete B
        _mark_node_complete(
            node_b,
            True,
            diamond_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        # D shouldn't be ready yet (still waiting on C)
        assert node_d not in ready
        assert remaining_deps[node_d] == 1

        # Complete C
        in_progress.add(node_c)
        _mark_node_complete(
            node_c,
            True,
            diamond_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        # Now D should be ready
        assert node_d in ready
        assert remaining_deps[node_d] == 0

    def test_diamond_one_path_fails(
        self,
        diamond_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
        node_d: ScriptNode,
    ) -> None:
        """If B fails in diamond, D should fail even though C succeeds."""
        completed: CompletedSet = {node_a}
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_b}
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 0, node_c: 0, node_d: 2}
        ready: ReadyList = []
        results: ResultsList = []

        # B fails
        _mark_node_complete(
            node_b,
            False,
            diamond_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        # D should be failed
        assert node_d in failed

    def test_calls_callback_for_propagated_failures(
        self,
        linear_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
    ) -> None:
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = {node_a}
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 1, node_c: 1}
        ready: ReadyList = []
        results: ResultsList = []
        callback = Mock()

        _mark_node_complete(
            node_a,
            False,
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            callback,
        )

        # Callback should be called for B and C
        assert callback.call_count == 2


# ============================================================================
# Tests for ExecutionResult dataclass
# ============================================================================


class TestExecutionResult:
    def test_default_values(self, node_a: ScriptNode) -> None:
        result = ExecutionResult(node=node_a, success=True)
        assert result.error is None
        assert result.duration == 0.0

    def test_with_error(self, node_a: ScriptNode) -> None:
        error = RuntimeError("Test error")
        result = ExecutionResult(node=node_a, success=False, error=error, duration=1.5)
        assert result.error == error
        assert result.duration == 1.5


# ============================================================================
# Integration tests
# ============================================================================


class TestIntegration:
    def test_full_linear_execution_success(
        self,
        linear_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
    ) -> None:
        """Simulate successful execution of A -> B -> C."""
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = set()
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 1, node_c: 1}
        ready: ReadyList = [node_a]
        results: ResultsList = []

        # Execute A
        node = _get_next_ready(ready)
        assert node == node_a
        in_progress.add(node)
        _mark_node_complete(
            node,
            True,
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        # B should now be ready
        assert node_b in ready
        assert _is_execution_done(completed, 3) is False

        # Execute B
        node = _get_next_ready(ready)
        assert node == node_b
        in_progress.add(node)
        _mark_node_complete(
            node,
            True,
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        # C should now be ready
        assert node_c in ready

        # Execute C
        node = _get_next_ready(ready)
        assert node == node_c
        in_progress.add(node)
        _mark_node_complete(
            node,
            True,
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        # All done
        assert _is_execution_done(completed, 3) is True
        assert len(failed) == 0
        assert len(results) == 0  # No failures recorded

    def test_full_linear_execution_early_failure(
        self,
        linear_graph: nx.DiGraph,
        node_a: ScriptNode,
        node_b: ScriptNode,
        node_c: ScriptNode,
    ) -> None:
        """Simulate A fails, B and C should be skipped."""
        completed: CompletedSet = set()
        failed: FailedSet = set()
        in_progress: InProgressSet = set()
        remaining_deps: RemainingDepsDict = {node_a: 0, node_b: 1, node_c: 1}
        ready: ReadyList = [node_a]
        results: ResultsList = []

        # Execute A (fails)
        node = _get_next_ready(ready)
        in_progress.add(node)
        _mark_node_complete(
            node,
            False,  # Failed!
            linear_graph,
            completed,
            failed,
            in_progress,
            remaining_deps,
            ready,
            results,
            None,
        )

        # All nodes should be marked as completed (A executed, B & C skipped)
        assert _is_execution_done(completed, 3) is True
        assert node_a in failed
        assert node_b in failed
        assert node_c in failed
        assert len(results) == 2  # B and C recorded as failed
