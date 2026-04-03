"""Tests for display_parallel: parallel execution display functions."""

import importlib
import sys
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import trilogy.scripts.display_core as display_core
from trilogy.scripts.display_parallel import (
    ParallelProgressTracker,
    _make_futures_context_getter,
    show_parallel_execution_start,
    show_parallel_execution_summary,
    show_script_result,
)

RICH_AVAILABLE = False
if importlib.util.find_spec("rich") is not None:
    RICH_AVAILABLE = True


@contextmanager
def capture_stdout():
    old = sys.stdout
    buf = StringIO()
    sys.stdout = buf
    try:
        yield buf
    finally:
        sys.stdout = old


@contextmanager
def capture_rich_output():
    if display_core.RICH_AVAILABLE and RICH_AVAILABLE:
        from rich.console import Console

        buf = StringIO()
        test_console = Console(file=buf, force_terminal=True, width=100)
        original = display_core.console
        display_core.console = test_console
        try:
            yield buf
        finally:
            display_core.console = original
    else:
        with capture_stdout() as buf:
            yield buf


@pytest.fixture(params=[True, False], ids=["rich", "fallback"])
def rich_mode(request):
    if request.param and not RICH_AVAILABLE:
        pytest.skip("Rich not available")
    original = display_core.RICH_AVAILABLE
    display_core.RICH_AVAILABLE = request.param
    if not request.param:
        display_core.console = None
    else:
        try:
            from rich.console import Console

            display_core.console = Console()
        except ImportError:
            display_core.RICH_AVAILABLE = False
    yield request.param
    display_core.RICH_AVAILABLE = original
    if original:
        try:
            from rich.console import Console

            display_core.console = Console()
        except ImportError:
            display_core.console = None
    else:
        display_core.console = None


def _make_script_node(name: str = "script.preql"):
    from trilogy.scripts.dependency import ScriptNode

    return ScriptNode(path=Path(name))


def _make_managed_node(address: str = "my_table"):
    from trilogy.scripts.dependency import ManagedRefreshNode

    owner = _make_script_node()
    return ManagedRefreshNode(address=address, owner_script=owner, assets=[])


def _make_result(node, success=True, duration=1.5, error=None, stats=None):
    from trilogy.scripts.parallel_execution import ExecutionResult

    return ExecutionResult(
        node=node,
        success=success,
        error=error,
        duration=duration,
        stats=stats,
    )


def _make_summary(results):
    from trilogy.scripts.parallel_execution import ParallelExecutionSummary

    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful
    return ParallelExecutionSummary(
        total_scripts=len(results),
        successful=successful,
        skipped=0,
        failed=failed,
        total_duration=3.0,
        results=results,
    )


class TestShowParallelExecutionStart:
    def test_outputs_file_and_dependency_counts(self, rich_mode):
        with capture_rich_output() as buf:
            show_parallel_execution_start(
                num_files=4, num_edges=3, parallelism=2, strategy="eager_bfs"
            )
            captured = buf.getvalue()
        assert "4" in captured
        assert "3" in captured
        assert "2" in captured
        assert "eager_bfs" in captured

    def test_default_strategy(self, rich_mode):
        with capture_rich_output() as buf:
            show_parallel_execution_start(num_files=1, num_edges=0, parallelism=1)
            captured = buf.getvalue()
        assert "eager_bfs" in captured


class TestShowScriptResult:
    def test_script_node_success(self, rich_mode):
        node = _make_script_node("my_script.preql")
        result = _make_result(node, success=True)
        with capture_rich_output() as buf:
            show_script_result(result)
            captured = buf.getvalue()
        assert "my_script.preql" in captured
        assert "1." in captured and "50s" in captured

    def test_script_node_failure(self, rich_mode):
        node = _make_script_node("bad.preql")
        result = _make_result(node, success=False, error="some error")
        with capture_rich_output() as buf:
            show_script_result(result)
            captured = buf.getvalue()
        assert "bad.preql" in captured
        assert "some error" in captured

    def test_managed_node_success(self, rich_mode):
        node = _make_managed_node("schema.my_table")
        result = _make_result(node, success=True)
        with capture_rich_output() as buf:
            show_script_result(result)
            captured = buf.getvalue()
        assert "schema.my_table" in captured

    def test_managed_node_failure(self, rich_mode):
        node = _make_managed_node("schema.bad_table")
        result = _make_result(node, success=False, error="refresh failed")
        with capture_rich_output() as buf:
            show_script_result(result)
            captured = buf.getvalue()
        assert "schema.bad_table" in captured
        assert "refresh failed" in captured


class TestShowParallelExecutionSummary:
    def test_all_successful(self, rich_mode):
        results = [
            _make_result(_make_script_node("a.preql"), success=True),
            _make_result(_make_script_node("b.preql"), success=True),
        ]
        summary = _make_summary(results)
        with capture_rich_output() as buf:
            show_parallel_execution_summary(summary)
            captured = buf.getvalue()
        assert "2" in captured
        assert "0" in captured  # failed count

    def test_with_failures(self, rich_mode):
        results = [
            _make_result(_make_script_node("ok.preql"), success=True),
            _make_result(_make_script_node("fail.preql"), success=False, error="boom"),
        ]
        summary = _make_summary(results)
        with capture_rich_output() as buf:
            show_parallel_execution_summary(summary)
            captured = buf.getvalue()
        assert "fail.preql" in captured
        assert "boom" in captured

    def test_with_stats(self, rich_mode):
        from trilogy.scripts.common import ExecutionStats

        stats = ExecutionStats(update_count=3, validate_count=2, persist_count=1)
        results = [
            _make_result(_make_script_node("a.preql"), success=True, stats=stats)
        ]
        summary = _make_summary(results)
        with capture_rich_output() as buf:
            show_parallel_execution_summary(summary)
            captured = buf.getvalue()
        assert "3" in captured  # update count
        assert "2" in captured  # validate count


class TestParallelProgressTracker:
    def test_enter_exit_no_rich(self):
        original = display_core.RICH_AVAILABLE
        display_core.RICH_AVAILABLE = False
        orig_console = display_core.console
        display_core.console = None
        try:
            tracker = ParallelProgressTracker()
            with tracker:
                assert tracker._progress is None
        finally:
            display_core.RICH_AVAILABLE = original
            display_core.console = orig_console

    def test_on_start_on_complete_no_rich(self):
        original = display_core.RICH_AVAILABLE
        display_core.RICH_AVAILABLE = False
        orig_console = display_core.console
        display_core.console = None
        try:
            node = _make_script_node("work.preql")
            result = _make_result(node, success=True)
            tracker = ParallelProgressTracker()
            with capture_stdout() as buf:
                with tracker:
                    tracker.on_start(node)
                    tracker.on_complete(result)
                captured = buf.getvalue()
            assert "work.preql" in captured
        finally:
            display_core.RICH_AVAILABLE = original
            display_core.console = orig_console

    def test_on_start_managed_node_no_rich(self):
        original = display_core.RICH_AVAILABLE
        display_core.RICH_AVAILABLE = False
        orig_console = display_core.console
        display_core.console = None
        try:
            node = _make_managed_node("my_table")
            tracker = ParallelProgressTracker()
            with capture_stdout() as buf:
                with tracker:
                    tracker.on_start(node)
                captured = buf.getvalue()
            assert "my_table" in captured
        finally:
            display_core.RICH_AVAILABLE = original
            display_core.console = orig_console


class TestMakeFuturesContextGetter:
    def test_returns_active_labels(self):
        node = _make_script_node("active.preql")
        future_done = MagicMock()
        future_done.done.return_value = True
        future_active = MagicMock()
        future_active.done.return_value = False

        futures = {future_done: _make_script_node("done.preql"), future_active: node}
        getter = _make_futures_context_getter(futures)
        result = getter()
        assert "active.preql" in result
        assert "done.preql" not in result

    def test_empty_when_all_done(self):
        future = MagicMock()
        future.done.return_value = True
        futures = {future: _make_script_node("done.preql")}
        getter = _make_futures_context_getter(futures)
        assert getter() == ""


class TestShowParallelSummaryManagedNode:
    def test_failed_managed_node_in_summary(self, rich_mode):
        node = _make_managed_node("schema.tbl")
        result = _make_result(node, success=False, error="boom")
        summary = _make_summary([result])
        with capture_rich_output() as buf:
            show_parallel_execution_summary(summary)
            captured = buf.getvalue()
        assert "schema.tbl" in captured
        assert "boom" in captured


class TestShowScriptResultWithStats:
    def test_shows_stats_string(self, rich_mode):
        from trilogy.scripts.common import ExecutionStats

        node = _make_script_node("a.preql")
        stats = ExecutionStats(update_count=2)
        result = _make_result(node, success=True, stats=stats)
        with capture_rich_output() as buf:
            show_script_result(result)
            captured = buf.getvalue()
        assert "a.preql" in captured


@pytest.mark.skipif(not RICH_AVAILABLE, reason="Rich not available")
class TestParallelProgressTrackerRich:
    def test_enter_exit_with_rich(self):
        from io import StringIO

        from rich.console import Console

        buf = StringIO()
        test_console = Console(file=buf, force_terminal=True, width=80)
        orig = display_core.console
        orig_avail = display_core.RICH_AVAILABLE
        display_core.console = test_console
        display_core.RICH_AVAILABLE = True
        try:
            tracker = ParallelProgressTracker()
            with tracker:
                assert tracker._progress is not None
        finally:
            display_core.console = orig
            display_core.RICH_AVAILABLE = orig_avail

    def test_on_start_and_complete_with_rich(self):
        from io import StringIO

        from rich.console import Console

        buf = StringIO()
        test_console = Console(file=buf, force_terminal=True, width=80)
        orig = display_core.console
        orig_avail = display_core.RICH_AVAILABLE
        display_core.console = test_console
        display_core.RICH_AVAILABLE = True
        try:
            node = _make_script_node("rich_work.preql")
            result = _make_result(node, success=True)
            tracker = ParallelProgressTracker()
            with tracker:
                tracker.on_start(node)
                tracker.on_complete(result)
        finally:
            display_core.console = orig
            display_core.RICH_AVAILABLE = orig_avail
