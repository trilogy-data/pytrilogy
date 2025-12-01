import importlib
import re
import sys
from contextlib import contextmanager
from datetime import timedelta
from io import StringIO

import pytest

from trilogy.scripts import display

RICH_AVAILABLE = False
if importlib.util.find_spec("rich") is not None:
    RICH_AVAILABLE = True


def strip_ansi(text):
    """Remove ANSI escape sequences from text."""
    ansi_escape = re.compile(r"\x1b\[[0-9;]*m")
    return ansi_escape.sub("", text)


@pytest.mark.skipif(not RICH_AVAILABLE, reason="Rich library not available")
def test_rich_available():
    """Test Rich availability functionality - only runs when Rich is available."""
    assert display.is_rich_available() is True
    with display.set_rich_mode(False):
        assert display.is_rich_available() is False
    assert display.is_rich_available() is True
    with display.set_rich_mode(True):
        assert display.is_rich_available() is True
    assert display.is_rich_available() is True
    display.RICH_AVAILABLE = False
    assert display.is_rich_available() is False
    with display.set_rich_mode(True):
        assert display.is_rich_available() is True
    assert display.is_rich_available() is False
    with display.set_rich_mode(False):
        assert display.is_rich_available() is False
    assert display.is_rich_available() is False
    display.RICH_AVAILABLE = True


@pytest.fixture(params=[True, False], ids=["rich_enabled", "rich_disabled"])
def rich_mode(request):
    """
    Parameterized fixture that runs each test with Rich enabled and disabled.
    When Rich is not available, only runs with rich_disabled.

    Args:
        request.param: True for Rich enabled, False for Rich disabled

    Yields:
        bool: Actual Rich mode state (may differ from request if Rich unavailable)
    """
    # Skip Rich enabled tests if Rich is not available
    if request.param and not RICH_AVAILABLE:
        pytest.skip("Rich library not available, skipping rich_enabled test")

    # Store original state
    original_state = display.is_rich_available()

    # Attempt to set the Rich mode using dynamic switching
    display.set_rich_mode(request.param)

    yield request.param

    # Cleanup: restore original state
    display.set_rich_mode(original_state)


@contextmanager
def capture_all_output():
    """Capture both stdout and stderr to catch all possible output."""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    stdout_capture = StringIO()
    stderr_capture = StringIO()

    try:
        sys.stdout = stdout_capture
        sys.stderr = stderr_capture
        yield stdout_capture, stderr_capture
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr


@contextmanager
def capture_rich_console_output():
    """Create a Rich console that captures to a StringIO buffer."""
    if display.is_rich_available() and RICH_AVAILABLE:
        from rich.console import Console

        # Create a console that writes to a string buffer
        output_buffer = StringIO()
        test_console = Console(file=output_buffer, force_terminal=True, width=80)

        # Temporarily replace the display module's console
        original_console = display.console
        display.console = test_console

        try:
            yield output_buffer
        finally:
            display.console = original_console
    else:
        # If Rich isn't available, just use regular output capture
        with capture_all_output() as (stdout, stderr):
            yield stdout


class TestPrintFunctions:
    """Test all print functions in both Rich and fallback modes."""

    def test_print_success(self, rich_mode):
        """Test print_success outputs correctly in both modes."""
        message = "Test success message"

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.print_success(message)
                captured = output.getvalue()
                assert message in captured
                # Rich output should contain ANSI escape codes for styling
                assert "\x1b[" in captured  # ANSI escape sequence for color
        else:
            with capture_all_output() as (stdout, stderr):
                display.print_success(message)
                captured = stdout.getvalue() + stderr.getvalue()
                assert message in captured

    def test_print_info(self, rich_mode):
        """Test print_info outputs correctly in both modes."""
        message = "Test info message"

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.print_info(message)
                captured = output.getvalue()
                assert message in captured
                assert "\x1b[" in captured  # Should have color codes
        else:
            with capture_all_output() as (stdout, stderr):
                display.print_info(message)
                captured = stdout.getvalue() + stderr.getvalue()
                assert message in captured

    def test_print_warning(self, rich_mode):
        """Test print_warning outputs correctly in both modes."""
        message = "Test warning message"

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.print_warning(message)
                captured = output.getvalue()
                assert message in captured
                assert "\x1b[" in captured
        else:
            with capture_all_output() as (stdout, stderr):
                display.print_warning(message)
                captured = stdout.getvalue() + stderr.getvalue()
                assert message in captured

    def test_print_error(self, rich_mode):
        """Test print_error outputs correctly in both modes."""
        message = "Test error message"

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.print_error(message)
                captured = output.getvalue()
                assert message in captured
                assert "\x1b[" in captured
        else:
            with capture_all_output() as (stdout, stderr):
                display.print_error(message)
                captured = stdout.getvalue() + stderr.getvalue()
                assert message in captured

    def test_print_header(self, rich_mode):
        """Test print_header outputs correctly in both modes."""
        message = "Test header message"

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.print_header(message)
                captured = output.getvalue()
                assert message in captured
                assert "\x1b[" in captured
        else:
            with capture_all_output() as (stdout, stderr):
                display.print_header(message)
                captured = stdout.getvalue() + stderr.getvalue()
                assert message in captured

    def test_different_styles_produce_different_output(self, rich_mode):
        """Test that different print functions produce visibly different output."""
        if not rich_mode or not RICH_AVAILABLE:
            pytest.skip(
                "Rich styling test only meaningful when Rich is available and enabled"
            )

        message = "Same message"

        with capture_rich_console_output() as output:
            display.print_success(message)
            success_output = output.getvalue()

        with capture_rich_console_output() as output:
            display.print_error(message)
            error_output = output.getvalue()

        with capture_rich_console_output() as output:
            display.print_warning(message)
            warning_output = output.getvalue()

        # All should contain the message
        assert message in success_output
        assert message in error_output
        assert message in warning_output

        # But the styling should be different (different ANSI codes)
        assert success_output != error_output
        assert success_output != warning_output
        assert error_output != warning_output


class TestUtilityFunctions:
    """Test utility functions."""

    def test_format_duration_milliseconds(self, rich_mode):
        """Test duration formatting for milliseconds."""
        duration = timedelta(milliseconds=500)
        result = display.format_duration(duration)
        assert result == "500ms"

    def test_format_duration_seconds(self, rich_mode):
        """Test duration formatting for seconds."""
        duration = timedelta(seconds=2.5)
        result = display.format_duration(duration)
        assert result == "2.50s"

    def test_format_duration_minutes(self, rich_mode):
        """Test duration formatting for minutes."""
        duration = timedelta(seconds=125.75)
        result = display.format_duration(duration)
        assert result == "2m 5.75s"

    def test_is_rich_available_reflects_mode(self, rich_mode):
        """Test that is_rich_available reflects the current mode."""
        expected = rich_mode and RICH_AVAILABLE
        assert display.is_rich_available() == expected


class TestDisplayFunctions:
    """Test display functions in both modes."""

    def test_show_execution_info(self, rich_mode):
        """Test show_execution_info display outputs correctly."""
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_execution_info(
                    input_type="file",
                    input_name="test.sql",
                    dialect="postgresql",
                    debug=True,
                )
                captured = output.getvalue()
                assert "test.sql" in captured
                assert "postgresql" in captured
                assert "Debug" in captured
                # Rich output should have box drawing characters for panels
                assert any(
                    char in captured
                    for char in ["┌", "│", "┐", "└", "┘", "╭", "╮", "╯", "╰"]
                )
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_execution_info(
                    input_type="file",
                    input_name="test.sql",
                    dialect="postgresql",
                    debug=True,
                )
                captured = stdout.getvalue() + stderr.getvalue()
                assert "test.sql" in captured
                assert "postgresql" in captured
                assert "True" in captured

    def test_show_environment_params_with_params(self, rich_mode):
        """Test show_environment_params with parameters."""
        env_params = {"key1": "value1", "key2": "value2"}

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_environment_params(env_params)
                captured = output.getvalue()
                assert "Environment parameters" in captured
                assert "key1" in captured
                assert "value1" in captured
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_environment_params(env_params)
                captured = stdout.getvalue() + stderr.getvalue()
                assert "Environment parameters" in captured
                assert "key1" in captured

    def test_show_environment_params_empty(self, rich_mode):
        """Test show_environment_params with empty dict."""
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_environment_params({})
                captured = output.getvalue()
                assert captured.strip() == ""  # Should be empty
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_environment_params({})
                captured = stdout.getvalue() + stderr.getvalue()
                assert captured.strip() == ""  # Should be empty

    def test_show_debug_mode(self, rich_mode):
        """Test show_debug_mode display."""
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_debug_mode()
                captured = output.getvalue()
                assert "Debug mode enabled" in captured
                # Should have panel formatting
                assert any(
                    char in captured
                    for char in ["┌", "│", "┐", "└", "┘", "╭", "╮", "╯", "╰"]
                )
        else:
            # Fallback mode doesn't implement this function - skip test
            pytest.skip("show_debug_mode not implemented in fallback mode")

    def test_show_statement_type(self, rich_mode):
        """Test show_statement_type with different configurations."""
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_statement_type(idx=0, total=3, statement_type="SELECT")
                captured = output.getvalue()
                assert "Statement" in captured
                assert "SELECT" in captured
                assert "\x1b[" in captured  # Should have styling
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_statement_type(idx=0, total=3, statement_type="SELECT")
                captured = stdout.getvalue() + stderr.getvalue()
                assert "Statement 1/3" in captured
                assert "SELECT" in captured


class TestProgressAndExecution:
    """Test progress and execution display functions."""

    def test_show_execution_start(self, rich_mode):
        """Test show_execution_start with different statement counts."""
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_execution_start(1)
                captured = output.getvalue()
                assert "1 statement" in strip_ansi(captured)
                assert "\x1b[" in captured  # Bold formatting

            with capture_rich_console_output() as output:
                display.show_execution_start(5)
                captured = output.getvalue()
                assert "5 statements" in strip_ansi(captured)
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_execution_start(1)
                captured = stdout.getvalue() + stderr.getvalue()
                assert "1 statement" in captured

            with capture_all_output() as (stdout, stderr):
                display.show_execution_start(5)
                captured = stdout.getvalue() + stderr.getvalue()
                assert "5 statements" in captured

    def test_show_statement_result_success_with_results(self, rich_mode):
        """Test show_statement_result for successful execution with results."""
        duration = timedelta(seconds=1.5)

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_statement_result(
                    idx=0, total=2, duration=duration, has_results=True, error=None
                )
                captured = output.getvalue()
                assert "Statement 1/2" in strip_ansi(captured)
                assert "Results" in strip_ansi(captured)
                assert "1.50s" in strip_ansi(captured)
                assert "\x1b[" in captured  # Styling
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_statement_result(
                    idx=0, total=2, duration=duration, has_results=True, error=None
                )
                captured = stdout.getvalue() + stderr.getvalue()
                assert "Statement 1/2" in captured
                assert "1.50s" in captured

    def test_show_statement_result_success_no_results(self, rich_mode):
        """Test show_statement_result for successful execution without results."""
        duration = timedelta(seconds=0.8)

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_statement_result(
                    idx=1, total=2, duration=duration, has_results=False, error=None
                )
                captured = output.getvalue()
                assert "Statement 2/2" in strip_ansi(captured)
                assert "completed" in strip_ansi(captured)
                assert "800ms" in strip_ansi(captured)
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_statement_result(
                    idx=1, total=2, duration=duration, has_results=False, error=None
                )
                captured = stdout.getvalue() + stderr.getvalue()
                assert "Statement 2/2" in captured
                assert "completed" in captured
                assert "800ms" in captured

    def test_show_statement_result_error(self, rich_mode):
        """Test show_statement_result for failed execution."""
        duration = timedelta(seconds=0.1)

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_statement_result(
                    idx=0,
                    total=1,
                    duration=duration,
                    has_results=False,
                    error="Syntax error near 'SELECT'",
                    exception_type=ValueError,
                )
                captured = output.getvalue()
                assert "Statement 1" in strip_ansi(captured)
                assert "failed" in strip_ansi(captured)
                assert "Syntax error near 'SELECT'" in strip_ansi(captured)
                assert "\x1b[" in captured  # Error styling
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_statement_result(
                    idx=0,
                    total=1,
                    duration=duration,
                    has_results=False,
                    error="Syntax error near 'SELECT'",
                    exception_type=ValueError,
                )
                captured = stdout.getvalue() + stderr.getvalue()
                assert "Statement 1" in captured
                assert "failed" in captured
                assert "Syntax error near 'SELECT'" in captured

    def test_show_execution_summary(self, rich_mode):
        """Test show_execution_summary."""
        total_duration = timedelta(seconds=10.5)

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_execution_summary(3, total_duration, True)
                captured = output.getvalue()
                assert "Execution Complete" in captured
                assert "10.50s" in captured
                assert "3" in captured
                # Should have panel formatting
                assert any(
                    char in captured
                    for char in ["┌", "│", "┐", "└", "┘", "╭", "╮", "╯", "╰"]
                )
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_execution_summary(3, total_duration, True)
                captured = stdout.getvalue() + stderr.getvalue()
                assert "Statements: 3" in captured
                assert "10.50s" in captured

    def test_show_formatting_result(self, rich_mode):
        """Test show_formatting_result."""
        duration = timedelta(seconds=2.3)

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_formatting_result(
                    filename="test.sql", num_queries=5, duration=duration
                )
                captured = output.getvalue()
                assert "test.sql" in captured
                assert "5" in captured
                assert "2.30s" in strip_ansi(captured)
                assert "\x1b[" in captured  # Should have styling
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_formatting_result(
                    filename="test.sql", num_queries=5, duration=duration
                )
                captured = stdout.getvalue() + stderr.getvalue()
                assert "5 queries" in captured
                assert "2.30s" in captured


class TestResultsTable:
    """Test results table functionality."""

    def test_print_results_table_no_results(self, rich_mode):
        results = display.ResultSet(rows=[], columns=["col1", "col2"])
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.print_results_table(results)
                captured = output.getvalue()
                assert "No results returned" in captured
        else:
            with capture_all_output() as (stdout, stderr):
                display.print_results_table(results)
                captured = stdout.getvalue() + stderr.getvalue()
                assert "Install rich for prettier table output" in captured
                assert "col1, col2" in captured

    def test_print_results_table_with_results(self, rich_mode):
        """Test print_results_table with actual data."""

        results = display.ResultSet(
            rows=[
                ("value1", "value2"),
                ("value3", None),
                ("value4", "value5"),
            ],
            columns=["column1", "column2"],
        )
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.print_results_table(results)
                captured = output.getvalue()
                assert "value1" in captured
                assert "value2" in captured
                assert "value3" in captured
                assert "column1" in captured
                assert "column2" in captured
                # Rich tables have box drawing characters
                assert any(
                    char in captured
                    for char in ["┌", "│", "┐", "└", "┘", "─", "┼", "├", "┤"]
                )
        else:
            with capture_all_output() as (stdout, stderr):
                display.print_results_table(results)
                captured = stdout.getvalue() + stderr.getvalue()
                assert "Install rich for prettier table output" in captured
                assert "column1, column2" in captured
                assert "value1" in captured
                assert "---" in captured  # Separator

    def test_print_results_table_large_dataset(self, rich_mode):
        """Test print_results_table with large dataset to test truncation."""

        large_results = [(f"row{i}", f"data{i}") for i in range(100)]

        results = display.ResultSet(rows=large_results, columns=["col1", "col2"])
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.print_results_table(results)
                captured = output.getvalue()
                # Should show truncation message
                assert "Showing first 50" in strip_ansi(captured)
                assert "Result set was larger" in strip_ansi(captured)
                # Should contain some data
                assert "row0" in captured
                # Box drawing characters for table
                assert any(char in captured for char in ["┌", "│", "┐", "└", "┘", "─"])
        else:
            with capture_all_output() as (stdout, stderr):
                display.print_results_table(results)
                captured = stdout.getvalue() + stderr.getvalue()
                # Fallback shows all results
                assert "row0" in captured
                assert "row50" in captured


class TestContextManagers:
    """Test context managers with actual output."""

    def test_with_status_context_manager(self, rich_mode):
        """Test with_status as context manager."""
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output():
                with display.with_status("Testing operation"):
                    pass
                # Rich status doesn't write to output until completion
                # But we can test it doesn't crash
        else:
            with capture_all_output() as (stdout, stderr):
                with display.with_status("Testing operation"):
                    pass
                captured = stdout.getvalue() + stderr.getvalue()
                assert "Testing operation" in captured

    def test_with_status_exception_handling(self, rich_mode):
        """Test with_status handles exceptions properly."""
        try:
            with display.with_status("Operation with error"):
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected
        # Main test is that no additional exceptions are raised


class TestDynamicSwitching:
    """Test dynamic mode switching functionality with actual output validation."""

    @pytest.mark.skipif(not RICH_AVAILABLE, reason="Rich library not available")
    def test_mode_switching_changes_output_behavior(self):
        """Test that mode switching actually changes output behavior."""
        original_state = display.is_rich_available()
        message = "Test message"

        try:
            # Test fallback mode output
            display.set_rich_mode(False)
            with capture_all_output() as (stdout, stderr):
                display.print_success(message)
                fallback_output = stdout.getvalue() + stderr.getvalue()

            # Test Rich mode output (if available)
            display.set_rich_mode(True)
            if display.is_rich_available():
                with capture_rich_console_output() as output:
                    display.print_success(message)
                    rich_output = output.getvalue()

                # Both should contain the message but be formatted differently
                assert message in fallback_output
                assert message in rich_output

                # Rich output should have ANSI codes, fallback might not (depends on click)
                # At minimum, they should be different if Rich adds any formatting
                if "\x1b[" in rich_output:
                    assert rich_output != fallback_output
        finally:
            # Restore original state
            display.set_rich_mode(original_state)


class TestActualFormattingDifferences:
    """Test that Rich and fallback modes actually produce different formatted output."""

    def test_panel_vs_plain_output(self, rich_mode):
        """Test that Rich panels look different from fallback output."""
        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.show_execution_info(
                    input_type="file",
                    input_name="test.sql",
                    dialect="postgresql",
                    debug=False,
                )
                rich_output = output.getvalue()

                # Rich should have box drawing characters
                has_box_chars = any(
                    char in repr(rich_output)
                    for char in ["┌", "|", "┐", "└", "┘", "╭", "╮", "╯", "╰"]
                )
                assert (
                    has_box_chars
                ), f"Expected box drawing characters in Rich output: {repr(rich_output)}"
        else:
            with capture_all_output() as (stdout, stderr):
                display.show_execution_info(
                    input_type="file",
                    input_name="test.sql",
                    dialect="postgresql",
                    debug=False,
                )
                fallback_output = stdout.getvalue() + stderr.getvalue()

                # Fallback should not have box drawing characters
                has_box_chars = any(
                    char in fallback_output
                    for char in ["┌", "│", "┐", "└", "┘", "╭", "╮", "╯", "╰"]
                )
                assert (
                    not has_box_chars
                ), f"Did not expect box drawing characters in fallback output: {repr(fallback_output)}"

    def test_table_vs_plain_list_output(self, rich_mode):
        """Test that Rich tables look different from fallback output."""

        query = display.ResultSet(
            rows=[("Alice", 30), ("Bob", 25)], columns=["name", "age"]
        )

        if rich_mode and RICH_AVAILABLE:
            with capture_rich_console_output() as output:
                display.print_results_table(query)
                rich_output = output.getvalue()

                # Rich tables should have table formatting characters
                has_table_chars = any(
                    char in repr(rich_output)
                    for char in ["┌", "│", "┐", "└", "┘", "─", "┼", "├", "┤"]
                )
                assert (
                    has_table_chars
                ), f"Expected table characters in Rich output: {repr(rich_output)}"
        else:
            with capture_all_output() as (stdout, stderr):
                display.print_results_table(query)
                fallback_output = stdout.getvalue() + stderr.getvalue()

                # Fallback should have comma-separated headers and simple tuples
                assert "name, age" in fallback_output
                assert "Alice" in fallback_output
                assert "---" in fallback_output  # Simple separator

                # Should not have table drawing characters
                has_table_chars = any(
                    char in fallback_output
                    for char in ["┌", "│", "┐", "└", "┘", "─", "┼", "├", "┤"]
                )
                assert (
                    not has_table_chars
                ), f"Did not expect table characters in fallback output: {repr(fallback_output)}"


def test_create_progress_context():
    """Test create_progress_context with different query counts."""
    original_state = display.is_rich_available()

    try:
        # Test with Rich enabled (if available)
        if RICH_AVAILABLE:
            display.set_rich_mode(True)
            context = display.create_progress_context()
            assert context is not None
            assert hasattr(context, "__enter__") and hasattr(context, "__exit__")
    finally:
        # Restore original state
        display.set_rich_mode(original_state)
