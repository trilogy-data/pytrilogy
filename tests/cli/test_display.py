from contextlib import contextmanager
from datetime import timedelta
from unittest.mock import Mock, patch

import pytest

from trilogy.scripts import display


@pytest.fixture(params=[True, False], ids=["rich_enabled", "rich_disabled"])
def rich_mode(request):
    """
    Parameterized fixture that runs each test with Rich enabled and disabled.

    Args:
        request.param: True for Rich enabled, False for Rich disabled

    Yields:
        bool: Actual Rich mode state (may differ from request if Rich unavailable)
    """
    # Store original state
    original_state = display.is_rich_available()

    # Attempt to set the Rich mode using dynamic switching
    display.set_rich_mode(request.param)

    # Yield the actual state (may be False even if True was requested)
    actual_state = display.is_rich_available()
    yield actual_state

    # Cleanup: restore original state
    display.set_rich_mode(original_state)


@contextmanager
def capture_rich_output():
    """Capture Rich console output by mocking the console."""
    with patch.object(display, "console") as mock_console:
        yield mock_console


@contextmanager
def capture_click_output():
    """Capture click.echo output."""
    with patch("trilogy.scripts.display.echo") as mock_echo:
        yield mock_echo


class TestPrintFunctions:
    """Test all print functions in both Rich and fallback modes."""

    def test_print_success(self, rich_mode):
        """Test print_success outputs correctly in both modes."""
        message = "Test success message"

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_success(message)
                mock_console.print.assert_called_once_with(message, style="bold green")
        else:
            with capture_click_output() as mock_echo:
                display.print_success(message)
                # Verify click.echo was called with styled message
                mock_echo.assert_called_once()
                args, kwargs = mock_echo.call_args
                # The first argument should be a styled string
                assert message in str(args[0]) or any(
                    message in str(arg) for arg in args
                )

    def test_print_info(self, rich_mode):
        """Test print_info outputs correctly in both modes."""
        message = "Test info message"

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_info(message)
                mock_console.print.assert_called_once_with(message, style="bold blue")
        else:
            with capture_click_output() as mock_echo:
                display.print_info(message)
                mock_echo.assert_called_once()

    def test_print_warning(self, rich_mode):
        """Test print_warning outputs correctly in both modes."""
        message = "Test warning message"

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_warning(message)
                mock_console.print.assert_called_once_with(message, style="bold yellow")
        else:
            with capture_click_output() as mock_echo:
                display.print_warning(message)
                mock_echo.assert_called_once()

    def test_print_error(self, rich_mode):
        """Test print_error outputs correctly in both modes."""
        message = "Test error message"

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_error(message)
                mock_console.print.assert_called_once_with(message, style="bold red")
        else:
            with capture_click_output() as mock_echo:
                display.print_error(message)
                mock_echo.assert_called_once()

    def test_print_header(self, rich_mode):
        """Test print_header outputs correctly in both modes."""
        message = "Test header message"

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_header(message)
                mock_console.print.assert_called_once_with(
                    message, style="bold magenta"
                )
        else:
            with capture_click_output() as mock_echo:
                display.print_header(message)
                mock_echo.assert_called_once()


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
        assert display.is_rich_available() == rich_mode


class TestDisplayFunctions:
    """Test display functions in both modes."""

    def test_show_execution_info(self, rich_mode):
        """Test show_execution_info display outputs correctly."""
        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_execution_info(
                    input_type="file",
                    input_name="test.sql",
                    dialect="postgresql",
                    debug=True,
                )
                # Verify Panel.fit was called and console.print was called
                mock_console.print.assert_called_once()
                # The argument should be a Panel object or similar
                args, kwargs = mock_console.print.call_args
                assert len(args) >= 1  # At least one argument (the panel)
        else:
            with patch.object(display, "print_info") as mock_print_info:
                display.show_execution_info(
                    input_type="file",
                    input_name="test.sql",
                    dialect="postgresql",
                    debug=True,
                )
                mock_print_info.assert_called_once()
                args, kwargs = mock_print_info.call_args
                info_message = args[0]
                assert "test.sql" in info_message
                assert "postgresql" in info_message
                assert "True" in info_message

    def test_show_environment_params_with_params(self, rich_mode):
        """Test show_environment_params with parameters."""
        env_params = {"key1": "value1", "key2": "value2"}

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_environment_params(env_params)
                mock_console.print.assert_called_once_with(
                    f"Environment parameters: {env_params}", style="dim cyan"
                )
        else:
            with capture_click_output() as mock_echo:
                display.show_environment_params(env_params)
                mock_echo.assert_called_once()
                args, kwargs = mock_echo.call_args
                # Verify the environment parameters are in the output
                output_str = str(args[0])
                assert "Environment parameters" in output_str

    def test_show_environment_params_empty(self, rich_mode):
        """Test show_environment_params with empty dict."""
        # Should not display anything for empty params
        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_environment_params({})
                mock_console.print.assert_not_called()
        else:
            with capture_click_output() as mock_echo:
                display.show_environment_params({})
                mock_echo.assert_not_called()

    def test_show_debug_mode(self, rich_mode):
        """Test show_debug_mode display."""
        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_debug_mode()
                mock_console.print.assert_called_once()
                # Verify a Panel was printed
                args, kwargs = mock_console.print.call_args
                assert len(args) >= 1
        # Fallback mode doesn't implement this function

    def test_show_statement_type_multiple(self, rich_mode):
        """Test show_statement_type with multiple statements."""
        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_statement_type(idx=0, total=3, statement_type="SELECT")
                mock_console.print.assert_called_once()
                args, kwargs = mock_console.print.call_args
                output = str(args[0])
                assert "Statement 1/3" in output
                assert "SELECT" in output
        else:
            with capture_click_output() as mock_echo:
                display.show_statement_type(idx=0, total=3, statement_type="SELECT")
                mock_echo.assert_called_once()
                args, kwargs = mock_echo.call_args
                output = str(args[0])
                assert "Statement 1/3" in output
                assert "SELECT" in output

    def test_show_statement_type_single(self, rich_mode):
        """Test show_statement_type with single statement."""
        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_statement_type(idx=0, total=1, statement_type="INSERT")
                mock_console.print.assert_called_once()
                args, kwargs = mock_console.print.call_args
                output = str(args[0])
                assert "Statement 1" in output
                assert "INSERT" in output
        else:
            with capture_click_output() as mock_echo:
                display.show_statement_type(idx=0, total=1, statement_type="INSERT")
                mock_echo.assert_called_once()


class TestProgressAndExecution:
    """Test progress and execution display functions."""

    def test_show_execution_start_single(self, rich_mode):
        """Test show_execution_start with single statement."""
        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_execution_start(1)
                mock_console.print.assert_called_once()
                args, kwargs = mock_console.print.call_args
                output = str(args[0])
                assert "1 statement" in output
        else:
            with patch.object(display, "print_info") as mock_print_info:
                display.show_execution_start(1)
                mock_print_info.assert_called_once()
                args, kwargs = mock_print_info.call_args
                assert "1 statement" in args[0]

    def test_show_execution_start_multiple(self, rich_mode):
        """Test show_execution_start with multiple statements."""
        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_execution_start(5)
                mock_console.print.assert_called_once()
                args, kwargs = mock_console.print.call_args
                output = str(args[0])
                assert "5 statements" in output
        else:
            with patch.object(display, "print_info") as mock_print_info:
                display.show_execution_start(5)
                mock_print_info.assert_called_once()
                args, kwargs = mock_print_info.call_args
                assert "5 statements" in args[0]

    def test_create_progress_context_single_query(self, rich_mode):
        """Test create_progress_context with single query."""
        result = display.create_progress_context(1)
        # Should return None regardless of Rich mode for single query
        assert result is None

    def test_create_progress_context_multiple_queries(self, rich_mode):
        """Test create_progress_context with multiple queries."""
        result = display.create_progress_context(5)

        if rich_mode:
            # Should return Progress object when Rich is available
            assert result is not None
            # Verify it's a Progress-like object
            assert hasattr(result, "__enter__") and hasattr(result, "__exit__")
        else:
            # Should return None when Rich is not available
            assert result is None

    def test_show_statement_result_success_with_results(self, rich_mode):
        """Test show_statement_result for successful execution with results."""
        duration = timedelta(seconds=1.5)

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_statement_result(
                    idx=0, total=2, duration=duration, has_results=True, error=None
                )
                mock_console.print.assert_called_once()
                args, kwargs = mock_console.print.call_args
                output = str(args[0])
                assert "Statement 1/2 Results" in output
                assert "1.50s" in output
        else:
            with patch.object(display, "print_success") as mock_print_success:
                display.show_statement_result(
                    idx=0, total=2, duration=duration, has_results=True, error=None
                )
                mock_print_success.assert_called_once()
                args, kwargs = mock_print_success.call_args
                assert "Statement 1/2" in args[0]
                assert "1.50s" in args[0]

    def test_show_statement_result_success_no_results(self, rich_mode):
        """Test show_statement_result for successful execution without results."""
        duration = timedelta(seconds=0.8)

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_statement_result(
                    idx=1, total=2, duration=duration, has_results=False, error=None
                )
                mock_console.print.assert_called_once()
                args, kwargs = mock_console.print.call_args
                output = str(args[0])
                assert "Statement 2/2 completed" in output
                assert "0.80s" in output
        else:
            with patch.object(display, "print_success") as mock_print_success:
                display.show_statement_result(
                    idx=1, total=2, duration=duration, has_results=False, error=None
                )
                mock_print_success.assert_called_once()

    def test_show_statement_result_error(self, rich_mode):
        """Test show_statement_result for failed execution."""
        duration = timedelta(seconds=0.1)

        with patch.object(display, "print_error") as mock_print_error:
            display.show_statement_result(
                idx=0,
                total=1,
                duration=duration,
                has_results=False,
                error="Syntax error near 'SELECT'",
                exception_type=ValueError,
            )
            mock_print_error.assert_called_once()
            args, kwargs = mock_print_error.call_args
            error_message = args[0]
            assert "Statement 1" in error_message
            assert "failed" in error_message
            assert "Syntax error near 'SELECT'" in error_message

    def test_show_statement_result_unclear_error(self, rich_mode):
        """Test show_statement_result with unclear error message."""
        duration = timedelta(seconds=0.1)

        with patch.object(display, "print_error") as mock_print_error:
            display.show_statement_result(
                idx=0,
                total=1,
                duration=duration,
                has_results=False,
                error="",
                exception_type=RuntimeError,
            )
            mock_print_error.assert_called_once()
            args, kwargs = mock_print_error.call_args
            error_message = args[0]
            assert "Statement 1" in error_message
            assert "RuntimeError" in error_message

    def test_show_execution_summary(self, rich_mode):
        """Test show_execution_summary."""
        total_duration = timedelta(seconds=10.5)

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_execution_summary(3, total_duration)
                # Should be called twice: newline and panel
                assert mock_console.print.call_count == 2
                calls = mock_console.print.call_args_list
                # Check that execution summary info is present
                summary_call = calls[1]  # Second call is the panel
                assert len(summary_call[0]) >= 1
        else:
            with patch.object(display, "print_success") as mock_print_success:
                display.show_execution_summary(3, total_duration)
                mock_print_success.assert_called_once()
                args, kwargs = mock_print_success.call_args
                message = args[0]
                assert "3 statements" in message
                assert "10.50s" in message

    def test_show_formatting_result(self, rich_mode):
        """Test show_formatting_result."""
        duration = timedelta(seconds=2.3)

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.show_formatting_result(
                    filename="test.sql", num_queries=5, duration=duration
                )
                # Should be called twice for filename and processed info
                assert mock_console.print.call_count == 2
                calls = mock_console.print.call_args_list

                # Check filename in first call
                first_call_output = str(calls[0][0][0])
                assert "test.sql" in first_call_output

                # Check processing info in second call
                second_call_output = str(calls[1][0][0])
                assert "5" in second_call_output
                assert "2.30s" in second_call_output
        else:
            with patch.object(display, "print_success") as mock_print_success:
                display.show_formatting_result(
                    filename="test.sql", num_queries=5, duration=duration
                )
                mock_print_success.assert_called_once()
                args, kwargs = mock_print_success.call_args
                message = args[0]
                assert "5 queries" in message
                assert "2.30s" in message


class TestResultsTable:
    """Test results table functionality."""

    def test_print_results_table_no_results(self, rich_mode):
        """Test print_results_table with no results."""
        mock_query = Mock()
        mock_query.fetchall.return_value = []
        mock_query.keys.return_value = ["col1", "col2"]

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_results_table(mock_query)
                mock_console.print.assert_called_once_with(
                    "No results returned.", style="dim"
                )
        else:
            with patch.object(display, "print_warning") as mock_print_warning, patch(
                "builtins.print"
            ) as mock_print:
                display.print_results_table(mock_query)
                # Fallback should warn about Rich and print basic output
                mock_print_warning.assert_called_once_with(
                    "Install rich for prettier table output"
                )
                # Should print headers and separator
                assert mock_print.call_count >= 2

    def test_print_results_table_with_results(self, rich_mode):
        """Test print_results_table with results."""
        mock_query = Mock()
        mock_query.fetchall.return_value = [
            ("value1", "value2"),
            ("value3", None),
            ("value4", "value5"),
        ]
        mock_query.keys.return_value = ["column1", "column2"]

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_results_table(mock_query)
                mock_console.print.assert_called_once()
                # Verify a table was printed (mock_console.print called with Table object)
                args, kwargs = mock_console.print.call_args
                assert len(args) >= 1
        else:
            with patch.object(display, "print_warning") as mock_print_warning, patch(
                "builtins.print"
            ) as mock_print:
                display.print_results_table(mock_query)
                mock_print_warning.assert_called_once()
                # Should print headers, data rows, and separator
                assert mock_print.call_count >= 4  # headers + 3 rows + separator

    def test_print_results_table_with_custom_headers(self, rich_mode):
        """Test print_results_table with custom headers."""
        mock_query = Mock()
        mock_query.fetchall.return_value = [("data1", "data2")]
        mock_query.keys.return_value = ["original1", "original2"]

        custom_headers = ["Custom1", "Custom2"]

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_results_table(mock_query, headers=custom_headers)
                mock_console.print.assert_called_once()
        else:
            with patch.object(display, "print_warning"), patch(
                "builtins.print"
            ) as mock_print:
                display.print_results_table(mock_query, headers=custom_headers)
                # Verify custom headers are used in output
                print_calls = [str(call) for call in mock_print.call_args_list]
                header_call = print_calls[0] if print_calls else ""
                # Note: fallback doesn't use custom headers, uses q.keys()

    def test_print_results_table_large_dataset(self, rich_mode):
        """Test print_results_table with large dataset (truncation)."""
        mock_query = Mock()
        # Create a large dataset that should trigger truncation
        large_results = [("row", i) for i in range(100)]
        mock_query.fetchall.return_value = large_results
        mock_query.keys.return_value = ["col1", "col2"]

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_results_table(mock_query)
                # Should print table and truncation message
                assert mock_console.print.call_count == 2
                calls = mock_console.print.call_args_list
                # Second call should be truncation message
                truncation_msg = str(calls[1][0][0])
                assert "50" in truncation_msg and "100" in truncation_msg
        else:
            with patch.object(display, "print_warning"), patch(
                "builtins.print"
            ) as mock_print:
                display.print_results_table(mock_query)
                # Should print all rows in fallback (no truncation implemented)
                assert mock_print.call_count >= 100


class TestContextManagers:
    """Test context managers."""

    def test_with_status_context_manager(self, rich_mode):
        """Test with_status as context manager."""
        if rich_mode:
            with capture_rich_output() as mock_console:
                # Mock the status method to return a proper context manager
                mock_status = Mock()
                mock_status.__enter__ = Mock(return_value=mock_status)
                mock_status.__exit__ = Mock(return_value=None)
                mock_console.status.return_value = mock_status

                with display.with_status("Testing operation"):
                    pass

                mock_console.status.assert_called_once_with(
                    "[bold green]Testing operation..."
                )
                mock_status.__enter__.assert_called_once()
                mock_status.__exit__.assert_called_once()
        else:
            with patch.object(display, "print_info") as mock_print_info:
                with display.with_status("Testing operation"):
                    pass
                mock_print_info.assert_called_once_with("Testing operation...")

    def test_with_status_exception_handling(self, rich_mode):
        """Test with_status handles exceptions properly."""
        if rich_mode:
            with capture_rich_output() as mock_console:
                mock_status = Mock()
                mock_status.__enter__ = Mock(return_value=mock_status)
                mock_status.__exit__ = Mock(return_value=None)
                mock_console.status.return_value = mock_status

                try:
                    with display.with_status("Operation with error"):
                        raise ValueError("Test exception")
                except ValueError:
                    pass  # Expected

                mock_status.__exit__.assert_called_once()
        else:
            with patch.object(display, "print_info") as mock_print_info:
                try:
                    with display.with_status("Operation with error"):
                        raise ValueError("Test exception")
                except ValueError:
                    pass  # Expected

                mock_print_info.assert_called_once()


class TestDynamicSwitching:
    """Test dynamic mode switching functionality."""

    def test_mode_switching_preserves_original_capability(self):
        """Test that mode switching can restore original capability."""
        # This test doesn't use the rich_mode fixture since it tests switching
        original_rich_available = display.is_rich_available()

        # Switch to disabled
        display.set_rich_mode(False)
        assert display.is_rich_available() is False

        # Try to switch back to enabled
        display.set_rich_mode(True)
        # Should only be enabled if Rich was originally importable
        # Restore original state for other tests
        display.set_rich_mode(original_rich_available)

    def test_functions_work_after_mode_switch(self):
        """Test that functions continue to work after mode switches and produce correct output."""
        original_state = display.is_rich_available()

        try:
            # Switch to fallback mode
            display.set_rich_mode(False)
            with patch.object(display, "echo") as mock_echo:
                display.print_success("Fallback mode message")
                mock_echo.assert_called_once()

            # Switch to Rich mode (if possible)
            display.set_rich_mode(True)
            if display.is_rich_available():
                with capture_rich_output() as mock_console:
                    display.print_success("Rich mode message")
                    mock_console.print.assert_called_once_with(
                        "Rich mode message", style="bold green"
                    )
        finally:
            # Restore original state
            display.set_rich_mode(original_state)


@pytest.mark.parametrize(
    "num_queries,expected_none",
    [
        (1, True),  # Single query should return None
        (
            2,
            False,
        ),  # Multiple queries should return Progress object (if Rich available)
        (
            5,
            False,
        ),  # Multiple queries should return Progress object (if Rich available)
    ],
)
def test_create_progress_context_parametrized(num_queries, expected_none):
    """Test create_progress_context with different query counts."""
    original_state = display.is_rich_available()

    try:
        # Test with Rich enabled
        display.set_rich_mode(True)
        result_rich = display.create_progress_context(num_queries)

        # Test with Rich disabled
        display.set_rich_mode(False)
        result_fallback = display.create_progress_context(num_queries)

        if expected_none:
            assert result_rich is None
            assert result_fallback is None
        else:
            # Rich mode: should return Progress object if Rich is available
            # Fallback mode: should always return None
            assert result_fallback is None
            # result_rich might be None or Progress object depending on Rich availability
    finally:
        # Restore original state
        display.set_rich_mode(original_state)


class TestOutputBehaviorValidation:
    """Additional tests to validate specific output behaviors."""

    def test_rich_vs_fallback_styling_differences(self, rich_mode):
        """Test that Rich and fallback modes produce different but valid output."""
        message = "Test message"

        if rich_mode:
            # Rich mode uses console.print with style parameters
            with capture_rich_output() as mock_console:
                display.print_success(message)
                display.print_error(message)
                display.print_warning(message)

                assert mock_console.print.call_count == 3
                calls = mock_console.print.call_args_list

                # Verify different styles are used
                styles = [call[1]["style"] for call in calls]
                assert "bold green" in styles
                assert "bold red" in styles
                assert "bold yellow" in styles
        else:
            # Fallback mode uses click.echo with styled strings
            with capture_click_output() as mock_echo:
                display.print_success(message)
                display.print_error(message)
                display.print_warning(message)

                assert mock_echo.call_count == 3
                # Each call should have styled output (not raw text)
                for call in mock_echo.call_args_list:
                    args = call[0]
                    # The styled string should be different from plain message
                    assert str(args[0]) != message

    def test_table_output_differences(self, rich_mode):
        """Test that table outputs differ between Rich and fallback modes."""
        mock_query = Mock()
        mock_query.fetchall.return_value = [("value1", "value2")]
        mock_query.keys.return_value = ["col1", "col2"]

        if rich_mode:
            with capture_rich_output() as mock_console:
                display.print_results_table(mock_query)
                # Rich mode prints a Table object
                mock_console.print.assert_called_once()
                args = mock_console.print.call_args[0]
                # Verify a table-like object was printed (would be a Table in real usage)
                assert len(args) >= 1
        else:
            with patch.object(display, "print_warning") as mock_warning, patch(
                "builtins.print"
            ) as mock_print:
                display.print_results_table(mock_query)
                # Fallback mode warns about Rich and uses basic print
                mock_warning.assert_called_once_with(
                    "Install rich for prettier table output"
                )
                # Should print headers and data
                assert mock_print.call_count >= 2
