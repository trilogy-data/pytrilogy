import pytest
import sys
import os
from datetime import timedelta
from io import StringIO
from unittest.mock import  Mock

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


class TestPrintFunctions:
    """Test all print functions in both Rich and fallback modes."""
    
    def test_print_success(self, rich_mode):
        """Test print_success in both modes."""
        # Capture output to verify behavior
        original_stdout = sys.stdout
        captured_output = StringIO()
        sys.stdout = captured_output
        
        try:
            display.print_success("Test success message")
            output = captured_output.getvalue()
            
            # Verify function runs without error
            assert display.is_rich_available() == rich_mode
            
            # In Rich mode, output may be empty (goes to Rich console)
            # In fallback mode, output should contain styled text
            if not rich_mode:
                # Should have some output from click.echo
                pass  # click.echo doesn't write to sys.stdout directly
                
        finally:
            sys.stdout = original_stdout
    
    def test_print_info(self, rich_mode):
        """Test print_info in both modes."""
        # Test that function executes without error
        display.print_info("Test info message")
        assert display.is_rich_available() == rich_mode
    
    def test_print_warning(self, rich_mode):
        """Test print_warning in both modes."""
        display.print_warning("Test warning message")
        assert display.is_rich_available() == rich_mode
    
    def test_print_error(self, rich_mode):
        """Test print_error in both modes."""
        display.print_error("Test error message")
        assert display.is_rich_available() == rich_mode
    
    def test_print_header(self, rich_mode):
        """Test print_header in both modes."""
        display.print_header("Test header message")
        assert display.is_rich_available() == rich_mode


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
        """Test show_execution_info display."""
        # Should execute without error regardless of mode
        display.show_execution_info(
            input_type="file",
            input_name="test.sql", 
            dialect="postgresql",
            debug=True
        )
        assert display.is_rich_available() == rich_mode
    
    def test_show_environment_params_with_params(self, rich_mode):
        """Test show_environment_params with parameters."""
        env_params = {"key1": "value1", "key2": "value2"}
        display.show_environment_params(env_params)
        assert display.is_rich_available() == rich_mode
    
    def test_show_environment_params_empty(self, rich_mode):
        """Test show_environment_params with empty dict."""
        # Should not display anything for empty params
        display.show_environment_params({})
        assert display.is_rich_available() == rich_mode
    
    def test_show_debug_mode(self, rich_mode):
        """Test show_debug_mode display."""
        if rich_mode:
            # Only test if Rich is available (fallback mode doesn't implement this)
            display.show_debug_mode()
        assert display.is_rich_available() == rich_mode
    
    def test_show_statement_type_multiple(self, rich_mode):
        """Test show_statement_type with multiple statements."""
        display.show_statement_type(
            idx=0, 
            total=3, 
            statement_type="SELECT"
        )
        assert display.is_rich_available() == rich_mode
    
    def test_show_statement_type_single(self, rich_mode):
        """Test show_statement_type with single statement."""
        display.show_statement_type(
            idx=0, 
            total=1, 
            statement_type="INSERT"
        )
        assert display.is_rich_available() == rich_mode


class TestProgressAndExecution:
    """Test progress and execution display functions."""
    
    def test_show_execution_start_single(self, rich_mode):
        """Test show_execution_start with single statement."""
        display.show_execution_start(1)
        assert display.is_rich_available() == rich_mode
    
    def test_show_execution_start_multiple(self, rich_mode):
        """Test show_execution_start with multiple statements."""
        display.show_execution_start(5)
        assert display.is_rich_available() == rich_mode
    
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
        else:
            # Should return None when Rich is not available
            assert result is None
    
    def test_show_statement_result_success_with_results(self, rich_mode):
        """Test show_statement_result for successful execution with results."""
        duration = timedelta(seconds=1.5)
        display.show_statement_result(
            idx=0,
            total=2,
            duration=duration,
            has_results=True,
            error=None
        )
        assert display.is_rich_available() == rich_mode
    
    def test_show_statement_result_success_no_results(self, rich_mode):
        """Test show_statement_result for successful execution without results."""
        duration = timedelta(seconds=0.8)
        display.show_statement_result(
            idx=1,
            total=2,
            duration=duration,
            has_results=False,
            error=None
        )
        assert display.is_rich_available() == rich_mode
    
    def test_show_statement_result_error(self, rich_mode):
        """Test show_statement_result for failed execution."""
        duration = timedelta(seconds=0.1)
        display.show_statement_result(
            idx=0,
            total=1,
            duration=duration,
            has_results=False,
            error="Syntax error near 'SELECT'",
            exception_type=ValueError
        )
        assert display.is_rich_available() == rich_mode
    
    def test_show_statement_result_unclear_error(self, rich_mode):
        """Test show_statement_result with unclear error message."""
        duration = timedelta(seconds=0.1)
        display.show_statement_result(
            idx=0,
            total=1,
            duration=duration,
            has_results=False,
            error="",
            exception_type=RuntimeError
        )
        assert display.is_rich_available() == rich_mode
    
    def test_show_execution_summary(self, rich_mode):
        """Test show_execution_summary."""
        total_duration = timedelta(seconds=10.5)
        display.show_execution_summary(3, total_duration)
        assert display.is_rich_available() == rich_mode
    
    def test_show_formatting_result(self, rich_mode):
        """Test show_formatting_result."""
        duration = timedelta(seconds=2.3)
        display.show_formatting_result(
            filename="test.sql",
            num_queries=5,
            duration=duration
        )
        assert display.is_rich_available() == rich_mode


class TestResultsTable:
    """Test results table functionality."""
    
    def test_print_results_table_no_results(self, rich_mode):
        """Test print_results_table with no results."""
        mock_query = Mock()
        mock_query.fetchall.return_value = []
        mock_query.keys.return_value = ["col1", "col2"]
        
        # Should handle empty results gracefully
        display.print_results_table(mock_query)
        assert display.is_rich_available() == rich_mode
    
    def test_print_results_table_with_results(self, rich_mode):
        """Test print_results_table with results."""
        mock_query = Mock()
        mock_query.fetchall.return_value = [
            ("value1", "value2"),
            ("value3", None),
            ("value4", "value5")
        ]
        mock_query.keys.return_value = ["column1", "column2"]
        
        # Should display results appropriately for the mode
        display.print_results_table(mock_query)
        assert display.is_rich_available() == rich_mode
    
    def test_print_results_table_with_custom_headers(self, rich_mode):
        """Test print_results_table with custom headers."""
        mock_query = Mock()
        mock_query.fetchall.return_value = [("data1", "data2")]
        mock_query.keys.return_value = ["original1", "original2"]
        
        custom_headers = ["Custom1", "Custom2"]
        display.print_results_table(mock_query, headers=custom_headers)
        assert display.is_rich_available() == rich_mode
    
    def test_print_results_table_large_dataset(self, rich_mode):
        """Test print_results_table with large dataset (truncation)."""
        mock_query = Mock()
        # Create a large dataset that should trigger truncation
        large_results = [("row", i) for i in range(100)]
        mock_query.fetchall.return_value = large_results
        mock_query.keys.return_value = ["col1", "col2"]
        
        # Should handle large datasets appropriately
        display.print_results_table(mock_query)
        assert display.is_rich_available() == rich_mode


class TestContextManagers:
    """Test context managers."""
    
    def test_with_status_context_manager(self, rich_mode):
        """Test with_status as context manager."""
        # Should work as context manager in both modes
        with display.with_status("Testing operation"):
            # Do some work
            pass
        
        assert display.is_rich_available() == rich_mode
    
    def test_with_status_exception_handling(self, rich_mode):
        """Test with_status handles exceptions properly."""
        try:
            with display.with_status("Operation with error"):
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected
        
        # Context manager should clean up properly
        assert display.is_rich_available() == rich_mode


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
        # (This test assumes Rich is available in the test environment)
    
    def test_functions_work_after_mode_switch(self):
        """Test that functions continue to work after mode switches."""
        # Switch to fallback mode
        display.set_rich_mode(False)
        display.print_success("Fallback mode message")
        
        # Switch to Rich mode (if possible)
        display.set_rich_mode(True)
        display.print_success("Rich mode message")
        
        # Both should execute without errors


@pytest.mark.parametrize("num_queries,expected_none", [
    (1, True),    # Single query should return None
    (2, False),   # Multiple queries should return Progress object (if Rich available)
    (5, False),   # Multiple queries should return Progress object (if Rich available)
])
def test_create_progress_context_parametrized(num_queries, expected_none):
    """Test create_progress_context with different query counts."""
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


if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v"])