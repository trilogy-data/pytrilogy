#!/usr/bin/env python3
"""
Profiling script for test_generate_queries_perf function.
Focuses on profiling the dialect.parse_text() call specifically.
"""

import cProfile
import pstats
import io
import time
from datetime import datetime
from pathlib import Path

from trilogy.core.models.environment import Environment
from trilogy import Dialects


def setup_environment():
    """Setup the environment exactly as in the original test."""
    working_path = Path(__file__).parent
    
    env, imports = Environment(working_path=working_path).parse(
        """
import call_center as call_center;
import catalog_returns as catalog_returns;
import catalog_sales as catalog_sales;
import customer_demographic as customer_demographic;
import customer as customer;
import inventory as inventory;
import item as item;
import promotion as promotion;
import store_returns as store_returns;
import store_sales as store_sales;
import store as store;
import time as time;
import date as date;
import warehouse as warehouse;
import web_sales as web_sales;
"""
    )
    
    dialect = Dialects.DUCK_DB.default_executor(environment=env)
    
    test_queries = """
select
    store_sales.date.year,
    count(store_sales.ticket_number) as store_order_count;

select
    store_sales.date.year,
    count(store_sales.ticket_number) as store_order_count;

    select
    store_sales.date.year,
    count(store_sales.ticket_number) as store_order_count;


"""
    
    return dialect, test_queries


def profile_parse_text(dialect, test_queries, iterations: int = 1):
    """Profile the parse_text operation specifically."""
    
    def target_function():
        """The exact function we want to profile."""
        for _ in range(iterations):
            dialect.parse_text(test_queries)
    
    # Create profiler
    profiler = cProfile.Profile()
    
    # Time the operation
    start_time = time.perf_counter()
    start_datetime = datetime.now()
    
    # Profile the target function
    profiler.enable()
    target_function()
    profiler.disable()
    
    end_time = time.perf_counter()
    end_datetime = datetime.now()
    
    # Calculate timing
    wall_time = end_time - start_time
    datetime_delta = end_datetime - start_datetime
    
    return profiler, wall_time, datetime_delta


def print_profile_results(profiler, wall_time, datetime_delta, iterations: int = 1):
    """Print formatted profiling results."""
    
    print(f"=== PROFILING RESULTS ===")
    print(f"Iterations: {iterations}")
    print(f"Wall time (perf_counter): {wall_time:.6f} seconds")
    print(f"Wall time (datetime): {datetime_delta.total_seconds():.6f} seconds")
    print(f"Average per iteration: {wall_time/iterations:.6f} seconds")
    print()
    
    # Create a string buffer to capture profile output
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    
    # Sort by cumulative time and print top functions
    ps.sort_stats('cumulative')
    ps.print_stats(20)  # Top 20 functions
    
    print("=== TOP 20 FUNCTIONS BY CUMULATIVE TIME ===")
    print(s.getvalue())
    
    # Reset buffer and sort by total time
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    ps.sort_stats('tottime')
    ps.print_stats(10)  # Top 10 functions
    
    print("\n=== TOP 10 FUNCTIONS BY TOTAL TIME ===")
    print(s.getvalue())


def detailed_timing_analysis(dialect, test_queries, iterations: int = 10):
    """Run multiple iterations to analyze timing consistency."""
    
    print(f"=== DETAILED TIMING ANALYSIS ({iterations} iterations) ===")
    
    times = []
    for i in range(iterations):
        start = time.perf_counter()
        dialect.parse_text(test_queries)
        end = time.perf_counter()
        duration = end - start
        times.append(duration)
        print(f"Iteration {i+1:2d}: {duration:.6f} seconds")
    
    # Calculate statistics
    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)
    
    print(f"\nTiming Statistics:")
    print(f"  Average: {avg_time:.6f} seconds")
    print(f"  Minimum: {min_time:.6f} seconds")
    print(f"  Maximum: {max_time:.6f} seconds")
    print(f"  Range:   {max_time - min_time:.6f} seconds")
    
    # Check against original assertion
    original_limit = 0.01  # from original test
    print(f"\nOriginal test limit: {original_limit} seconds")
    print(f"Average vs limit: {'PASS' if avg_time < original_limit else 'FAIL'}")
    print(f"Max vs limit: {'PASS' if max_time < original_limit else 'FAIL'}")


def save_profile_data(profiler, filename: str = "parse_text_profile.prof"):
    """Save profile data to file for later analysis."""
    profiler.dump_stats(filename)
    print(f"\nProfile data saved to: {filename}")
    print(f"Analyze with: python -m pstats {filename}")


def main():
    """Main profiling function."""
    print("Setting up environment...")
    dialect, test_queries = setup_environment()
    
    # Single run with profiling
    print("\nRunning single profiled execution...")
    profiler, wall_time, datetime_delta = profile_parse_text(dialect, test_queries, iterations=1)
    print_profile_results(profiler, wall_time, datetime_delta, iterations=1)
    
    # Save profile data
    save_profile_data(profiler, "single_run_profile.prof")
    
    # Multiple runs for timing analysis
    print("\n" + "="*60)
    detailed_timing_analysis(dialect, test_queries, iterations=10)
    
    # Stress test with multiple iterations
    print("\n" + "="*60)
    print("Running stress test (100 iterations)...")
    stress_profiler, stress_wall_time, stress_datetime_delta = profile_parse_text(
        dialect, test_queries, iterations=100
    )
    print_profile_results(stress_profiler, stress_wall_time, stress_datetime_delta, iterations=100)
    save_profile_data(stress_profiler, "stress_test_profile.prof")


if __name__ == "__main__":
    main()