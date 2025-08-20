"""
Profiling script for test_generate_queries_perf function.
Focuses on profiling the dialect.parse_text() call specifically.
"""

import cProfile
import io
import pstats
import sys
import time
from datetime import datetime
from pathlib import Path

# Add local pytrilogy path (3 directories up) to Python path
local_pytrilogy_path = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(local_pytrilogy_path))


from trilogy import Dialects  # noqa: E402
from trilogy.core.models.environment import Environment  # noqa: E402


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

    test_queries = (
        """


    import store_sales as store_sales;
    import customer as customer;

    const zips_pre <- unnest(['24128',
                                        '76232',
                                        '65084',
                                        '87816',
                                        '83926',
                                        '77556',
                                        '20548',
                                        '26231',
                                        '43848',
                                        '15126',
                                        '91137',
                                        '61265',
                                        '98294',
                                        '25782',
                                        '17920',
                                        '18426',
                                        '98235',
                                        '40081',
                                        '84093',
                                        '28577',
                                        '55565',
                                        '17183',
                                        '54601',
                                        '67897',
                                        '22752',
                                        '86284',
                                        '18376',
                                        '38607',
                                        '45200',
                                        '21756',
                                        '29741',
                                        '96765',
                                        '23932',
                                        '89360',
                                        '29839',
                                        '25989',
                                        '28898',
                                        '91068',
                                        '72550',
                                        '10390',
                                        '18845',
                                        '47770',
                                        '82636',
                                        '41367',
                                        '76638',
                                        '86198',
                                        '81312',
                                        '37126',
                                        '39192',
                                        '88424',
                                        '72175',
                                        '81426',
                                        '53672',
                                        '10445',
                                        '42666',
                                        '66864',
                                        '66708',
                                        '41248',
                                        '48583',
                                        '82276',
                                        '18842',
                                        '78890',
                                        '49448',
                                        '14089',
                                        '38122',
                                        '34425',
                                        '79077',
                                        '19849',
                                        '43285',
                                        '39861',
                                        '66162',
                                        '77610',
                                        '13695',
                                        '99543',
                                        '83444',
                                        '83041',
                                        '12305',
                                        '57665',
                                        '68341',
                                        '25003',
                                        '57834',
                                        '62878',
                                        '49130',
                                        '81096',
                                        '18840',
                                        '27700',
                                        '23470',
                                        '50412',
                                        '21195',
                                        '16021',
                                        '76107',
                                        '71954',
                                        '68309',
                                        '18119',
                                        '98359',
                                        '64544',
                                        '10336',
                                        '86379',
                                        '27068',
                                        '39736',
                                        '98569',
                                        '28915',
                                        '24206',
                                        '56529',
                                        '57647',
                                        '54917',
                                        '42961',
                                        '91110',
                                        '63981',
                                        '14922',
                                        '36420',
                                        '23006',
                                        '67467',
                                        '32754',
                                        '30903',
                                        '20260',
                                        '31671',
                                        '51798',
                                        '72325',
                                        '85816',
                                        '68621',
                                        '13955',
                                        '36446',
                                        '41766',
                                        '68806',
                                        '16725',
                                        '15146',
                                        '22744',
                                        '35850',
                                        '88086',
                                        '51649',
                                        '18270',
                                        '52867',
                                        '39972',
                                        '96976',
                                        '63792',
                                        '11376',
                                        '94898',
                                        '13595',
                                        '10516',
                                        '90225',
                                        '58943',
                                        '39371',
                                        '94945',
                                        '28587',
                                        '96576',
                                        '57855',
                                        '28488',
                                        '26105',
                                        '83933',
                                        '25858',
                                        '34322',
                                        '44438',
                                        '73171',
                                        '30122',
                                        '34102',
                                        '22685',
                                        '71256',
                                        '78451',
                                        '54364',
                                        '13354',
                                        '45375',
                                        '40558',
                                        '56458',
                                        '28286',
                                        '45266',
                                        '47305',
                                        '69399',
                                        '83921',
                                        '26233',
                                        '11101',
                                        '15371',
                                        '69913',
                                        '35942',
                                        '15882',
                                        '25631',
                                        '24610',
                                        '44165',
                                        '99076',
                                        '33786',
                                        '70738',
                                        '26653',
                                        '14328',
                                        '72305',
                                        '62496',
                                        '22152',
                                        '10144',
                                        '64147',
                                        '48425',
                                        '14663',
                                        '21076',
                                        '18799',
                                        '30450',
                                        '63089',
                                        '81019',
                                        '68893',
                                        '24996',
                                        '51200',
                                        '51211',
                                        '45692',
                                        '92712',
                                        '70466',
                                        '79994',
                                        '22437',
                                        '25280',
                                        '38935',
                                        '71791',
                                        '73134',
                                        '56571',
                                        '14060',
                                        '19505',
                                        '72425',
                                        '56575',
                                        '74351',
                                        '68786',
                                        '51650',
                                        '20004',
                                        '18383',
                                        '76614',
                                        '11634',
                                        '18906',
                                        '15765',
                                        '41368',
                                        '73241',
                                        '76698',
                                        '78567',
                                        '97189',
                                        '28545',
                                        '76231',
                                        '75691',
                                        '22246',
                                        '51061',
                                        '90578',
                                        '56691',
                                        '68014',
                                        '51103',
                                        '94167',
                                        '57047',
                                        '14867',
                                        '73520',
                                        '15734',
                                        '63435',
                                        '25733',
                                        '35474',
                                        '24676',
                                        '94627',
                                        '53535',
                                        '17879',
                                        '15559',
                                        '53268',
                                        '59166',
                                        '11928',
                                        '59402',
                                        '33282',
                                        '45721',
                                        '43933',
                                        '68101',
                                        '33515',
                                        '36634',
                                        '71286',
                                        '19736',
                                        '58058',
                                        '55253',
                                        '67473',
                                        '41918',
                                        '19515',
                                        '36495',
                                        '19430',
                                        '22351',
                                        '77191',
                                        '91393',
                                        '49156',
                                        '50298',
                                        '87501',
                                        '18652',
                                        '53179',
                                        '18767',
                                        '63193',
                                        '23968',
                                        '65164',
                                        '68880',
                                        '21286',
                                        '72823',
                                        '58470',
                                        '67301',
                                        '13394',
                                        '31016',
                                        '70372',
                                        '67030',
                                        '40604',
                                        '24317',
                                        '45748',
                                        '39127',
                                        '26065',
                                        '77721',
                                        '31029',
                                        '31880',
                                        '60576',
                                        '24671',
                                        '45549',
                                        '13376',
                                        '50016',
                                        '33123',
                                        '19769',
                                        '22927',
                                        '97789',
                                        '46081',
                                        '72151',
                                        '15723',
                                        '46136',
                                        '51949',
                                        '68100',
                                        '96888',
                                        '64528',
                                        '14171',
                                        '79777',
                                        '28709',
                                        '11489',
                                        '25103',
                                        '32213',
                                        '78668',
                                        '22245',
                                        '15798',
                                        '27156',
                                        '37930',
                                        '62971',
                                        '21337',
                                        '51622',
                                        '67853',
                                        '10567',
                                        '38415',
                                        '15455',
                                        '58263',
                                        '42029',
                                        '60279',
                                        '37125',
                                        '56240',
                                        '88190',
                                        '50308',
                                        '26859',
                                        '64457',
                                        '89091',
                                        '82136',
                                        '62377',
                                        '36233',
                                        '63837',
                                        '58078',
                                        '17043',
                                        '30010',
                                        '60099',
                                        '28810',
                                        '98025',
                                        '29178',
                                        '87343',
                                        '73273',
                                        '30469',
                                        '64034',
                                        '39516',
                                        '86057',
                                        '21309',
                                        '90257',
                                        '67875',
                                        '40162',
                                        '11356',
                                        '73650',
                                        '61810',
                                        '72013',
                                        '30431',
                                        '22461',
                                        '19512',
                                        '13375',
                                        '55307',
                                        '30625',
                                        '83849',
                                        '68908',
                                        '26689',
                                        '96451',
                                        '38193',
                                        '46820',
                                        '88885',
                                        '84935',
                                        '69035',
                                        '83144',
                                        '47537',
                                        '56616',
                                        '94983',
                                        '48033',
                                        '69952',
                                        '25486',
                                        '61547',
                                        '27385',
                                        '61860',
                                        '58048',
                                        '56910',
                                        '16807',
                                        '17871',
                                        '35258',
                                        '31387',
                                        '35458',
                                        '35576'
            ]);

    auto zips <- substring(cast(zips_pre as string),1,5);

    auto zip_p_count <- count(filter customer.id where customer.preferred_cust_flag = 'Y')  by customer.zip;

    auto p_cust_zip <- filter customer.zip where zip_p_count  > 10;

    auto final_zips <-substring(filter zips where zips in substring(p_cust_zip, 1,5),1,2);


    SELECT
        store_sales.store.name,
        sum(store_sales.net_profit
        ) ->store_net_profit
    where 
            store_sales.date.quarter = 2
            and store_sales.date.year = 1998
            and substring(store_sales.store.zip, 1, 2) in final_zips
    order by
        store_sales.store.name asc
    limit 100
    ;

"""
        * 3
    )

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

    print("=== PROFILING RESULTS ===")
    print(f"Iterations: {iterations}")
    print(f"Wall time (perf_counter): {wall_time:.6f} seconds")
    print(f"Wall time (datetime): {datetime_delta.total_seconds():.6f} seconds")
    print(f"Average per iteration: {wall_time/iterations:.6f} seconds")
    print()

    # Create a string buffer to capture profile output
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)

    # Sort by cumulative time and print top functions
    ps.sort_stats("cumulative")
    top = 40
    ps.print_stats(top)  # Top 20 functions

    print(f"=== TOP {40} FUNCTIONS BY CUMULATIVE TIME ===")
    print(s.getvalue())

    # Reset buffer and sort by total time
    s = io.StringIO()
    ps = pstats.Stats(profiler, stream=s)
    ps.sort_stats("tottime")
    top = 20
    ps.print_stats(top)  # Top 20 functions

    print(f"\n=== TOP {top} FUNCTIONS BY TOTAL TIME ===")
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

    print("\nTiming Statistics:")
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
    profiler, wall_time, datetime_delta = profile_parse_text(
        dialect, test_queries, iterations=1
    )
    print_profile_results(profiler, wall_time, datetime_delta, iterations=1)

    # Save profile data
    save_profile_data(profiler, "single_run_profile.prof")

    # Multiple runs for timing analysis
    print("\n" + "=" * 60)
    detailed_timing_analysis(dialect, test_queries, iterations=10)

    # Stress test with multiple iterations
    print("\n" + "=" * 60)
    print("Running stress test (100 iterations)...")
    stress_profiler, stress_wall_time, stress_datetime_delta = profile_parse_text(
        dialect, test_queries, iterations=25
    )
    print_profile_results(
        stress_profiler, stress_wall_time, stress_datetime_delta, iterations=100
    )
    save_profile_data(stress_profiler, "stress_test_profile.prof")


if __name__ == "__main__":
    main()


## SCRATCHPAD
#    ncalls  tottime  percall  cumtime  percall filename:lineno(function)
#   1067100    8.911    0.000   15.924    0.000 C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\pydantic\main.py:306(model_construct)
#      2325    4.669    0.002    5.915    0.003 C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\networkx\classes\digraph.py:489(add_nodes_from)
# 18117675/18116475    3.371    0.000    7.135    0.000 {built-in method builtins.isinstance}
#      2325    3.161    0.001    5.765    0.002 C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\networkx\classes\digraph.py:737(add_edges_from)
#       525    2.425    0.005   28.664    0.055 c:\Users\ethan\coding_projects\pytrilogy\trilogy\core\processing\node_generators\select_merge_node.py:181(create_pruned_concept_graph)
#    409975    2.325    0.000    3.780    0.000 C:\Users\ethan\coding_projects\pytrilogy\.venv\Lib\site-packages\pydantic\_internal\_model_construction.py:336(init_private_attributes)