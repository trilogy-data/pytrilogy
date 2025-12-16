from trilogy import Environment, Dialects
from pathlib import Path


def test_csv_source():
    script = '''
key id int;
property id.name string;

datasource fib_numbers(
    id,
    name
)
grain (id)
file `./test.csv`;


select
    id.count as total_ids;
'''

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent))
    
    results = executor.execute_text(script)

    assert results[-1].fetchone()[0] ==4