from trilogy import Environment, Dialects
from pathlib import Path


def test_arrow_source():
    script = '''
key fib_index int;
property fib_index.value int;

datasource fib_numbers(
    index:fib_index,
    fibonacci: value
)
grain (fib_index)
file `./fib.py`;


select
    sum(value) as total_fib;
'''

    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=Path(__file__).parent))
    
    results = executor.execute_text(script)

    assert results[-1].fetchone()[0] > 100