from trilogy import parse, Dialects

from pathlib import Path
from logging import StreamHandler, DEBUG

from trilogy.constants import logger

logger.setLevel(DEBUG)
logger.addHandler(StreamHandler())

def test_pushdown():

    with open(Path(__file__).parent / "pushdown.preql") as f:
        text = f.read() 

    env, queries = parse(text)


    generated = Dialects.DUCK_DB.default_executor(environment=env).generate_sql(queries[-1])[0]

    print(generated)
    test_str =''' db."date" = '2024-01-01' '''.strip()
    assert generated.count(test_str) == 2