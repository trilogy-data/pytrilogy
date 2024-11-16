from trilogy import Dialects, parse
from pathlib import Path


def test_file_parsing():
    target = Path(__file__).parent / "test_env.preql"
    parsed = Dialects.DUCK_DB.default_executor().parse_file(target)
    assert len(list(parsed)) == 1


def test_can_handle_everything():
    env, queries = parse(
        """
key x int;
                         
RAW_SQL('''select 1 ''');
"""
    )
    execs = Dialects.DUCK_DB.default_executor(environment=env)
    for q in queries:
        execs.execute_query(q)
