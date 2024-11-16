from trilogy import Dialects, parse, Environment
from pathlib import Path


def test_file_parsing():
    target = Path(__file__).parent / "test_env.preql"
    parsed = Dialects.DUCK_DB.default_executor().parse_file(target)
    assert len(list(parsed)) == 1


def test_can_handle_everything():
    env = Environment(working_path=Path(__file__).parent)
    env, queries = parse(
        """
import test_env as test_env;

key x int;

merge test_env.id into x;
                         
RAW_SQL('''select 1 ''');
""",
        environment=env,
    )
    execs = Dialects.DUCK_DB.default_executor(environment=env)
    for q in queries:
        execs.execute_query(q)
