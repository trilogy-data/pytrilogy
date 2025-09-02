from pathlib import Path

from trilogy import Dialects, parse
from trilogy.core.models.environment import Environment
from trilogy.dialect.metadata import MockResult, MockResultRow


def test_file_parsing():
    directory = Path(__file__).parent
    target = directory / "test_env.preql"
    parsed = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=directory)
    ).parse_file(target)
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


def test_mock_result():
    result = MockResult(
        columns=["a", "b", "c"],
        values=[
            MockResultRow({"a": 1, "b": 2, "c": 3}),
            MockResultRow({"a": 4, "b": 5, "c": 6}),
        ],
    )
    assert result.columns == ["a", "b", "c"]

    for row in result:
        assert isinstance(row, MockResultRow)
        assert set(result.keys()) == {"a", "b", "c"}
        assert all(isinstance(v, int) for v in row.values())

    result = MockResult(
        columns=["a", "b", "c"],
        values=[
            MockResultRow({"a": 1, "b": 2, "c": 3}),
            MockResultRow({"a": 4, "b": 5, "c": 6}),
        ],
    )
    assert result.columns == ["a", "b", "c"]

    x = result.fetchone()
    assert isinstance(x, MockResultRow)
    assert x["a"] == 1
    r2 = result.fetchmany(10)
    assert len(r2) == 1
