from pathlib import Path

from trilogy import Dialects
from trilogy.core.models_environment import Environment
from trilogy.hooks.query_debugger import DebuggingHook


def test_complex():
    hooks = [DebuggingHook()]
    env = Environment(working_path=Path(__file__).parent)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    engine.execute_file(Path(__file__).parent / "final_persist.preql")
    r1 = engine.execute_text(
        """select 
        generic.split, 
        generic.scalar
        order by generic.split asc, generic.scalar asc;"""
    )[-1]

    env = Environment(working_path=Path(__file__).parent)
    engine = Dialects.DUCK_DB.default_executor(environment=env, hooks=hooks)
    engine.execute_file(Path(__file__).parent / "optimize.preql")
    r2 = engine.execute_text(
        """select 
        generic.split, 
        generic.scalar
        order by generic.split asc, generic.scalar asc;"""
    )[-1]

    _ = engine.generate_sql(
        """select 
        generic.split, 
        generic.scalar
        order by generic.split asc, generic.scalar asc;"""
    )[-1]
    # assert sql == "abc", sql
    r1f = r1.fetchall()
    r2f = r2.fetchall()
    assert len(r1f) == len(r2f)
    for i in range(len(r1f)):
        assert r1f[i] == r2f[i]


def test_persist_in_import():
    env = Environment(
        working_path=Path(__file__).parent
    )  # .parse_file(Path(__file__).parent / "query_one.preql")
    engine = Dialects.DUCK_DB.default_executor(environment=env)

    results = engine.execute_file(Path(__file__).parent / "query_one.preql")
    rlist = results[-1].fetchall()
    assert len(rlist) == 4
    assert rlist[0] == (4, "two")
