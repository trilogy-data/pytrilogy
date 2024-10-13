from trilogy import Environment
from trilogy import Dialects
from pathlib import Path
from trilogy.hooks.query_debugger import DebuggingHook


def test_complex():
    hooks = [DebuggingHook()]
    env = Environment(working_path=Path(__file__).parent)
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    engine.execute_file(Path(__file__).parent / "final_persist.preql")
    r1 = engine.execute_text(
        """select 
        generic.split, 
        generic.scalar;"""
    )[-1]

    env = Environment(working_path=Path(__file__).parent)
    engine = Dialects.DUCK_DB.default_executor(environment=env, hooks=hooks)
    engine.execute_file(Path(__file__).parent / "optimize.preql")
    r2 = engine.execute_text(
        """select 
        generic.split, 
        generic.scalar;"""
    )[-1]

    _ = engine.generate_sql(
        """select 
        generic.split, 
        generic.scalar;"""
    )[-1]
    # assert sql == "abc", sql

    assert r1.fetchall() == r2.fetchall()
