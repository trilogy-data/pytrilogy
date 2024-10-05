from pathlib import Path

from trilogy import Environment
from trilogy import Executor
from trilogy import Dialects
from datetime import datetime


working_path = Path(__file__).parent


def run_query(engine: Executor, start, idx: int):

    with open(working_path / f"query{idx:02d}.preql") as f:
        text = f.read()

    _, query = engine.environment.parse(text)
    print("parsed files", datetime.now() - start)
    _ = engine.generate_sql(query[-1])

    print("ran query", datetime.now() - start)
    # comp_results = list(results[-1].fetchall())
    # assert len(comp_results)>0, "No results returned"
    # # run the built-in comp
    # base = engine.execute_raw_sql(f'PRAGMA tpcds({idx});')
    # base_results = list(base.fetchall())

    # # check we got it
    # if len(base_results) != len(comp_results):
    #     assert False, f"Row count mismatch: {len(base_results)} != {len(comp_results)}"
    # for idx, row in enumerate(base_results):
    #     assert row == comp_results[idx]


if __name__ == "__main__":
    engine = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=working_path), hooks=[]
    )
    # TODO: Detect if loaded
    start = datetime.now()
    #     engine.execute_raw_sql('''
    # INSTALL tpcds;
    # LOAD tpcds;
    # SELECT * FROM dsdgen(sf=1);''')
    print("loaded dataset:", datetime.now() - start)

    import cProfile

    cProfile.run("run_query(engine, start, 10)", "prof_stats.prof")
