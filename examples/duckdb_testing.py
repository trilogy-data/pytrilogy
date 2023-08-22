

from preql import Executor, Dialects
from sqlalchemy import create_engine

engine = create_engine(r"duckdb:///:memory:", future=True)

test = Executor(engine=engine, dialect=Dialects.DUCK_DB)


results = test.execute_text('select 1 -> test;')


for row in results[0].fetchall():
    print(row)