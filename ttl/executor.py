from sqlalchemy import *
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import *
engine = create_engine('bigquery://project')


with engine.connect() as con:

    rs = con.execute('SELECT * FROM book')

    for row in rs:
        print(row)