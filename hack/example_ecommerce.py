from preql.parser import parse
from preql.dialect.bigquery import BigqueryDialect
from preql import Executor, Environment, Dialects

from sqlalchemy import create_engine

with open("demo/ecommerce/ecommerce.preql", "r") as f:
    text = f.read()

env = Environment(namespace="demo/ecommerce", working_path="demo/ecommerce")

env, parsed = parse(text, environment=env)

query = "import concepts.order as order; SELECT order.count;"

env, statements = parse(query, environment=env)

generator = BigqueryDialect()

sql = generator.generate_queries(environment=env, statements=statements)


engine = create_engine("bigquery://", credentials_path="bigquery.json")

executor = Executor(dialect=Dialects.BIGQUERY, engine=engine)

for statement in sql:
    compiled = generator.compile_statement(statement)
    results = executor.execute_query(statement)


all = results.fetchall()

print(all)