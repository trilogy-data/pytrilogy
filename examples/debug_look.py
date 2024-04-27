
nb_path = __file__
from sys import path
from os.path import dirname

root_path = dirname(dirname(nb_path))
path.insert(0, r"C:\Users\ethan\coding_projects\trilogy-public-models")
path.insert(0, r"C:\Users\ethan\coding_projects\pypreql")
print(root_path)
from preql import Dialects
from trilogy_public_models import models

env = models["bigquery.usa_names"]


print(env.concepts["name_count.sum"])

executor = Dialects.BIGQUERY.default_executor(environment=env)

print(type(executor.environment.concepts["name_count.sum"].lineage.arguments[0]))
results = executor.execute_text(
    """

key vermont_names <- filter name where state = 'VT';


SELECT
    vermont_names,
    name_count.sum,
    year
where
    year = 1950
order by name_count.sum desc
LIMIT 100;"""
)

for row in results[0].fetchall():
    print(row)
