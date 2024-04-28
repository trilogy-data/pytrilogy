from preql import Dialects
from trilogy_public_models import models

env = models["bigquery.stack_overflow"]

print(env.concepts["question.creation_date.year"].address)

executor = Dialects.BIGQUERY.default_executor(environment=env)

executor.execute_text(
    """SELECT
    question.count,
    question.creation_date.year,
ORDER BY
    question.count desc

LIMIT 100;"""
)
