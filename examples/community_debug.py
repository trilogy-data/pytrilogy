from trilogy_public_models import models
from preql import Dialects
from preql.core.processing.concept_strategies_v2 import resolve_function_parent_concepts

environment = models["bigquery.stack_overflow"]
executor = Dialects.BIGQUERY.default_executor(environment=environment)
environment.parse("auto question.answer.count <- count(answer.id) by question.id;")
environment.parse("auto question.answer.count.avg <- answer.count/ question.count;")

test = environment.concepts["question.answer.count.avg"]
# print(test.lineage)
print(test.derivation)
print(test.lineage.concept_arguments)
parents = resolve_function_parent_concepts(
    environment.concepts["question.answer.count.avg"]
)

for x in parents:
    print(x)

results = executor.execute_text(
    """SELECT
question.answer.count.avg,
limit 100;"""
)
for row in results:
    answers = row.fetchall()
    for x in answers:
        print(x)
