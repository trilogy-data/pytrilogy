
from trilogy_public_models import models
from preql import Executor, Dialects
from preql.hooks.query_debugger import DebuggingHook
from preql.core.processing.concept_strategies_v2 import resolve_function_parent_concepts

environment = models['bigquery.thelook_ecommerce']
executor = Dialects.BIGQUERY.default_executor(environment=environment, hooks=[DebuggingHook()])
# environment.parse('auto question.answer.count <- count(answer.id) by question.id;')
# environment.parse('auto question.answer.count.avg <- answer.count/ question.count;')

# test = environment.concepts['question.answer.count.avg']
# # print(test.lineage)
# print(test.derivation)
# print(test.lineage.concept_arguments)
# parents = resolve_function_parent_concepts(environment.concepts['question.answer.count.avg'])

# for x in parents:
#     print(x)

results = executor.execute_text(
'''key cancelled_orders <- filter orders.id where orders.status = 'Cancelled';
auto orders.id.cancelled_count <- count(cancelled_orders);

SELECT
    orders.id.cancelled_count / orders.id.count -> cancellation_rate,
    orders.id.cancelled_count,
    orders.id.count,
    orders.created_at.year,
    users.city,
WHERE
    (orders.created_at.year = 2020)
    and orders.id.count>20
ORDER BY
    cancellation_rate desc
LIMIT 10;''',


)
for row in results:
    answers = row.fetchall()
    for x in answers:
        print(x)



