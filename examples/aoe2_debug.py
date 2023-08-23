from trilogy_public_models import models
from preql import Executor, Dialects
from preql.hooks.query_debugger import DebuggingHook, PrintMode
from preql.core.processing.concept_strategies_v2 import resolve_function_parent_concepts
from preql.core.models import CTE, QueryDatasource, Grain
from logging import INFO

environment = models["bigquery.age_of_empires_2"]

executor = Dialects.BIGQUERY.default_executor(
    environment=environment,
    hooks=[
        DebuggingHook(
            level=INFO,
            process_ctes=False,
            process_nodes=PrintMode.BASIC,
            process_other=False,
            process_datasources=PrintMode.BASIC,
        ),
    ],
)

text = """
key longbow_creation <- filter unit_creations.id
where units.id = 8;

key games_with_longbows <- filter matches.id where longbow_creation.count>1;

persist game_ids_with_longbows into aoe2.game_ids_with_longbows
select games_with_longbows;

key unit_creations_in_longbow_games <- filter unit_creations.id where matches.id = games_with_longbows;


select 
    units.name, 
    unit_creations.id.count,
    unit_creations_in_longbow_games.count,
    (unit_creations.id.count - unit_creations_in_longbow_games.count) / unit_creations.id.count  -> percent_longbow_vs_all
order by unit_creations_in_longbow_games.count desc;
"""

# executor.parse_text(text)


results = executor.execute_text(text)


idx = 0
answers = None
for row in results:
    answers = row.fetchall()
else:
    for x in answers:
        print(x)
