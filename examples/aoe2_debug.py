from trilogy_public_models import models
from preql import Dialects
from preql.hooks.query_debugger import DebuggingHook, PrintMode
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

metric creations_per_longbow_game <- count(unit_creations_in_longbow_games) by matches.id;
metric creations_per_game <- count(unit_creations.id) by matches.id;


select 
    units.name, 
    avg(creations_per_game) -> normal_avg
order by 
    unit_creations_in_longbow_games.count desc;
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
