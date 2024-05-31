
from preql import Environment, Dialects

executor = Dialects.DUCK_DB.default_executor(environment=Environment())

results = executor.execute_text("""
key user_id int;
property user_id.name string;

datasource users (
    uid:user_id,
    name:name
)
grain(user_id)
query '''
select 1 uid, 'Bach' as "name"
union all 
select 2, 'Mozart',
union all
select 3, 'Beethoven'
''';

# run our query
select 
    user_id,
    name,
    len(name)->name_length
;

""")


for rs in results:
    for row in rs.fetchall():
        print(row)