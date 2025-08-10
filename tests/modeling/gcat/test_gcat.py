from trilogy import Dialects, Environment
from pathlib import Path
from trilogy.hooks import DebuggingHook

def test_join():
    DebuggingHook()
    env = Environment(
        working_path = Path(__file__).parent,
    )
    base = Dialects.DUCK_DB.default_executor(environment=env)

    queries = base.parse_text(
        '''import launch;


where vehicle.name like '%Falcon%'
select 
platform.class,
# platform.name,
vehicle.name,
count(Launch_Tag) as launches;'''
    )

    sql = base.generate_sql(queries[-1])
    assert 'FULL JOIN' in sql[0], sql[0]