from trilogy.execution.state.state_store import BaseStateStore
from trilogy import Dialects

state_store = BaseStateStore()

script = """

key x int;
property x.value string;

datasource source (
x,
value
)
grain (x)
query '''
select 1 as x, 'hello' as value
union all
select 2 as x, 'world' as value
union all
select 3 as x, '!!!' as value
'''
incremental by x
;



datasource x_store (
x,
)
grain (x)
address x_store
incremental by x;

CREATE IF NOT EXISTS DATASOURCE x_store;


datasource x_store_two (
x,
)
grain (x)
address x_store_two
incremental by x;

CREATE IF NOT EXISTS DATASOURCE x_store_two;

RAW_SQL('''
insert into x_store_two select 1 union all select 2;
''');

"""

engine = Dialects.DUCK_DB.default_executor()

engine.execute_text(script)

# Watermark all assets at once
all_watermarks = state_store.watermark_all_assets(engine.environment, engine)
print("All watermarks:")
for ds_id, wm in all_watermarks.items():
    print(f"  {ds_id}: {wm}")

# Find stale assets - source is the root (has all 3 rows)
stale_assets = state_store.get_stale_assets(
    engine.environment,
    engine,
    root_assets={"source"},
)

print("\nStale assets:")
for asset in stale_assets:
    print(f"  {asset.datasource_id}: {asset.reason}")
    if asset.filters:
        print(f"filters: {asset.filters}")
        filters = []
        for key, filter in asset.filters.items():
            filter_string = f"{key} > {filter.value}"
            filters.append(filter_string)
        final_filter_string = " AND ".join(filters)
        cmd = f"APPEND {asset.datasource_id} WHERE {final_filter_string};"
    else:
        cmd = f"PERSIST {asset.datasource_id};"
    engine.execute_text(cmd)

# Re-check for stale assets after persisting
stale_assets = state_store.get_stale_assets(
    engine.environment,
    engine,
    root_assets={"source"},
)
print("\nStale assets post refresh:")
for asset in stale_assets:
    print(f"  {asset.datasource_id}: {asset.reason}")
