from trilogy.execution.state.state_store import BaseStateStore
from trilogy import Dialects, Environment
from pathlib import Path

state_store = BaseStateStore()

script = """
#import users as u;
import funnel_reporting as fr;
#import sales_reporting as sr;
#import customer_location as cs;
"""

engine = Dialects.BIGQUERY.default_executor(
    environment=Environment(
        working_path=r"C:\Users\ethan\coding_projects\ga-reporting\thelook_ecommerce"
    )
)

engine.execute_text(script)

# Watermark all assets at once
all_watermarks = state_store.watermark_all_assets(engine.environment, engine)
print("All watermarks:")
for ds_id, wm in all_watermarks.items():
    print(f"  {ds_id}: {wm}")

# Find stale assets - uses is_root from datasource definitions
stale_assets = state_store.get_stale_assets(
    engine.environment,
    engine,
)

print("\nStale assets:")
for asset in stale_assets:
    print(f"  {asset.datasource_id}: {asset.reason}")
    if asset.filters:
        print(f"filters: {asset.filters}")
#         filters = []
#         for key, filter in asset.filters.items():
#             filter_string = f"{key} > {filter.value}"
#             filters.append(filter_string)
#         final_filter_string = " AND ".join(filters)
#         cmd = f"APPEND {asset.datasource_id} WHERE {final_filter_string};"
#     else:
#         cmd = f"PERSIST {asset.datasource_id};"
#     engine.execute_text(cmd)

# # Re-check for stale assets after persisting
# stale_assets = state_store.get_stale_assets(
#     engine.environment,
#     engine,
#     root_assets={"source"},
# )
# print("\nStale assets post refresh:")
# for asset in stale_assets:
#     print(f"  {asset.datasource_id}: {asset.reason}")
