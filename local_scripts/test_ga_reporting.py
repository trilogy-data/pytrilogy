from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.execution.state.state_store import BaseStateStore
from pathlib import Path

if __name__ == "__main__":
    target_path = (
        Path(__file__).parent.parent.parent / "ga-reporting" / "thelook_ecommerce"
    )
    state_store = BaseStateStore()

    script = """
    #import users as u;
    import funnel_reporting as fr;
    #import sales_reporting as sr;
    #import customer_location as cs;
    """

    engine = Dialects.BIGQUERY.default_executor(
        environment=Environment(working_path=target_path)
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
        engine.update_datasource(engine.environment.datasources[asset.datasource_id])

    # Re-check for stale assets after persisting
    stale_assets = state_store.get_stale_assets(
        engine.environment,
        engine,
        root_assets={"source"},
    )
    print("\nStale assets post refresh:")
    for asset in stale_assets:
        print(f"  {asset.datasource_id}: {asset.reason}")
