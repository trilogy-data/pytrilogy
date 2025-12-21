from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.execution.state.state_store import BaseStateStore
from trilogy.hooks import DebuggingHook

if __name__ == "__main__":
    target_path = (
        Path(__file__).parent.parent.parent / "ga-reporting" / "thelook_ecommerce"
    )
    state_store = BaseStateStore()
    DebuggingHook()

    script = """
import funnel_reporting;


select events.created_at.date, product_count
order by events.created_at.date desc;
    """

    engine = Dialects.BIGQUERY.default_executor(
        environment=Environment(working_path=target_path)
    )

    results = engine.execute_text(script)
    for row in results[-1].fetchall()[0:10]:
        print(row)
