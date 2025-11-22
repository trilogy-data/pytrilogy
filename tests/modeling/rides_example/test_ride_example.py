from pathlib import Path

from trilogy import Dialects, Environment
from trilogy.hooks import DebuggingHook


def test_example_model():
    env = Environment(working_path=Path(__file__).parent)

    executor = Dialects.DUCK_DB.default_executor(environment=env)

    DebuggingHook()

    results = executor.execute_text(
        """
import script;
WHERE end_station.id in (start_station.id ? daily_station_rank <= 3)
SELECT
    ride_id,
    start_station.id,
    end_station.id,
    ride_seconds
order by ride_id asc
;

                          """
    )
    rows = list(results[-1].fetchall())
    for row in rows:
        print(row)
    assert rows[0].ride_id == 4
