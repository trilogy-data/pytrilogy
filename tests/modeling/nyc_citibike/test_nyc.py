from pathlib import Path

from trilogy import BoundEnvironment
from trilogy.core.execute_models import BoundFunction


def test_datasource_func_namespace():
    env = BoundEnvironment.from_file(Path(__file__).parent / "entrypoint.preql")
    year_assignment = env.datasources["trip.citibike_trips"].columns[-1]

    assert isinstance(year_assignment.alias, BoundFunction)
    assert year_assignment.alias.arguments[0].namespace == "trip"


def test_rendering():
    pass
