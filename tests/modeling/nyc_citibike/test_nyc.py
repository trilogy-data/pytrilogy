from trilogy import Environment
from pathlib import Path
from trilogy.core.models import Function


def test_datasource_func_namespace():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    year_assignment = env.datasources["trip.citibike_trips"].columns[-1]

    assert isinstance(year_assignment.alias, Function)
    assert year_assignment.alias.arguments[0].namespace == "trip"


def test_rendering():
    pass
