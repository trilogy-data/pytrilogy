from pathlib import Path

from trilogy.core.models.author import Function
from trilogy.core.models.environment import Environment


def test_datasource_func_namespace():
    env = Environment.from_file(Path(__file__).parent / "entrypoint.preql")
    year_assignment = env.datasources["trip.citibike_trips"].columns[-1]

    assert isinstance(year_assignment.alias, Function)
    assert year_assignment.alias.arguments[0].address.startswith("trip.")


def test_rendering():
    pass
