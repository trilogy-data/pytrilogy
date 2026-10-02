"""A group that builds nothing must not silently drop the WHERE it hosts.

Skipping the condition-hosting group here used to leave the aggregate group
delivering `g` unfiltered: both groups came back, not just the one passing
`m > 5`.
"""

import pytest

import trilogy.core.processing.v4_node_generators as generators
from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.environment import Environment

MODEL = """
key id int;
property id.g string;
property id.x int;
datasource facts (id: id, g: g, x: x) grain (id)
query '''select 1 id, 'a' g, 10 x union all select 2 id, 'b' g, 1 x''';
"""

QUERY = "auto m <- sum(x) by g; select g where m > 5;"


def _engine():
    env = Environment()
    env.parse(MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


def test_condition_hosting_group_plans():
    assert _engine().execute_text(QUERY)[-1].fetchall() == [("a",)]


def test_unbuilt_condition_hosting_group_raises(monkeypatch):
    real = generators.build_node
    monkeypatch.setattr(
        generators,
        "build_node",
        lambda **kwargs: None if kwargs["conditions"] is not None else real(**kwargs),
    )
    with pytest.raises(UnresolvableQueryException) as exc:
        _engine().generate_sql(QUERY)
    message = str(exc.value)
    assert "grp:root" in message
    assert "local.m > 5" in message
