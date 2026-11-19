import pytest

from tests.engine.test_duckdb_rowset_null_group_rejoin import (
    NESTED_ROWSET_QUERY,
    ROWSET_QUERY,
    UNSOLD_MODEL,
)
from tests.helpers.planning import Spy
from trilogy import Dialects
from trilogy.core.processing import concept_strategies_v4
from trilogy.core.processing.v4_node_generators import rowset_witness


@pytest.mark.parametrize(
    "query, bodies", [(ROWSET_QUERY, {"s"}), (NESTED_ROWSET_QUERY, {"s", "t"})]
)
def test_a_body_plan_groups_over_the_keyspace_its_witness_read(
    monkeypatch, query, bodies
):
    witnessed = Spy(rowset_witness.rowset_witness, lambda _, name, h, body, *a: body)
    grouped = Spy(
        concept_strategies_v4.build_group_graph, lambda _, *a, **kw: kw["keyspace"]
    )
    monkeypatch.setattr(rowset_witness, "rowset_witness", witnessed)
    monkeypatch.setattr(concept_strategies_v4, "build_group_graph", grouped)
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(UNSOLD_MODEL)
    executor.generate_sql(query)
    assert len(witnessed.seen) == len(bodies)
    for body in witnessed.seen:
        assert any(keyspace is body for keyspace in grouped.seen)
