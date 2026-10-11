"""Lock: a merged-key output keeps the WHERE's aggregate gate connected.

`merge m into label` computes `label` under its origin's name, so the group
producing it listed only `m`. Seeding the main lineage by output address
missed it, the gate's host looked disconnected from every output, and
`select id, label where id = anchor` raised DisconnectedConceptsException
while the same select of `m` planned.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key id int;
key label string;
property id.g int;
property id.v string;
auto m <- max(v) by g;
auto anchor <- min(id) by g;
merge m into label;
datasource t (id: id, g: g, v: v) grain (id)
query '''SELECT * FROM (VALUES (1, 1, 'a'), (2, 1, 'z'), (3, 2, 'b')) AS t(id, g, v)''';
"""


def _rows(query: str) -> list[tuple]:
    env = Environment()
    env.parse(MODEL)
    return sorted(
        Dialects.DUCK_DB.default_executor(environment=env)
        .execute_query(query)
        .fetchall()
    )


@pytest.mark.parametrize("where", ["id = anchor", "anchor = 1"])
def test_merged_key_output_plans_like_its_origin(where: str):
    assert _rows(f"select id, label where {where};") == _rows(
        f"select id, m where {where};"
    )
