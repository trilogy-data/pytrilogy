"""A `by *` aggregate is a constant of the query: the rows join its one-row
result, never the table it aggregates (#953)."""

import pytest

from tests.helpers.planning import plan
from tests.helpers.rows import executor_for, fetch_rows
from trilogy.core.enums import Derivation

MODEL = """
key tree_id string;
key genus string;
property tree_id.tree_class string;

key fit_class string;
property fit_class.fit_a float;

root datasource trees (tree_id: tree_id, genus: genus, tree_class: tree_class)
grain (tree_id)
query '''select * from (values ('t1','acer','broadleaf'), ('t2','pinus','conifer'),
('t3','acer','broadleaf')) t(tree_id, genus, tree_class)''';

root datasource class_fits (fit_class: fit_class, a: fit_a)
grain (fit_class)
query '''select * from (values ('broadleaf', 1.0), ('conifer', 2.0), ('palm', 3.0),
('all', 4.0)) t(fit_class, a)''';

auto broadleaf_a <- max(fit_a ? fit_class = 'broadleaf') by *;
auto conifer_a <- max(fit_a ? fit_class = 'conifer') by *;
auto tree_a <- case when tree_class = 'broadleaf' then broadleaf_a
    when tree_class = 'conifer' then conifer_a end;
auto top_a <- max(fit_a) by *;
auto top_broadleaf <- case when tree_class = 'broadleaf' then top_a end;
"""

CASES = [
    ("select tree_id, tree_a;", [("t1", 1.0), ("t2", 2.0), ("t3", 1.0)]),
    ("select tree_id, top_broadleaf;", [("t1", 4.0), ("t2", None), ("t3", 4.0)]),
    ("select tree_class, top_broadleaf;", [("broadleaf", 4.0), ("conifer", None)]),
]


@pytest.mark.parametrize("query,expected", CASES)
def test_rows_never_join_the_aggregated_table(query: str, expected: list) -> None:
    info, _ = plan(MODEL, query)
    for attrs in info.group_attrs.values():
        if attrs.derivation == Derivation.ROOT:
            members = set(attrs.primary_members)
            assert not ({"local.fit_a", "local.fit_class"} & members) or not (
                {"local.tree_id", "local.tree_class"} & members
            ), members
    executor = executor_for(MODEL)
    sql = executor.generate_sql(query)[-1]
    assert "FULL JOIN" not in sql and "GROUP BY" not in sql.split("SELECT")[-1]
    rows = sorted(
        (a, None if b is None else float(b)) for a, b in fetch_rows(executor, query)
    )
    assert rows == expected
