"""Lock: a derivation over a merge-demoted key projects without its origin.

`merge source into label` demotes `label` to a lineage-less key whose value
comes only from `source`'s derivation. Every lineage reading `source` then reads
`label`, so the group computing `upper(label)` has to carry the origin column
itself: the plan rendered no column for it unless the origin was also selected,
and two optimizer folds that keep only a grouped child's columns dropped the
origin the kept column renders through.
"""

import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment

MODEL = """
key tree_id string;
key city enum<string>['X', 'Y'];
key raw_source enum<string>['A', 'B'];
auto source <- concat(raw_source, '');
property tree_id.label string;
merge source into label;
auto by_case <- case when source = 'B' then 'b' else 'a' end;
auto by_upper <- upper(label);
auto by_nested <- case when substring(source, 1, 1) = 'B' then 'b' else 'a' end;

root partial datasource part_a (tree_id: tree_id, city: city, data_source: raw_source)
grain (tree_id)
complete where city = 'X' and raw_source = 'A'
query '''SELECT 'a1' AS tree_id, 'X' AS city, 'A' AS data_source''';

root partial datasource part_b (tree_id: tree_id, city: city, data_source: raw_source)
grain (tree_id)
complete where city = 'X' and raw_source = 'B'
query '''SELECT 'b1' AS tree_id, 'X' AS city, 'B' AS data_source''';
"""


@pytest.fixture
def engine():
    env = Environment()
    env.parse(MODEL)
    return Dialects.DUCK_DB.default_executor(environment=env)


@pytest.mark.parametrize(
    "query, expected",
    [
        (
            "select tree_id, by_case where city = 'X' order by tree_id asc;",
            [("a1", "a"), ("b1", "b")],
        ),
        (
            "select tree_id, by_upper where city = 'X' order by tree_id asc;",
            [("a1", "A"), ("b1", "B")],
        ),
        (
            "select tree_id, by_nested where city = 'X' order by tree_id asc;",
            [("a1", "a"), ("b1", "b")],
        ),
        (
            (
                "select tree_id, case when label = 'B' then 1 else 0 end -> flag "
                "where city = 'X' order by tree_id asc;"
            ),
            [("a1", 0), ("b1", 1)],
        ),
        (
            (
                "select raw_source, by_case, count(tree_id) -> n where city = 'X' "
                "order by raw_source asc;"
            ),
            [("A", "a", 1), ("B", "b", 1)],
        ),
    ],
)
def test_derivation_over_demoted_key_projects(engine, query, expected):
    assert engine.execute_text(query)[-1].fetchall() == expected


@pytest.mark.parametrize(
    "query, expected",
    [
        ("select by_upper where city = 'X' order by by_upper asc;", [("A",), ("B",)]),
        (
            (
                "select by_upper, count(tree_id) -> n where city = 'X' "
                "order by by_upper asc;"
            ),
            [("A", 1), ("B", 1)],
        ),
        (
            (
                "select count(tree_id) by by_case -> n, by_case where city = 'X' "
                "order by by_case asc;"
            ),
            [(1, "a"), (1, "b")],
        ),
    ],
)
def test_grouped_derivation_over_demoted_key_keeps_origin(engine, query, expected):
    assert engine.execute_text(query)[-1].fetchall() == expected
