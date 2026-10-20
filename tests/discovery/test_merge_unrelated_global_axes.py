"""Lock (#758): two unrelated global `merge`s in one namespace are not legs of
one composite relation.

`count(tree_id)` beside `min(ecoregion_id) by species` over an unnest+merge axis
must count at tree grain. A second, unrelated `merge modified_tree_id into
~tree_id` used to widen the global count's input grain with the ecoregion axis
(`_composite_relation_sibling_axes` read both merge groups as one composite
join), so the count read the tree x ecoregion stream and fanned out."""

from trilogy import Dialects
from trilogy.core.models.environment import Environment

_MODEL = """
key tree_id int;
key species string;
property species.native_ecoregions list<int>;
key ecoregion_id int;
auto tree_eco_region <- unnest(native_ecoregions);
merge tree_eco_region into ecoregion_id;
key modified_tree_id int;
merge modified_tree_id into ~tree_id;

datasource trees (tree_id, species) grain (tree_id)
query '''select 1 tree_id, 'a' species union all select 2, 'a' union all select 3, 'b' ''';
datasource enrich (species, ?native_ecoregions) grain (species)
query '''select 'a' species, [1, 2, 3] native_ecoregions union all select 'b', [2, 3]''';
datasource eco (ecoregion_id) grain (ecoregion_id)
query '''select 1 ecoregion_id union all select 2 union all select 3''';
datasource tree_modifications (tree_id: modified_tree_id) grain (modified_tree_id)
query '''select 1 tree_id''';

auto rank_min <- min(ecoregion_id) by species;
"""

_QUERY = "select count(tree_id ? rank_min = 1) as c, count(tree_id) as t;"


def test_unrelated_merges_keep_tree_grain_count():
    env = Environment()
    engine = Dialects.DUCK_DB.default_executor(environment=env)
    engine.parse_text(_MODEL)
    rows = engine.execute_text(_QUERY)[-1].fetchall()
    assert [tuple(r) for r in rows] == [(2, 3)]
