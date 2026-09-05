"""An UNNEST drops the parent rows whose array is empty or NULL, so the parent
columns it carries through bind a subset of the parent's rows. Stamping them
partial keeps a join onto the unnested side row-preserving toward the fact
whatever other source for the fact's key is in scope."""

import pytest

from trilogy import Dialects
from trilogy.core.enums import JoinType, SourceType
from trilogy.core.models.execute import BaseJoin, QueryDatasource
from trilogy.core.query_processor import get_query_datasources
from trilogy.parser import parse_text

MODEL = """
key tree_id string;
property tree_id.city string;
key species string;

property species.native_ecoregions list<int>;
auto tree_eco_region <- unnest(native_ecoregions);
merge tree_eco_region into ecoregion_id;

key ecoregion_id int;
property ecoregion_id.biome string;

property native_locality_bucket <- CASE
    WHEN ecoregion_id = 423 THEN 'Native'
    WHEN biome = 'Mediterranean Forests' THEN 'Same biome, non-native'
  ELSE 'Non-Native, Different Biome'
END::string;

root partial datasource city_trees (
    tree_id: tree_id,
    city: city,
    species: species,
)
grain (tree_id)
complete where city = 'USSFO'
query '''
select 't1' as tree_id, 'USSFO' as city, 'Quercus agrifolia' as species
union all select 't2', 'USSFO', 'Platanus x hispanica'
union all select 't3', 'USSFO', 'Unknown'
''';

root datasource species_enrichment (
    species: species,
    native_ecoregions: native_ecoregions,
)
grain (species)
query '''
select 'Quercus agrifolia' as species, [423] as native_ecoregions
union all select 'Platanus x hispanica', [664]
''';

root datasource ecoregions (
    ecoregion_id: ecoregion_id,
    biome: biome,
)
grain (ecoregion_id)
query '''
select 423 as ecoregion_id, 'Mediterranean Forests' as biome
union all select 664, 'Temperate Broadleaf & Mixed Forests'
''';
"""

# the same rows unpartitioned; never named by the query
SECOND_SOURCE = """
root datasource all_trees (
    tree_id: tree_id,
    city: city,
    species: species,
)
grain (tree_id)
query '''
select 't1' as tree_id, 'USSFO' as city, 'Quercus agrifolia' as species
union all select 't2', 'USSFO', 'Platanus x hispanica'
union all select 't3', 'USSFO', 'Unknown'
union all select 't4', 'USNYC', 'Acer rubrum'
''';
"""

QUERY = """
select count(tree_id) -> tree_count
where city = 'USSFO' and native_locality_bucket = 'Non-Native, Different Biome';
"""


def _nodes(qds: QueryDatasource) -> list[QueryDatasource]:
    out: list[QueryDatasource] = []
    stack = [qds]
    while stack:
        current = stack.pop()
        if isinstance(current, QueryDatasource):
            out.append(current)
            stack.extend(current.datasources)
    return out


def _tree_join(qds: QueryDatasource) -> BaseJoin:
    for node in _nodes(qds):
        for join in node.joins:
            if isinstance(join, BaseJoin) and any(
                pair.right.address == "local.species" for pair in join.concept_pairs
            ):
                return join
    raise AssertionError("no join on species")


@pytest.mark.parametrize("extra", ["", SECOND_SOURCE], ids=["one", "two"])
def test_unnested_dimension_join_preserves_trees(extra: str):
    executor = Dialects.DUCK_DB.default_executor()
    executor.parse_text(MODEL + extra)
    _, statements = parse_text(QUERY, executor.environment)
    final = get_query_datasources(executor.environment, statements[-1])

    unnest = next(n for n in _nodes(final) if n.source_type == SourceType.UNNEST)
    assert "local.species" in {c.address for c in unnest.partial_concepts}
    assert "local.tree_eco_region" not in {c.address for c in unnest.partial_concepts}

    join = _tree_join(final)
    trees = join.right_datasource.identifier
    assert trees.startswith(("city_trees", "all_trees")), trees
    assert join.join_type in (JoinType.RIGHT_OUTER, JoinType.FULL)

    assert executor.execute_text(QUERY)[-1].fetchall() == [(2,)]
