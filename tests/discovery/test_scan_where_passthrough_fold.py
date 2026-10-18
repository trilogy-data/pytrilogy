"""A coarse attribute projected off a finer-grain row stream must ride that
stream's key, not be joined back on the coarse column.

`fit` is a species-grain `auto` over two enrichment columns. The count reads it
beside `tree_id`; the planner slices `tree_id` off the root and asks the `fit`
projection to absorb that slice (`_fold_passthrough_parents`). When `trees` is a
`partial ... complete where` source the slice is a direct scan carrying the
datasource's own `where` as its conditions, and the fold refused it: the gate
could not see that the projection's own scan of `trees` reads under that same
clause. The slice then had to be bridged back through the root merge, which
paired the projection to the trees on `species`: every species counted n**2
times (a 1.5M-tree city became a 40-billion-row join).

Each model variant below plans to the same three-CTE shape with the CASE inline
over the trees-to-species join and no GROUP BY above a join on `species`.
"""

import pytest

from tests.helpers.grain_contract import find_fanout_joins
from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException

_MODEL = """
key tree_id string;
key species string;
property tree_id.city string;
property tree_id.cluster_id string;
property species.zone_min int;
property species.zone_max int;

auto fit <- CASE
  when zone_min is null then 'Unknown'
  when 7 < zone_min or 7 > zone_max then 'Outside'
  else 'Inside'
end::string;

datasource enrichment (
  species,
  ?zone_min,
  ?zone_max,
)
grain (species)
query '''
select * from (values ('Acer', 3, 9), ('Quercus', 5, 6)) t(species, zone_min, zone_max)
''';

{partial}datasource trees (
  tree_id,
  species,
  city,
  cluster_id,
)
grain (tree_id)
{complete}query '''
select * from (values
  ('a','Acer','X','a'), ('b','Acer','X','b'), ('c','Acer','X','c'),
  ('d','Quercus','X','d'), ('e','Quercus','X','e'), ('f','Pinus','X','f')
) t(tree_id, species, city, cluster_id)
'''{where};
"""

_PARTIAL_WITH_WHERE = {
    "partial": "partial ",
    "complete": "complete where city = 'X'\n",
    "where": "\nwhere tree_id = cluster_id",
}

MODELS = [
    pytest.param(_PARTIAL_WITH_WHERE, id="partial_source_with_datasource_where"),
    pytest.param(
        dict(_PARTIAL_WITH_WHERE, where=""),
        id="partial_source",
    ),
    pytest.param(
        dict(_PARTIAL_WITH_WHERE, partial="", complete=""),
        id="datasource_where",
    ),
]

QUERIES = [
    pytest.param(
        "select fit, count(tree_id) as tree_count where city = 'X' order by tree_count desc;",
        id="count_by_bucket",
    ),
    pytest.param(
        "select fit, count(tree_id) as tree_count where city = 'X' and fit is not null order by tree_count desc;",
        id="count_by_bucket_filtered",
    ),
]


def _plan(model: dict[str, str], query: str):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_MODEL.format(**model))
    environment = executor.environment.materialize_for_select()
    processed = executor.parse_text(query)[-1]
    sql = executor.generator.compile_statement(processed)
    rows = executor.execute_raw_sql(sql).fetchall()
    return processed, environment, rows


@pytest.mark.parametrize("model", MODELS)
@pytest.mark.parametrize("query", QUERIES)
def test_bucket_count_rides_the_tree_key(model: dict[str, str], query: str):
    processed, environment, rows = _plan(model, query)
    assert not find_fanout_joins(processed.ctes, environment)
    assert len(processed.ctes) == 3, [cte.name for cte in processed.ctes]
    assert {row[0]: row[1] for row in rows} == {"Inside": 3, "Outside": 2}


def test_dependent_scalar_over_bucket_counts_once():
    _, _, rows = _plan(
        _PARTIAL_WITH_WHERE,
        "select fit, count(tree_id) as tree_count, "
        "case fit when 'Inside' then '#0f0' else '#f00' end::string as color "
        "where city = 'X' order by tree_count desc;",
    )
    assert {row[0]: (row[1], row[2]) for row in rows} == {
        "Inside": (3, "#0f0"),
        "Outside": (2, "#f00"),
    }


def test_partial_source_without_its_completing_filter_stays_unresolvable():
    """The tree key absorbed into the bucket projection keeps its partial stamp
    (`add_output_concepts`): without the `city` filter that completes the
    partition, the count has no complete source and the planner says so."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(_MODEL.format(**_PARTIAL_WITH_WHERE))
    with pytest.raises(UnresolvableQueryException):
        executor.parse_text("select fit, count(tree_id) as tree_count;")
