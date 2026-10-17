"""A coarse derivation projected off a finer-grain parent must not be joined
back on the coarse key.

`fit` is a property of `species`, planned off a tree-grain parent (trees joined
to the species table) when the trees are a partitioned datasource carrying its
own row filter. The projection emitted one row per tree while claiming species
grain, and the count joined it back on species: every species counted
n_species**2 times (9 and 4 for 3 and 2 trees). A 1.5M-tree city turned that
into a 40-billion-row join that never finished.
"""

import pytest

from tests.helpers.grain_contract import find_fanout_joins
from trilogy import Dialects

SPECIES_BUCKET_OVER_FILTERED_PARTITION = """
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

partial datasource trees (
  tree_id,
  species,
  city,
  cluster_id,
)
grain (tree_id)
complete where city = 'X'
query '''
select * from (values
  ('a','Acer','X','a'), ('b','Acer','X','b'), ('c','Acer','X','c'),
  ('d','Quercus','X','d'), ('e','Quercus','X','e'), ('f','Pinus','X','f')
) t(tree_id, species, city, cluster_id)
'''
where tree_id = cluster_id;
"""

CASES = [
    pytest.param(
        "select fit, count(tree_id) as tree_count where city = 'X' order by tree_count desc;",
        True,
        id="count_by_species_bucket",
    ),
    pytest.param(
        "select fit, count(tree_id) as tree_count where city = 'X' and fit is not null order by tree_count desc;",
        True,
        id="count_by_species_bucket_with_bucket_filter",
    ),
    # The scalar over the bucket is read back off the tree-grain parent and the
    # final select regroups, which the plan-time checker cannot see through;
    # the row counts are the contract here.
    pytest.param(
        "select fit, count(tree_id) as tree_count, case fit when 'Inside' then '#0f0' else '#f00' end::string as color where city = 'X' order by tree_count desc;",
        False,
        id="count_by_species_bucket_with_dependent_scalar",
    ),
]


@pytest.mark.parametrize("query,check_plan", CASES)
def test_species_bucket_count_does_not_fan_out(query: str, check_plan: bool):
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(SPECIES_BUCKET_OVER_FILTERED_PARTITION)
    environment = executor.environment.materialize_for_select()

    processed = executor.parse_text(query)[-1]
    if check_plan:
        violations = find_fanout_joins(processed.ctes, environment)
        assert not violations, "\n".join(str(v) for v in violations)

    rows = executor.execute_raw_sql(
        executor.generator.compile_statement(processed)
    ).fetchall()
    counts = {row[0]: row[1] for row in rows}
    # Pinus has no species row, so its bucket is null; the inner join may drop
    # it or the outer may keep it as 'Unknown'. Either way no bucket is squared.
    assert counts["Inside"] == 3
    assert counts["Outside"] == 2
    assert sum(counts.values()) <= 6


def test_projection_keeps_the_parent_grain_it_inherits():
    """The repair itself: the bucket's projection is stamped with its parent's
    tree grain, so it has to expose tree_id for a consumer to merge on."""
    executor = Dialects.DUCK_DB.default_executor()
    executor.execute_text(SPECIES_BUCKET_OVER_FILTERED_PARTITION)
    processed = executor.parse_text(
        "select fit, count(tree_id) as tree_count where city = 'X';"
    )[-1]
    for cte in processed.ctes:
        if cte.group_to_grain or not cte.parent_ctes:
            continue
        exposed = {c.address for c in cte.output_columns}
        assert (
            set(cte.grain.components) <= exposed
        ), f"{cte.name} claims grain {cte.grain.components} but exposes {exposed}"
