"""Replication case for coalesce+aggregate imputation merged into a property,
with a WHERE filter on city — mirrors the Boston tree reporting query shape."""

from trilogy import Dialects, parse
from trilogy.core.models.build import concept_is_relevant


_BASE_QUERY = """
key tree_id int;
property tree_id.city string;
property tree_id.species string;
property tree_id._raw_dbh float;
property tree_id.diameter_at_breast_height float;

auto processed_dbh <- group(coalesce(_raw_dbh, avg(_raw_dbh) by city, species)) by tree_id;

merge processed_dbh into diameter_at_breast_height;

datasource boston_trees (
    tree_id: tree_id,
    city: city,
    species: species,
    raw_dbh: _raw_dbh,
)
grain (tree_id)
query '''
select 1 tree_id, 'USBOS' city, 'Oak' species, 5.0 raw_dbh, 5.0 dbh
union all select 2, 'USBOS', 'Maple', -1.0, null
union all select 3, 'USBOS', 'Oak', null, null
union all select 4, 'USNYC', 'Oak', 3.0, 3.0
''';
"""


def test_virt_agg_grain_collapses_when_by_concepts_are_properties():
    """_virt_agg_avg should not be relevant when its by-concepts (city, species)
    are properties of tree_id, which is already in others."""
    env, _ = parse(_BASE_QUERY + "\nselect tree_id;")
    build = env.materialize_for_select()

    tree_id = build.concepts["tree_id"]
    virt_agg = next(
        c for c in build.concepts.values() if c.name.startswith("_virt_agg_avg")
    )

    assert not concept_is_relevant(virt_agg, [tree_id])


def test_merge_coalesce_impute_no_group_by():
    query = (
        _BASE_QUERY
        + """
where city = 'USBOS'
select
    tree_id,
    diameter_at_breast_height
;
"""
    )
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]
    assert exec.environment.concepts["processed_dbh"].keys == {"local.tree_id"}
    # highfalutin legitimately groups by city+species for the avg; cheerful must not
    cheerful_onward = sql[sql.index("cheerful as ("):]
    assert "GROUP BY" not in cheerful_onward, sql
