"""Replication case for coalesce+aggregate imputation merged into a property,
with a WHERE filter on city — mirrors the Boston tree reporting query shape."""

from trilogy import Dialects, parse


def test_merge_coalesce_impute_generates_sql():
    """Replication of boston tree query:
    _raw_dbh -> _cleaned_db (case) -> processed_dbh (coalesce+avg impute)
    -> merge into diameter_at_breast_height, filtered by city."""
    from trilogy.hooks import DebuggingHook
    from logging import INFO
    DebuggingHook(INFO)
    query = """
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


where city = 'USBOS'
select
    tree_id,
    diameter_at_breast_height
;
"""
    env, stmts = parse(query)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    sql = exec.generate_sql(stmts[-1])[-1]
    assert exec.environment.concepts['processed_dbh'].keys == {'local.tree_id'}
    build = env.materialize_for_select()
    assert build.concepts['processed_dbh'].keys == {'local.tree_id'}
    print(sql)
    assert sql =='abcs'  # placeholder — failure conditions TBD
