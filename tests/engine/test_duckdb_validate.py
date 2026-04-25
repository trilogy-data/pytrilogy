import pytest

from trilogy import Dialects
from trilogy.core.validation.fix import validate_and_rewrite


def test_validate():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    test = """validate all;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 0

    test = """validate datasources example;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 0


def test_validate_fix():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    test = """key x int; # guessing at type
# but who cares, right
key y int;

datasource dim_y (
    y: y
)
grain (y)
query '''
select 1 as y union all select 2 as y union all select 3 as y''';

# a fun comment
datasource example (
    x: x,
    y: y
    )
grain (x)
query '''
select 'abc' as x, 1 as y union all select null as x, null as y''';
"""
    rewritten = validate_and_rewrite(test, default_duckdb_engine)

    assert rewritten.strip() == """
key x string; # guessing at type
# but who cares, right
key y int;

datasource dim_y (
    y
)
grain (y)
query '''
select 1 as y union all select 2 as y union all select 3 as y''';
datasource example (
    x: ?x,
    y: ~?y
)
grain (x)
query '''
select 'abc' as x, 1 as y union all select null as x, null as y''';
""".strip(), rewritten.strip()


def test_validate_fix_types():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    test = """
import std.geography;
key x int; # guessing at type
key y int::latitude;
key z numeric::longitude;

# a fun comment
datasource example (
    x: x,
    y: y,
    z: z
)
grain (x)
query '''
select 'abc' as x, 1.0 as y, 2.0 as z union all select null as x, null as y, null as z''';
"""
    rewritten = validate_and_rewrite(test, default_duckdb_engine)

    assert rewritten.strip() == """import std.geography;

key x string; # guessing at type
key y numeric::latitude;
key z numeric::longitude;

datasource example (
    x: ?x,
    y: ?y,
    z: ?z
)
grain (x)
query '''
select 'abc' as x, 1.0 as y, 2.0 as z union all select null as x, null as y, null as z''';
""".strip(), rewritten.strip()


def test_validate_fix_bytes():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    test = """
key id int;
property id.payload string;

datasource example (
    id: id,
    payload: payload
)
grain (id)
query '''
select 1 as id, CAST('abc' AS BLOB) as payload''';
"""
    rewritten = validate_and_rewrite(test, default_duckdb_engine)

    assert rewritten is not None
    assert "property id.payload bytes;" in rewritten


def test_validate_duckdb_geometry_bytes_are_refined():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()

    try:
        default_duckdb_engine.execute_raw_sql("INSTALL spatial;")
        default_duckdb_engine.execute_raw_sql("LOAD spatial;")
    except Exception as exc:
        pytest.skip(f"DuckDB spatial extension unavailable: {exc}")

    test = """
import std.geography;
key id int;
property id.geom geography;

datasource example (
    id: id,
    geom: geom
)
grain (id)
query '''
select 1 as id, ST_GeomFromText('POINT(1 1)') as geom''';
"""
    rewritten = validate_and_rewrite(test, default_duckdb_engine)

    assert rewritten is None


def test_show_validate():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    test = """validate all;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 0
    for row in results:
        assert row.ran is True, str(row)

    test = """show validate all;"""

    results = default_duckdb_engine.execute_text(test)[0].fetchall()
    assert len(results) == 1
    for row in results:
        assert row.ran is False or row.check_type == "logical"


def test_geo_functions_e2e():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    try:
        default_duckdb_engine.execute_raw_sql("INSTALL spatial;")
        default_duckdb_engine.execute_raw_sql("LOAD spatial;")
    except Exception as exc:
        pytest.skip(f"DuckDB spatial extension unavailable: {exc}")

    test = """
key id int;
key raw_wkt string;

datasource geo_shape (
    id: id,
    raw_wkt: raw_wkt
)
grain (id)
query '''
select
    1 as id,
    'POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))' as raw_wkt
''';

auto shape <- geo_from_text(raw_wkt);
auto centroid <- geo_centroid(shape);
auto centroid_4326 <- geo_transform(centroid, 4326, 4326);
auto center_x <- geo_x(centroid_4326);
auto center_y <- geo_y(centroid_4326);

select center_x, center_y;
"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    assert len(results) == 1
    assert results[0].center_x is not None
    assert results[0].center_y is not None
    assert round(results[0].center_x, 6) == 2.0
    assert round(results[0].center_y, 6) == 2.0


def test_show_validate_generation():
    default_duckdb_engine = Dialects.DUCK_DB.default_executor()
    test = """
key x int;

datasource example (
x)
grain (x)
query '''
select 1 as x''';

where x = 1
SELECT unnest([1,2,3,4]) as value, 'example' as dim
having value = 2;
"""
    results = default_duckdb_engine.execute_text(test)[0].fetchall()

    test = """show validate all;"""

    results = default_duckdb_engine.parse_text(test)

    default_duckdb_engine.generate_sql(results[0])
