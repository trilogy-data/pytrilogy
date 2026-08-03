"""Snowflake array membership, executed rather than shape-asserted.

`FLATTEN` hands back a VARIANT, so the identity predicate has to cast it to
what the probe presents. Rendering alone cannot catch getting that wrong: the
uncast form is valid SQL that simply matches nothing.
"""

from trilogy import Environment

MODEL = """
key cust_zip string;
property cust_zip.allowed string;

datasource customers (
    cust_zip: cust_zip,
    allowed: allowed
)
grain (cust_zip)
address zip_membership;
"""


def _seeded(snowflake_engine):
    snowflake_engine.execute_raw_sql(
        "CREATE OR REPLACE TABLE ZIP_MEMBERSHIP (CUST_ZIP VARCHAR, ALLOWED VARCHAR)"
    )
    snowflake_engine.execute_raw_sql(
        "INSERT INTO ZIP_MEMBERSHIP VALUES "
        "('24128', '24128,76232'), ('76232', '24128,76232'), ('99999', '24128,76232')"
    )
    snowflake_engine.environment = Environment()
    snowflake_engine.parse_text(MODEL)
    return snowflake_engine


def test_array_valued_membership_matches_elements(snowflake_engine):
    engine = _seeded(snowflake_engine)
    rows = engine.execute_query(
        "where cust_zip in split(allowed, ',') select cust_zip order by cust_zip asc;"
    ).fetchall()
    assert [r[0] for r in rows] == ["24128", "76232"]


def test_array_valued_membership_excludes_non_elements(snowflake_engine):
    engine = _seeded(snowflake_engine)
    rows = engine.execute_query(
        "where cust_zip not in split(allowed, ',') select cust_zip;"
    ).fetchall()
    assert [r[0] for r in rows] == ["99999"]
