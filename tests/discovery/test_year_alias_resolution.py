from trilogy import Dialects

SETUP_CODE = """
key id int;
property id.flight_date date;

auto flight_year <- year(flight_date);
auto flight_count <- count(id);

datasource flights (
    id:id,
    flight_date:flight_date,
)
grain (id)
query '''
select 1 as id, '2024-01-15'::date as flight_date
union all
select 2 as id, '2024-01-16'::date as flight_date
union all
select 3 as id, '2025-01-15'::date as flight_date
''';

datasource flight_count_by_year (
    flight_year:flight_year,
    flight_count:flight_count,
)
grain (flight_year)
query '''
select 2024 as flight_year, 2 as flight_count
union all
select 2025 as flight_year, 1 as flight_count
''';
"""


def test_resolve_inline_alias_alongside_named():
    """Multiple distinct addresses with the same canonical lineage in a single
    select must all be produced. Previously raised
    `Invalid input concepts to node! ['local.flight_year2'] are missing
    non-hidden parent nodes` because the discovery layer collapsed both
    distinct concepts into one canonical entry."""
    exec_ = Dialects.DUCK_DB.default_executor()
    exec_.parse_text(SETUP_CODE)

    generated = exec_.generate_sql("""
SELECT
    year(flight_date) as flight_year2,
    flight_date.year,
    count(id) as flight_count
;
""")[-1]
    # All three columns appear in the projection.
    assert "flight_year2" in generated, generated
    assert "flight_date_year" in generated, generated
    assert "flight_count" in generated, generated


def test_resolve_attr_access_alongside_named():
    """`flight_date.year` (attr-access form) and `flight_year` (named auto)
    have the same lineage; selecting both must not error."""
    exec_ = Dialects.DUCK_DB.default_executor()
    exec_.parse_text(SETUP_CODE)

    generated = exec_.generate_sql("""
SELECT
    flight_date.year,
    flight_year,
    count(id) as flight_count
;
""")[-1]
    assert "flight_date_year" in generated, generated
    assert "flight_year" in generated, generated
    assert "flight_count" in generated, generated


def test_resolve_via_named_concept_uses_aggregate():
    """Sanity: querying through the named alias hits the pre-aggregated source."""
    exec_ = Dialects.DUCK_DB.default_executor()
    exec_.parse_text(SETUP_CODE)

    generated = exec_.generate_sql("""
SELECT
    flight_year,
    count(id) as flight_count
;
""")[-1]
    assert (
        "flight_count_by_year" in generated
    ), f"Expected flight_count_by_year, got: {generated}"
