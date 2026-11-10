import re

import pytest

from trilogy import Dialects, Environment
from trilogy.execution.state.isolation import hidden_datasources

FANOUT_MODEL = """
key id int;
key genus string;
property id.species string;
property genus.crown float;
{decl} cls <- case when species = 'u' then 'all' else 'Ang' end;

key taxon string;
property taxon.tv float;
def class_row(value, c) -> max(value ? taxon = c) by *;
auto fallback <- case
    when cls = 'Ang' then @class_row(tv, 'Ang')
    when cls = 'all' then @class_row(tv, 'all')
    else null
end;
auto crown2 <- coalesce(crown, fallback);

root datasource trees (id: id, species: species, genus: genus) grain (id) address trees;
root datasource crown_fits (genus: ~genus, crown: crown) grain (genus) address crown_fits;
root datasource fallbacks (taxon: taxon, tv: tv) grain (taxon) address fallbacks;
"""

FANOUT_SETUP = [
    (
        "CREATE TABLE trees AS SELECT * FROM (VALUES (1, 'sa', 'A'), (2, 'sb', 'B'),"
        " (3, 'u', 'U')) t(id, species, genus)"
    ),
    (
        "CREATE TABLE crown_fits AS SELECT * FROM (VALUES ('A', 1.0), ('B', 2.0))"
        " t(genus, crown)"
    ),
    (
        "CREATE TABLE fallbacks AS SELECT * FROM (VALUES ('Ang', 100.0), ('all', 300.0))"
        " t(taxon, tv)"
    ),
]


@pytest.mark.parametrize("decl", ["auto", "key"])
def test_value_by_derived_key_and_key_joins_back_on_both(decl: str):
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    for sql in FANOUT_SETUP:
        ex.execute_raw_sql(sql)
    ex.parse_text(FANOUT_MODEL.format(decl=decl))
    rows = ex.execute_text("select id, crown2 order by id asc;")[-1].fetchall()
    assert [(i, float(v)) for i, v in rows] == [(1, 1.0), (2, 2.0), (3, 300.0)]


LEFT_CHAIN_MODEL = """
key id int;
key species string;
key genus string;
property genus.level string;
property genus.crown float;
property genus.age float;
root datasource trees (id: id, species: ?species) grain (id) address trees;
root datasource enrichment (species: ~species, genus: ?genus) grain (species) address enrichment;
root datasource crown_fits (level: level, genus: genus, crown: crown) grain (genus)
    address crown_fits where level = 'genus';
root datasource age_fits (genus: genus, age: ?age) grain (genus) address age_fits;
"""

LEFT_CHAIN_SETUP = [
    (
        "CREATE TABLE trees AS SELECT * FROM (VALUES (1, 'sa'), (2, 'sb'), (3, 'sc'))"
        " t(id, species)"
    ),
    (
        "CREATE TABLE enrichment AS SELECT * FROM (VALUES ('sa', 'A'), ('sb', 'B'),"
        " ('sc', 'C')) t(species, genus)"
    ),
    (
        "CREATE TABLE crown_fits AS SELECT * FROM (VALUES ('genus', 'A', 1.0),"
        " ('genus', 'B', 2.0)) t(level, genus, crown)"
    ),
    "CREATE TABLE age_fits AS SELECT * FROM (VALUES ('A', 10.0)) t(genus, age)",
]


@pytest.mark.parametrize(
    "query, expected",
    [
        ("select id, genus order by id asc;", [("1", "A"), ("2", "B"), ("3", "C")]),
        (
            "select id, genus, age order by id asc;",
            [("1", "A", "10.0"), ("2", "B", None), ("3", "C", None)],
        ),
        (
            "select id, genus, crown, age order by id asc;",
            [
                ("1", "A", "1.0", "10.0"),
                ("2", "B", "2.0", None),
                ("3", "C", None, None),
            ],
        ),
    ],
)
def test_key_through_left_join_chain_reads_earliest_side(query: str, expected):
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    for sql in LEFT_CHAIN_SETUP:
        ex.execute_raw_sql(sql)
    ex.parse_text(LEFT_CHAIN_MODEL)
    rows = ex.execute_text(query)[-1].fetchall()
    assert [tuple(None if v is None else str(v) for v in r) for r in rows] == expected


STORED_MODEL = """
key id int;
key g string;
property id.x float;
property id.y float;
auto mean_x <- avg(x) by g;
auto mean_y <- avg(y) by g;
auto slope <- (avg(x * y) by g - mean_x * mean_y) / (avg(x * x) by g - mean_x ** 2);
auto resid <- y - mean_y - slope * (x - mean_x);
root datasource raw (id: id, g: g, x: x, y: y) grain (id) address raw;
datasource fits (g: g, mean_x: mean_x, mean_y: mean_y, slope: slope) grain (g) address fits;
"""

STORED_RAW = (
    "CREATE TABLE raw AS SELECT i AS id, 'g' || (i % 3) AS g, (i % 17)::DOUBLE AS x,"
    " 2.0 * (i % 17) + (i % 5) AS y FROM range(300) t(i)"
)


def _reads(sql: str) -> set[str]:
    return set(re.findall(r'(?:FROM|JOIN)\s+"(raw|fits)"', sql))


@pytest.mark.parametrize(
    "query",
    [
        "select id, slope order by id asc;",
        "select id, g, slope order by id asc;",
        "select id, resid order by id asc;",
        "select g, count(id ? abs(resid) < 1) as kept order by g asc;",
    ],
)
def test_stored_coarser_aggregate_is_joined_not_recomputed(query: str):
    ex = Dialects.DUCK_DB.default_executor(environment=Environment())
    ex.execute_raw_sql(STORED_RAW)
    ex.parse_text(STORED_MODEL)
    ex.update_datasource(ex.environment.datasources["fits"])
    sql = ex.generate_sql(query)[-1]
    assert _reads(sql) == {"raw", "fits"}
    assert "avg(" not in sql
    stored = ex.execute_text(query)[-1].fetchall()
    with hidden_datasources(ex.environment, ["fits"]):
        recomputed = ex.execute_text(query)[-1].fetchall()
    assert len(stored) == len(recomputed)
    for left, right in zip(stored, recomputed):
        assert left[0] == right[0]
        assert left[-1] == pytest.approx(right[-1])
