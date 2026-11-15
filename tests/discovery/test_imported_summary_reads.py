"""A summary table reads the same whether its model is local or imported: an
imported datasource's graph node is named by its identifier (`m.summary`),
not its bare name."""

from pathlib import Path

import pytest

from trilogy import Dialects, Environment

MODEL = """
key tree_id int;
key city string;
key species string;
property tree_id.dbh int;
property species.bucket string;

datasource trees (tree_id, city, species, dbh)
grain (tree_id)
query '''select * from (values (1, 'A', 'oak', 4), (2, 'A', 'elm', 6),
    (3, 'B', 'oak', 10), (4, 'B', 'fig', 1)) as t(tree_id, city, species, dbh)''';

datasource enrichment (species, bucket)
grain (species)
query '''select * from (values ('oak', 'Native'), ('elm', 'Native'),
    ('fig', 'Introduced')) as t(species, bucket)''';

auto total_dbh <- sum(dbh);
datasource summary (city, species, total_dbh)
grain (city, species)
query '''select * from (values ('A', 'oak', 4), ('A', 'elm', 6), ('B', 'oak', 10),
    ('B', 'fig', 1)) as t(city, species, total_dbh)''';
"""

QUERIES = {
    "where_property": "where {p}bucket = 'Native' select {p}city, {p}total_dbh"
    " order by {p}city asc;",
    "where_key": "where {p}species = 'oak' select {p}city, {p}total_dbh"
    " order by {p}city asc;",
    "filtered": "select {p}city, sum({p}dbh ? {p}bucket = 'Native') -> kept"
    " order by {p}city asc;",
}
EXPECTED = {
    "where_property": [("A", 10), ("B", 10)],
    "where_key": [("A", 4), ("B", 10)],
    "filtered": [("A", 10), ("B", 10)],
}


@pytest.mark.parametrize("imported", [False, True], ids=["local", "imported"])
@pytest.mark.parametrize("query", sorted(QUERIES))
def test_summary_reads_the_same_imported(tmp_path: Path, query: str, imported: bool):
    (tmp_path / "trees.preql").write_text(MODEL)
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.parse_text("import trees as m;" if imported else MODEL)
    sql = executor.generate_sql(QUERIES[query].format(p="m." if imported else ""))[-1]
    assert "tree_id" not in sql, sql
    rows = [tuple(r) for r in executor.execute_raw_sql(sql).fetchall()]
    assert rows == EXPECTED[query]
