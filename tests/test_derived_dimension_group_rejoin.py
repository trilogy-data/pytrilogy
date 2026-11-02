import math
from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects, Environment

TREES = "tid,species,x,y\n1,a,1.0,2.0\n2,a,2.0,4.5\n3,b,3.0,5.0\n4,c,4.0,9.0\n5,b,5.0,8.0\n6,d,6.0,13.0\n7,a,7.0,13.0\n"
FORMS = "species,form\na,broadleaf\nb,conifer\nd,conifer\n"

CLASSES = {
    "form": "case when form = 'conifer' then 'G' else 'A' end",
    "species_and_form": "case when species = 'c' then 'all' when form = 'conifer' then 'G' else 'A' end",
    "species": "case when species = 'c' then 'all' else 'A' end",
}

MODEL = """
key tid int;
key species string;
property tid.x float;
property tid.y float;
property species.form string;
datasource trees (tid: tid, species: species, x: x, y: y) grain (tid) file `./trees.csv`;
root datasource species_forms (species: ~species, form: form) grain (species) file `./forms.csv`;
auto cls <- {cls};
auto n_cls <- count(x) by cls;
auto ay <- avg(y) by cls;
auto ax <- avg(x) by cls;
auto ag <- avg(y) by *;
auto ratio <- ay / ax;
auto b_cls <- ((avg(x * y) by cls) - (avg(x) by cls) * (avg(y) by cls)) / ((avg(x * x) by cls) - (avg(x) by cls) ** 2);
auto a_cls <- (avg(y) by cls) - b_cls * (avg(x) by cls);
auto b_all <- ((avg(x * y) by *) - (avg(x) by *) * (avg(y) by *)) / ((avg(x * x) by *) - (avg(x) by *) ** 2);
auto b_pick <- case when cls != 'all' and n_cls > 2 then b_cls else b_all end;
auto gated <- case when cls = 'A' then ay else 0 end;
auto fallback <- case when cls = 'A' then ay else ag end;
"""

BY_CLS = "over (partition by cls)"
B_CLS = f"(avg(x * y) {BY_CLS} - avg(x) {BY_CLS} * avg(y) {BY_CLS}) / (avg(x * x) {BY_CLS} - avg(x) {BY_CLS} ** 2)"
B_ALL = "(avg(x * y) over () - avg(x) over () * avg(y) over ()) / (avg(x * x) over () - avg(x) over () ** 2)"
ORACLE_COLUMNS = {
    "n_cls": f"count(x) {BY_CLS}",
    "ay": f"avg(y) {BY_CLS}",
    "ratio": f"avg(y) {BY_CLS} / avg(x) {BY_CLS}",
    "b_cls": B_CLS,
    "a_cls": f"avg(y) {BY_CLS} - ({B_CLS}) * avg(x) {BY_CLS}",
    "b_pick": f"case when cls != 'all' and count(x) {BY_CLS} > 2 then {B_CLS} else {B_ALL} end",
    "gated": f"case when cls = 'A' then avg(y) {BY_CLS} else 0 end",
    "fallback": f"case when cls = 'A' then avg(y) {BY_CLS} else avg(y) over () end",
}

EXPRESSIONS = {
    "{c}": "{c}",
    "{c} * x": "{c} * x",
    "{c} + x": "{c} + x",
}


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    (tmp_path / "trees.csv").write_text(TREES)
    (tmp_path / "forms.csv").write_text(FORMS)
    return tmp_path


def oracle(data_dir: Path, cls: str, column: str, expression: str) -> list[tuple]:
    rows = f"""
        select t.*, s.form from read_csv('{(data_dir / 'trees.csv').as_posix()}') t
        left join read_csv('{(data_dir / 'forms.csv').as_posix()}') s using (species)
    """
    classed = f"select *, {cls} as cls from ({rows})"
    value = expression.format(c=f"({ORACLE_COLUMNS[column]})")
    sql = f"select tid, {value} from ({classed}) order by tid"
    return duckdb.sql(sql).fetchall()


def rounded(rows: list[tuple]) -> list[tuple]:
    return [
        tuple(
            (
                None
                if v is None or (isinstance(v, float) and math.isnan(v))
                else round(v, 6) if isinstance(v, float) else v
            )
            for v in row
        )
        for row in rows
    ]


@pytest.mark.parametrize("expression", list(EXPRESSIONS))
@pytest.mark.parametrize("column", list(ORACLE_COLUMNS))
@pytest.mark.parametrize("cls", list(CLASSES))
def test_class_aggregate_joins_back_to_rows(
    data_dir: Path, cls: str, column: str, expression: str
):
    env = Environment(working_path=data_dir)
    env.parse(MODEL.format(cls=CLASSES[cls]))
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    value = expression.format(c=column)
    got = executor.execute_text(f"select tid, {value} -> v order by tid asc;")[-1]
    assert rounded(got.fetchall()) == rounded(
        oracle(data_dir, CLASSES[cls], column, expression)
    )


def test_issue_846_repro(data_dir: Path):
    (data_dir / "trees.csv").write_text(
        "tid,species,x\n1,a,1.0\n2,a,2.0\n3,b,3.0\n4,c,4.0\n"
    )
    (data_dir / "forms.csv").write_text("species,form\na,broadleaf\nb,conifer\n")
    env = Environment(working_path=data_dir)
    env.parse("""
key tid int;
key species string;
property tid.x float;
property species.form string;
datasource trees (tid: tid, species: species, x: x) grain (tid) file `./trees.csv`;
root datasource species_forms (species: ~species, form: form) grain (species) file `./forms.csv`;
auto division <- case when form = 'conifer' then 'Gymnosperm' else 'Angiosperm' end;
auto division_n <- count(x) by division;
""")
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    assert sorted(executor.execute_text("select tid, division_n;")[-1].fetchall()) == [
        (1, 3),
        (2, 3),
        (3, 1),
        (4, 3),
    ]
    assert sorted(
        executor.execute_text("select tid, division, division_n;")[-1].fetchall()
    ) == [
        (1, "Angiosperm", 3),
        (2, "Angiosperm", 3),
        (3, "Gymnosperm", 1),
        (4, "Angiosperm", 3),
    ]
