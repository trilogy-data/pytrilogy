import math
from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects, Environment

TREES = "tid,species,x,y\n1,a,1.0,2.0\n2,a,2.0,4.5\n3,b,3.0,5.0\n4,c,4.0,9.0\n5,b,5.0,8.0\n6,d,6.0,13.0\n7,a,7.0,13.0\n8,e,8.0,3.0\n"
FORMS = "species,form\na,broadleaf\nb,conifer\nd,conifer\ne,palm\n"

MODEL = """
key tid int;
key species string;
property tid.x float;
property tid.y float;
property species.form string;
datasource trees (tid: tid, species: species, x: x, y: y) grain (tid) file `./trees.csv`;
root datasource species_forms (species: ~species, form: form) grain (species) file `./forms.csv`;
auto cls <- case when species = 'c' then 'all' when form = 'palm' then 'none' when form = 'conifer' then 'G' else 'A' end;
auto fit_tree <- case when x >= 1 and cls != 'none' then true else false end;
auto fx <- case when fit_tree then log(x, 10) else null end;
auto fy <- case when fit_tree then log(y, 10) else null end;
auto d_n <- count(fx) by cls;
auto d_b <- ((avg(fx * fy) by cls) - (avg(fx) by cls) * (avg(fy) by cls)) / ((avg(fx * fx) by cls) - (avg(fx) by cls) ** 2);
auto d_a <- (avg(fy) by cls) - d_b * (avg(fx) by cls);
auto d_s <- case when d_n > 2 then sqrt(greatest(d_n / (d_n - 2) * ((avg(fy * fy) by cls) - (avg(fy) by cls) ** 2 - d_b * ((avg(fx * fy) by cls) - (avg(fx) by cls) * (avg(fy) by cls))), 0.0)) else null end;
auto d_pass <- case when d_n >= 2 and d_b >= 0 then true else false end;
auto g_n <- count(fx) by *;
auto g_b <- ((avg(fx * fy) by *) - (avg(fx) by *) * (avg(fy) by *)) / ((avg(fx * fx) by *) - (avg(fx) by *) ** 2);
auto g_a <- (avg(fy) by *) - g_b * (avg(fx) by *);
auto g_s <- case when g_n > 2 then sqrt(greatest(g_n / (g_n - 2) * ((avg(fy * fy) by *) - (avg(fy) by *) ** 2 - g_b * ((avg(fx * fy) by *) - (avg(fx) by *) * (avg(fy) by *))), 0.0)) else null end;
auto lvl <- case when cls = 'none' then null when cls != 'all' and d_pass then 'division' else 'global' end;
auto f_b <- case when lvl = 'division' then d_b when lvl = 'global' then g_b else null end;
auto f_scale <- case when lvl = 'division' then 10 ** (d_a + d_s ** 2 * 2.3 / 2) when lvl = 'global' then 10 ** (g_a + g_s ** 2 * 2.3 / 2) else null end;
auto f_xmax <- case when lvl = 'division' then 10 ** (max(fx) by cls) when lvl = 'global' then 10 ** (max(fx) by *) else null end;
auto age <- least(greatest(x, 1.0), f_xmax);
auto pred <- f_scale * (age ** f_b);
"""

ORACLE = """
with rows as (
    select t.*, s.form from read_csv('{trees}') t left join read_csv('{forms}') s using (species)
), classed as (
    select *, case when species = 'c' then 'all' when form = 'palm' then 'none'
        when form = 'conifer' then 'G' else 'A' end as cls from rows
), gated as (
    select *, case when x >= 1 and cls != 'none' then true else false end as fit_tree from classed
), points as (
    select *, case when fit_tree then log(x) end as fx, case when fit_tree then log(y) end as fy from gated
), moments as (
    select *,
        count(fx) over c as d_n, avg(fx) over c as d_mx, avg(fy) over c as d_my,
        avg(fx * fy) over c as d_mxy, avg(fx * fx) over c as d_mxx, avg(fy * fy) over c as d_myy,
        max(fx) over c as d_xmax,
        count(fx) over () as g_n, avg(fx) over () as g_mx, avg(fy) over () as g_my,
        avg(fx * fy) over () as g_mxy, avg(fx * fx) over () as g_mxx, avg(fy * fy) over () as g_myy,
        max(fx) over () as g_xmax
    from points window c as (partition by cls)
), slopes as (
    select *, (d_mxy - d_mx * d_my) / (d_mxx - d_mx ** 2) as d_b,
        (g_mxy - g_mx * g_my) / (g_mxx - g_mx ** 2) as g_b from moments
), fits as (
    select *, d_my - d_b * d_mx as d_a, g_my - g_b * g_mx as g_a,
        case when d_n > 2 then sqrt(greatest(d_n / (d_n - 2) * (d_myy - d_my ** 2 - d_b * (d_mxy - d_mx * d_my)), 0.0)) end as d_s,
        case when g_n > 2 then sqrt(greatest(g_n / (g_n - 2) * (g_myy - g_my ** 2 - g_b * (g_mxy - g_mx * g_my)), 0.0)) end as g_s,
        case when d_n >= 2 and d_b >= 0 then true else false end as d_pass
    from slopes
), levels as (
    select *, case when cls = 'none' then null when cls != 'all' and d_pass then 'division'
        else 'global' end as lvl from fits
), picked as (
    select *,
        case when lvl = 'division' then d_b when lvl = 'global' then g_b end as f_b,
        case when lvl = 'division' then 10 ** (d_a + d_s ** 2 * 2.3 / 2)
            when lvl = 'global' then 10 ** (g_a + g_s ** 2 * 2.3 / 2) end as f_scale,
        case when lvl = 'division' then 10 ** d_xmax when lvl = 'global' then 10 ** g_xmax end as f_xmax
    from levels
), aged as (
    select *, least(greatest(x, 1.0), f_xmax) as age from picked
)
select *, f_scale * (age ** f_b) as pred from aged
"""

NUMERIC = [
    "d_n",
    "d_b",
    "d_a",
    "d_s",
    "g_b",
    "g_s",
    "f_b",
    "f_scale",
    "f_xmax",
    "age",
    "pred",
]
LABELS = ["cls", "d_pass", "lvl"]


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    (tmp_path / "trees.csv").write_text(TREES)
    (tmp_path / "forms.csv").write_text(FORMS)
    return tmp_path


def oracle(data_dir: Path, value: str) -> list[tuple]:
    sql = ORACLE.format(
        trees=(data_dir / "trees.csv").as_posix(),
        forms=(data_dir / "forms.csv").as_posix(),
    )
    return duckdb.sql(f"select tid, {value} from ({sql}) order by tid").fetchall()


def planned(data_dir: Path, select: str) -> list[tuple]:
    env = Environment(working_path=data_dir)
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return executor.execute_text(f"{select} order by tid asc;")[-1].fetchall()


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


@pytest.mark.parametrize("column", NUMERIC + LABELS)
def test_fit_column_on_the_rows(data_dir: Path, column: str):
    assert rounded(planned(data_dir, f"select tid, {column}")) == rounded(
        oracle(data_dir, column)
    )


@pytest.mark.parametrize("column", NUMERIC)
def test_fit_column_times_a_row_value(data_dir: Path, column: str):
    assert rounded(planned(data_dir, f"select tid, {column} * x -> v")) == rounded(
        oracle(data_dir, f"{column} * x")
    )


def test_the_predictions_select(data_dir: Path):
    columns = "cls, lvl, f_b, f_scale, age, pred"
    assert rounded(planned(data_dir, f"select tid, {columns}")) == rounded(
        oracle(data_dir, columns)
    )
