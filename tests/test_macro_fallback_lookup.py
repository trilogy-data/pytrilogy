"""A `def` macro body is the expression it expands to, nothing more.

Two things a macro call carried that an inline spelling did not, found on
the urban-tree predictions model (greenmtnboy/tree_reporting#167):

- The call wrapper yielded its raw arguments as children beside the bound
  body, so a select pinned a lookup macro (`max(value ? taxon = cls) by *`)
  to the class table's key, and the row it was picked for could not be
  joined: "disconnected subgraphs", "Missing source reference", or a
  group-node cycle, depending on what else the select read.
- A bare parameter in a `when` was spelled `is not null` while the
  parameter was still untyped, so a boolean gate bound at the call was
  true for every non-null row.
"""

import math
from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects, Environment
from trilogy.core.enums import ComparisonOperator, FunctionType
from trilogy.core.having_normalization import _child_exprs
from trilogy.core.models.author import (
    BareCondition,
    CaseWhen,
    ConceptRef,
    Function,
    FunctionCallWrapper,
)

TREES = "tid,species,x\n1,a,1.0\n2,a,2.0\n3,b,3.0\n4,c,4.0\n5,d,5.0\n"
SPECIES = "species,genus,form\na,Ga,broadleaf\nb,Gb,conifer\nd,Gd,broadleaf\n"
GENUS_FITS = "genus,n,a,passes\nGa,3,1.5,true\nGb,4,2.5,false\n"
CLASS_FITS = "level,taxon,b,scale\ndivision,A,0.5,10.0\ndivision,G,0.6,20.0\nglobal,all,0.7,30.0\ngenus,Ga,0.9,99.0\n"

MODEL = """
key tid int;
key species string;
key genus string;
property tid.x float;
property species.form string;
property genus.genus_n int;
property genus.genus_a float;
property genus.genus_passes bool;
key fallback_taxon string;
property fallback_taxon.row_level string;
property fallback_taxon.row_b float;
property fallback_taxon.row_scale float;

datasource trees (tid: tid, species: species, x: x) grain (tid) file `./trees.csv`;
root datasource species_forms (species: ~species, genus: ?genus, form: ?form) grain (species) file `./species.csv`;
root datasource genus_fits (genus: genus, n: genus_n, a: ?genus_a, passes: ?genus_passes) grain (genus) file `./genus_fits.csv`;
root datasource class_fits (level: row_level, taxon: fallback_taxon, b: row_b, scale: row_scale) grain (fallback_taxon) file `./class_fits.csv`
where row_level = 'division' or row_level = 'global';

auto cls <- case when species = 'c' then 'all' when form = 'conifer' then 'G' else 'A' end;

def class_row(value, taxon, c) -> max(value ? taxon = c) by *;
def for_class(value, taxon) -> case
    when cls = 'A' then @class_row(value, taxon, 'A')
    when cls = 'G' then @class_row(value, taxon, 'G')
    when cls = 'all' then @class_row(value, taxon, 'all')
    else null
end;
auto fallback_b <- @for_class(row_b, fallback_taxon);
auto fallback_scale <- @for_class(row_scale, fallback_taxon);

auto uses_genus <- case when genus is null then false else coalesce(genus_passes, false) end;
def pick_fit(genus_passes, level, genus, division, everything) -> case
    when genus_passes then genus
    when level = 'division' then division
    when level = 'global' then everything
    else null
end;
auto level <- case when cls = 'all' then 'global' else 'division' end;
auto picked <- @pick_fit(uses_genus, level, genus_a, fallback_scale, max(row_scale ? fallback_taxon = 'all') by *);
"""

ORACLE = """
with rows as (
    select t.*, s.genus, s.form from read_csv('{trees}') t
    left join read_csv('{species}') s using (species)
), fits as (
    select r.*, g.n as genus_n, g.a as genus_a, g.passes as genus_passes
    from rows r left join read_csv('{genus_fits}') g using (genus)
), classed as (
    select *, case when species = 'c' then 'all' when form = 'conifer' then 'G' else 'A' end as cls from fits
), looked_up as (
    select c.*,
        (select scale from read_csv('{class_fits}') where level in ('division', 'global') and taxon = c.cls) as fallback_scale,
        (select b from read_csv('{class_fits}') where level in ('division', 'global') and taxon = c.cls) as fallback_b,
        (select scale from read_csv('{class_fits}') where taxon = 'all') as all_scale
    from classed c
), picked as (
    select *,
        case when genus is null then false else coalesce(genus_passes, false) end as uses_genus,
        case when cls = 'all' then 'global' else 'division' end as level
    from looked_up
)
select *, case when uses_genus then genus_a when level = 'division' then fallback_scale
    when level = 'global' then all_scale else null end as picked
from picked
"""


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    for name, text in (
        ("trees.csv", TREES),
        ("species.csv", SPECIES),
        ("genus_fits.csv", GENUS_FITS),
        ("class_fits.csv", CLASS_FITS),
    ):
        (tmp_path / name).write_text(text)
    return tmp_path


def oracle(data_dir: Path, columns: str) -> list[tuple]:
    sql = ORACLE.format(
        **{
            k: (data_dir / f"{k}.csv").as_posix()
            for k in ("trees", "species", "genus_fits", "class_fits")
        }
    )
    return duckdb.sql(f"select tid, {columns} from ({sql}) order by tid").fetchall()


def planned(data_dir: Path, columns: str) -> list[tuple]:
    env = Environment(working_path=data_dir)
    env.parse(MODEL)
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    return executor.execute_text(f"select tid, {columns} order by tid asc;")[
        -1
    ].fetchall()


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


@pytest.mark.parametrize(
    "columns",
    [
        "fallback_scale",
        "genus_a, fallback_scale",
        "genus, genus_a, fallback_scale",
        "genus_a, fallback_b, fallback_scale",
        "picked",
        "uses_genus, level, genus_a, fallback_scale, picked",
    ],
)
def test_a_macro_lookup_picked_by_a_derived_class_rejoins_its_row(
    data_dir: Path, columns: str
):
    assert rounded(planned(data_dir, columns)) == rounded(oracle(data_dir, columns))


def test_the_call_wrapper_is_only_its_body():
    """The body already has the arguments substituted; the raw args are not
    rows the expression reads."""
    body = Function(
        operator=FunctionType.ADD,
        output_datatype="int",
        output_purpose="property",
        arguments=[ConceptRef(address="local.x"), 1],
    )
    call = FunctionCallWrapper(
        content=body, name="m", args=[ConceptRef(address="local.x")]
    )
    assert list(_child_exprs(call)) == [body]


ROWS = "tid,flag,n,label\n1,true,0,a\n2,false,3,\n3,,0,c\n"

GATES = """
key tid int;
property tid.flag bool;
property tid.n int;
property tid.label string;
datasource rows (tid: tid, flag: flag, n: n, label: label) grain (tid) file `./rows.csv`;
def gate(cond, yes, no) -> case when cond then yes else no end;
auto by_flag <- @gate(flag, 1, 0);
auto by_n <- @gate(n, 1, 0);
auto by_label <- @gate(label, 1, 0);
auto inline_flag <- case when flag then 1 else 0 end;
"""


def test_a_bare_macro_parameter_is_spelled_once_bound(tmp_path: Path):
    (tmp_path / "rows.csv").write_text(ROWS)
    env = Environment(working_path=tmp_path)
    env.parse(GATES)
    spellings = {}
    for name in ("by_flag", "by_n", "by_label"):
        lineage = env.concepts[name].lineage
        assert isinstance(lineage, FunctionCallWrapper)
        when = lineage.content.arguments[0]
        assert isinstance(when, CaseWhen) and isinstance(when.comparison, BareCondition)
        spellings[name] = (when.comparison.operator, when.comparison.right)
    assert spellings["by_flag"] == (ComparisonOperator.EQ, True)
    assert spellings["by_n"] == (ComparisonOperator.EQ, True)
    assert spellings["by_label"][0] == ComparisonOperator.IS_NOT
    executor = Dialects.DUCK_DB.default_executor(environment=env)
    rows = executor.execute_text(
        "select tid, by_flag, by_n, by_label, inline_flag order by tid asc;"
    )[-1].fetchall()
    assert rows == [(1, 1, 0, 1, 1), (2, 0, 1, 0, 0), (3, 0, 0, 1, 0)]
