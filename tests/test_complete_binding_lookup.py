"""A binding without `~` claims its table holds every value of the key, and
the planner trusts it: two complete bindings of one key join INNER.

#855 bound a genus lookup complete while it held fits for only some genera, so
the join dropped the species rows of every other genus and a `case` over their
form took its first branch. `validate` names that binding; declared `~`, every
projection matches an oracle written over the raw rows."""

import itertools
from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects, Environment
from trilogy.core.exceptions import ModelValidationError
from trilogy.executor import Executor

MODEL = """
key tid int;
key species string;
key genus string;
key fit_class string;
property genus.gn int;
property fit_class.cn int;
property species.form string;

datasource trees (tid: tid, species: species) grain (tid) file `./trees.csv`;
root datasource enrichment (species: ~species, genus: ?genus, form: form) grain (species) file `./species.csv`;
root datasource genus_fits (genus: {genus}, gn: gn) grain (genus) file `./genus_fits.csv`;
root datasource class_fits (cls: fit_class, cn: cn) grain (fit_class) file `./class_fits.csv`;

auto cls <- case when form = 'conifer' then 'B' else 'A' end;
auto class_n <- case
    when cls = 'A' then max(cn ? fit_class = 'A') by *
    when cls = 'B' then max(cn ? fit_class = 'B') by *
    else null
end;
auto picked <- case when gn is not null then gn else class_n end;
"""

ORACLE = """
with joined as (
    select t.tid, t.species, s.genus, s.form, g.gn from read_csv('{d}/trees.csv') t
    left join read_csv('{d}/species.csv') s using (species)
    left join read_csv('{d}/genus_fits.csv') g on s.genus = g.genus
), classed as (
    select *, case when form = 'conifer' then 'B' else 'A' end as cls from joined
), looked_up as (
    select *, (select max(cn) from read_csv('{d}/class_fits.csv') f where f.cls = c.cls) as class_n
    from classed c
)
select *, case when gn is not null then gn else class_n end as picked from looked_up
"""

DATA = {
    "issue": {
        "trees": "tid,species\n1,a\n2,b\n3,c\n",
        "species": "species,genus,form\na,G1,broadleaf\nb,G2,conifer\nc,G2,broadleaf\n",
    },
    # a species with no enrichment row, and one whose genus is unknown
    "gaps": {
        "trees": "tid,species\n1,a\n2,b\n3,c\n4,d\n5,e\n6,b\n",
        "species": "species,genus,form\na,G1,broadleaf\nb,G2,conifer\nc,G2,broadleaf\ne,,conifer\n",
    },
}
COLUMNS = ["species", "genus", "form", "gn", "cls", "class_n", "picked"]
PROJECTIONS = [
    ", ".join(combo)
    for size in range(1, len(COLUMNS) + 1)
    for combo in itertools.combinations(COLUMNS, size)
]


def write_data(directory: Path, name: str) -> Path:
    for table, text in DATA[name].items():
        (directory / f"{table}.csv").write_text(text)
    (directory / "genus_fits.csv").write_text("genus,gn\nG1,10\n")
    (directory / "class_fits.csv").write_text("cls,cn\nA,100\nB,200\n")
    return directory


def executor_for(directory: Path, genus: str) -> Executor:
    env = Environment(working_path=directory)
    env.parse(MODEL.format(genus=genus))
    return Dialects.DUCK_DB.default_executor(environment=env)


def ordered(rows: list[tuple]) -> list[tuple]:
    return sorted(rows, key=repr)


@pytest.fixture(scope="module", params=sorted(DATA))
def data_dir(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory):
    return write_data(tmp_path_factory.mktemp(request.param), request.param)


@pytest.fixture(scope="module")
def partial_executor(data_dir: Path) -> Executor:
    return executor_for(data_dir, "~genus")


def test_a_complete_binding_missing_values_fails_validation(data_dir: Path):
    with pytest.raises(
        ModelValidationError, match="bound complete in datasource genus_fits"
    ):
        executor_for(data_dir, "genus").execute_text("validate all;")


def test_the_partial_binding_validates(partial_executor: Executor):
    partial_executor.execute_text("validate all;")


@pytest.mark.parametrize("columns", PROJECTIONS)
def test_a_partial_lookup_beside_a_derived_class_matches_the_rows(
    data_dir: Path, partial_executor: Executor, columns: str
):
    want = duckdb.sql(
        f"select tid, {columns} from ({ORACLE.format(d=data_dir.as_posix())})"
    ).fetchall()
    got = partial_executor.execute_text(f"select tid, {columns};")[-1].fetchall()
    assert ordered(got) == ordered(want)
