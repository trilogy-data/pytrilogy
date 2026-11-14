"""Per-tree predictions where `city` and the fallback class are keys, built the
way a datasource's update statement builds them: a HAVING over hidden per-tree
columns (#977). A step at a coarser grain than the trees it reads either holds
that grain's rows once or carries `tree_id`; it is never joined back on less
than its own grain, and no CTE repeats a tree."""

import math

import pytest

from tests.helpers.planning import plan
from tests.helpers.rows import as_float, executor_for, fetch_rows, widest_cte
from trilogy.core.enums import Derivation
from trilogy.core.processing.v4_helper.group_behaviors import behavior_for
from trilogy.core.processing.v4_helper.group_graph import GroupFacts, _offered_at_grain

# tree_id, city, data_source, species, plant_age, height_m, lat
TREES = [
    ("t1", "bos", "INV", "acer rubrum", 10.0, 5.0, 1.5),
    ("t2", "bos", "PREDICTED_X", "pinus strobus", 20.0, None, 1.5),
    ("t3", "bos", "INV", "acer rubrum", 30.0, None, 2.5),
    ("t4", "bos", "PREDICTED_X", "acer rubrum", 40.0, 7.0, 2.5),
    ("t5", "bos", "INV", "acer rubrum", None, None, None),
    ("t6", "bos", "INV", "acer saccharum", 60.0, None, 3.5),
    ("t7", "bos", "INV", "Unknown", 70.0, None, 3.5),
    ("t8", "sfo", "INV", "pinus strobus", 80.0, None, 3.5),
    ("t9", "bos", "INV", "quercus alba", 500.0, None, 3.5),
    ("t10", "bos", "INV", "Palm", 15.0, None, 3.5),
]
ENRICHMENT = {
    "acer rubrum": ("acer", "broadleaf"),
    "pinus strobus": ("pinus", "conifer"),
    "acer saccharum": ("acer", "broadleaf"),
    "quercus alba": ("quercus", "broadleaf"),
}
# genus: a, b, sigma, xmax, passes
GENUS_FITS = {
    "acer": (0.5, 0.5, 0.1, 2.0, True),
    "quercus": (0.6, 0.6, 0.1, 2.0, False),
}
# class: the class's own a, b, sigma, xmax, passes, then the fit over all trees
CLASS_FITS = {
    "Angiosperm": (0.4, 0.7, 0.2, 2.0, True, 0.3, 0.8, 0.2, 2.5),
    "Gymnosperm": (0.45, 0.75, 0.2, 2.0, False, 0.3, 0.8, 0.2, 2.5),
    "all": (0.3, 0.8, 0.2, 2.5, True, 0.3, 0.8, 0.2, 2.5),
}


def _lit(v) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    return f"'{v}'" if isinstance(v, str) else str(v)


def _values(rows) -> str:
    return ", ".join("(" + ", ".join(_lit(v) for v in row) + ")" for row in rows)


MODEL = f"""
key tree_id string;
key city string;
key species string;
key genus string;
property tree_id.data_source string;
property tree_id.plant_age float;
property tree_id.height_m float;
property tree_id.lat float;
property species.tree_form string;
property genus.genus_a float;
property genus.genus_b float;
property genus.genus_sigma float;
property genus.genus_xmax float;
property genus.genus_passes bool;

datasource trees (tree_id, city, data_source, species, plant_age: ?plant_age,
    height_m: ?height_m, lat: ?lat) grain (tree_id)
query '''select * from (values {_values(TREES)})
t(tree_id, city, data_source, species, plant_age, height_m, lat)''';

datasource enrichment (species: ~species, genus: ?genus, tree_form: ?tree_form)
grain (species)
query '''select * from (values {_values((s, *v) for s, v in ENRICHMENT.items())})
t(species, genus, tree_form)''';

datasource genus_fits (genus, a: ?genus_a, b: ?genus_b, sigma: ?genus_sigma,
    xmax: ?genus_xmax, passes: ?genus_passes) grain (genus)
query '''select * from (values {_values((g, *v) for g, v in GENUS_FITS.items())})
t(genus, a, b, sigma, xmax, passes)''';

key fit_class string;
property fit_class.division_a float;
property fit_class.division_b float;
property fit_class.division_sigma float;
property fit_class.division_xmax float;
property fit_class.division_passes bool;
property fit_class.global_a float;
property fit_class.global_b float;
property fit_class.global_sigma float;
property fit_class.global_xmax float;
datasource class_fits (fit_class, division_a: ?division_a, division_b: ?division_b,
    division_sigma: ?division_sigma, division_xmax: ?division_xmax,
    division_passes: ?division_passes, global_a: ?global_a, global_b: ?global_b,
    global_sigma: ?global_sigma, global_xmax: ?global_xmax) grain (fit_class)
query '''select * from (values {_values((c, *v) for c, v in CLASS_FITS.items())})
t(fit_class, division_a, division_b, division_sigma, division_xmax, division_passes,
global_a, global_b, global_sigma, global_xmax)''';

key fallback_class <- case
    when species = 'Unknown' then 'all'
    when tree_form = 'palm' or species = 'Palm' then 'none'
    when tree_form = 'conifer' then 'Gymnosperm'
    else 'Angiosperm'
end;
def class_row(value, cls) -> max(value ? fit_class = cls) by *;
def for_class(value) -> case
    when fallback_class = 'Angiosperm' then @class_row(value, 'Angiosperm')
    when fallback_class = 'Gymnosperm' then @class_row(value, 'Gymnosperm')
    when fallback_class = 'all' then @class_row(value, 'all')
    else null
end;
def pick_fit(genus_passes, level, genus, division, everything) -> case
    when genus_passes then genus
    when level = 'division' then division
    when level = 'global' then everything
    else null
end;
auto class_passes <- @for_class(division_passes);
auto class_a <- @for_class(division_a);
auto class_b <- @for_class(division_b);
auto class_sigma <- @for_class(division_sigma);
auto class_xmax <- @for_class(division_xmax);
auto all_a <- max(global_a) by *;
auto all_b <- max(global_b) by *;
auto all_sigma <- max(global_sigma) by *;
auto all_xmax <- max(global_xmax) by *;
auto uses_genus <- case when genus is null then false else coalesce(genus_passes, false) end;
auto level <- case
    when fallback_class = 'none' then null
    when fallback_class != 'all' and coalesce(class_passes, false) then 'division'
    else 'global'
end;
auto fit_a <- @pick_fit(uses_genus, level, genus_a, class_a, all_a);
auto fit_b <- @pick_fit(uses_genus, level, genus_b, class_b, all_b);
auto fit_sigma <- @pick_fit(uses_genus, level, genus_sigma, class_sigma, all_sigma);
auto fit_xmax <- @pick_fit(uses_genus, level, genus_xmax, class_xmax, all_xmax);
auto scale <- 10 ** (fit_a + fit_sigma ** 2 / 2);
auto max_age <- 10 ** fit_xmax;
auto clamped_age <- case
    when plant_age is null then null
    else least(greatest(plant_age, 1.0), max_age)
end;
auto predicted <- scale * (clamped_age ** fit_b);
auto model_level <- case
    when plant_age is null then 'none'
    when uses_genus then 'genus'
    when level is null then 'none'
    else level
end;
auto is_imagery <- data_source like 'PREDICTED_%';
auto cell <- coalesce(cast(floor(lat) as bigint), -1);
auto density <- case
    when cell = -1 then null
    else coalesce(count(tree_id ? not is_imagery) by cell, 0) / 0.25
end;
"""

GATE = "tree_id is not null and (not is_imagery or height_m is not null)"
WIDEST_TABLE = len(TREES)


def _class(species: str) -> str:
    form = ENRICHMENT.get(species, (None, None))[1]
    if species == "Unknown":
        return "all"
    if form == "palm" or species == "Palm":
        return "none"
    return "Gymnosperm" if form == "conifer" else "Angiosperm"


def _oracle() -> dict[str, dict]:
    kept = [t for t in TREES if not t[2].startswith("PREDICTED_") or t[5] is not None]
    out = {}
    for tree_id, city, source, species, age, height, lat in kept:
        genus = ENRICHMENT.get(species, (None, None))[0]
        klass = _class(species)
        fit = CLASS_FITS.get(klass)
        uses_genus = bool(GENUS_FITS.get(genus, (None,) * 5)[4])
        level = (
            None
            if klass == "none"
            else "division" if klass != "all" and fit[4] else "global"
        )
        if uses_genus:
            a, b, sigma, xmax = GENUS_FITS[genus][:4]
        elif level == "division":
            a, b, sigma, xmax = fit[:4]
        elif level == "global":
            a, b, sigma, xmax = CLASS_FITS["all"][5:]
        else:
            a = b = sigma = xmax = None
        predicted = (
            None
            if age is None or a is None
            else 10 ** (a + sigma**2 / 2) * min(max(age, 1.0), 10**xmax) ** b
        )
        cell = -1 if lat is None else math.floor(lat)
        counted = sum(
            1
            for t in TREES
            if t[6] is not None
            and math.floor(t[6]) == cell
            and not t[2].startswith("PREDICTED_")
        )
        out[tree_id] = {
            "city": city,
            "genus": genus,
            "height_m": height,
            "predicted": predicted,
            "model_level": (
                "none" if age is None else "genus" if uses_genus else level or "none"
            ),
            "density": None if cell == -1 else counted / 0.25,
        }
    return out


COLUMN_SETS = [
    ["city", "predicted"],
    ["predicted"],
    ["city", "density"],
    ["city", "density", "predicted"],
    ["city", "predicted", "model_level"],
    ["city", "genus", "predicted", "model_level", "density", "height_m"],
]


def _statement(columns: list[str], clause: str) -> str:
    """`where`, or the `having` over hidden columns an update statement writes."""
    hidden = "".join(
        f", --{c}"
        for c in ("is_imagery", "height_m")
        if clause == "having" and c not in columns
    )
    return f"select {', '.join(['tree_id', *columns])}{hidden} {clause} {GATE};"


def _close(got: tuple, want: tuple) -> bool:
    return len(got) == len(want) and all(
        g == w
        or (
            isinstance(g, float)
            and isinstance(w, float)
            and math.isclose(g, w, rel_tol=1e-9)
        )
        for g, w in zip(got, want)
    )


@pytest.fixture(scope="module")
def executor():
    executor = executor_for(MODEL)
    yield executor
    executor.close()


@pytest.mark.parametrize("clause", ["where", "having"])
@pytest.mark.parametrize("columns", COLUMN_SETS)
def test_each_tree_once_at_its_own_values(
    executor, columns: list[str], clause: str
) -> None:
    query = _statement(columns, clause)
    sql = executor.generate_sql(query)[-1]
    rows = sorted(tuple(as_float(v) for v in r) for r in fetch_rows(executor, query))
    expected = sorted(
        tuple(t if c == "tree_id" else row[c] for c in ["tree_id", *columns])
        for t, row in _oracle().items()
    )
    assert len(rows) == len(expected), sql
    assert all(_close(g, w) for g, w in zip(rows, expected)), (rows, expected, sql)
    assert widest_cte(executor, sql)[0] <= WIDEST_TABLE, sql


def test_a_coarser_step_reads_a_where_argument_off_the_rows() -> None:
    _, environment = plan(MODEL, "select tree_id, city, density;")
    fact = GroupFacts(
        primary={"local.density"},
        grain=frozenset({"local.cell"}),
        derivation=Derivation.BASIC,
        behavior=behavior_for(Derivation.BASIC),
    )
    # `is_imagery` is a tree's: carried beside a density by cell it would put
    # a row per tree under each cell
    assert _offered_at_grain({"local.is_imagery", "local.cell"}, fact, environment) == {
        "local.cell"
    }


def test_final_pairs_a_derived_grain_through_its_producer(executor) -> None:
    query = _statement(["city", "density", "predicted"], "where")
    sql = executor.generate_sql(query)[-1]
    final = sql[sql.rindex("\nSELECT") :]
    assert '"cell"' in final.split("FROM")[1], sql
