"""Per-tree predictions off partial lookups with a `by *` class fallback,
against an oracle over the raw rows (#972). Trees reach a genus only through a
partial species enrichment (`~species`, `?genus`), genus fits cover some
genera, and the class a tree falls back to is derived from the species itself
and a species property, so the class value is at species grain while the rows
it is read beside are trees. No CTE may repeat a tree: every CTE holds at most
as many rows as the widest table, under a WHERE on per-tree columns as well
(a WHERE gates a `by *` total on whether it kept a row; it never multiplies
the total's table by the kept rows)."""

import random
from collections import defaultdict

import pytest

from tests.helpers.rows import fetch_rows, normalize_numbers, widest_cte
from trilogy import Dialects

SEEDS = 300
CITIES = ["bos", "sfo"]
FORMS = ["broadleaf", "conifer", None]
CLASSES = ["Angiosperm", "Gymnosperm", "all"]
UNKNOWN = "unknown"
TREE_COLUMNS = ["city", "species", "genus", "dbh", "fallback_class"]
DERIVED = ["class_a", "pred_h", "pred_c", "rare"]
WHERES = {
    "coalesce(is_imagery, false) = false or height_m is not null": lambda r: (
        not r["is_imagery"] or r["height_m"] is not None
    ),
    "dbh > 3": lambda r: r["dbh"] > 3,
    "city = 'bos'": lambda r: r["city"] == "bos",
    "height_m is null": lambda r: r["height_m"] is None,
}
GROUP_KEYS = [[], ["city"], ["species"], ["genus"], ["city", "species"]]
AGGS = ["sum", "max", "count"]

CONCEPTS = """
key tree_id string;
key species string;
key genus string;
property tree_id.city string;
property tree_id.dbh float;
property tree_id.height_m float;
property tree_id.is_imagery bool;
property species.tree_form string;
property genus.height_a float;
property genus.crown_b float;
property genus.crown_level string;
key fit_class string;
property fit_class.fit_a float;
property fit_class.fit_b float;

auto fallback_class <- case
    when species = 'unknown' then 'all'
    when tree_form = 'conifer' then 'Gymnosperm'
    else 'Angiosperm'
end;
def class_row(value, cls) -> max(value ? fit_class = cls) by *;
def for_class(value) -> case
    when fallback_class = 'Angiosperm' then @class_row(value, 'Angiosperm')
    when fallback_class = 'Gymnosperm' then @class_row(value, 'Gymnosperm')
    when fallback_class = 'all' then @class_row(value, 'all')
end;
auto class_a <- @for_class(fit_a);
auto class_b <- @for_class(fit_b);
{predictions}
auto species_city_n <- count(tree_id) by city, species;
auto rare <- species_city_n < 2;
"""

# a species-grain step named between the class and the per-tree prediction,
# or the prediction spelled in one expression
PREDICTIONS = [
    """auto tree_a <- coalesce(height_a, class_a);
auto tree_b <- coalesce(crown_b, class_b);
auto pred_h <- dbh * tree_a;
auto pred_c <- dbh * tree_b;""",
    """auto pred_h <- dbh * coalesce(height_a, class_a);
auto pred_c <- dbh * coalesce(crown_b, class_b);""",
]


def _lit(v) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    return f"'{v}'" if isinstance(v, str) else str(v)


def _values(rows: list[tuple], columns: str, types: str) -> str:
    """A VALUES table; an empty one is a typed placeholder row its WHERE drops."""
    body = rows or [tuple(None for _ in columns.split(","))]
    first = (
        "("
        + ", ".join(f"cast({_lit(v)} as {t})" for v, t in zip(body[0], types.split()))
        + ")"
    )
    rest = ["(" + ", ".join(_lit(v) for v in row) + ")" for row in body[1:]]
    where = "" if rows else " WHERE false"
    return (
        f"'''SELECT * FROM (VALUES {', '.join([first, *rest])}) t({columns}){where}'''"
    )


def _covering(rng: random.Random, n: int, pool: list) -> list:
    picks = [pool[i % len(pool)] for i in range(max(n, len(pool)))]
    rng.shuffle(picks)
    return picks


def _draw(rng: random.Random) -> tuple[str, list[dict], int]:
    genera = [f"g{i}" for i in range(rng.randint(1, 3))]
    names = [f"s{i}" for i in range(rng.randint(1, 4))] + (
        [UNKNOWN] if rng.random() < 0.3 else []
    )
    enriched = {
        sp: (rng.choice(genera + [None]), rng.choice(FORMS))
        for sp in names
        if sp != UNKNOWN and rng.random() < 0.8
    }
    # the enrichment binds every genus (`?genus`); a fit covers some (`~genus`)
    known = sorted({g for g, _ in enriched.values() if g is not None})
    height = {g: float(rng.randint(1, 4)) for g in known if rng.random() < 0.5}
    crown = {g: float(rng.randint(1, 4)) for g in known if rng.random() < 0.5}
    # a row of another level for a genus: the crown table's WHERE drops it
    stray = [("species", g, 9.0) for g in known if g not in crown][:1]
    fitted = [c for c in CLASSES if rng.random() < 0.8] or [rng.choice(CLASSES)]
    class_fits = {
        c: (float(rng.randint(5, 9)), float(rng.randint(5, 9))) for c in fitted
    }
    trees = [
        (
            f"t{i}",
            rng.choice(CITIES),
            sp,
            float(i + 1),
            rng.choice([None, float(rng.randint(1, 9))]),
            rng.choice([None, True, False]),
        )
        for i, sp in enumerate(_covering(rng, rng.randint(1, 9), names))
    ]
    model = CONCEPTS.replace("{predictions}", rng.choice(PREDICTIONS)) + f"""
datasource trees (tree_id, city, species, dbh, height_m: ?height_m,
    is_imagery: ?is_imagery) grain (tree_id)
query {_values(trees, "tree_id, city, species, dbh, height_m, is_imagery", "varchar varchar varchar double double boolean")};
datasource enrichment (species: ~species, genus: ?genus, tree_form: ?tree_form)
grain (species)
query {_values([(s, g, f) for s, (g, f) in enriched.items()], "species, genus, tree_form", "varchar varchar varchar")};
datasource height_fits (taxon: ~genus, a: height_a) grain (genus)
query {_values(list(height.items()), "taxon, a", "varchar double")};
datasource crown_fits (level: crown_level, taxon: ~genus, b: crown_b) grain (genus)
query {_values([("genus", g, b) for g, b in crown.items()] + stray, "level, taxon, b", "varchar varchar double")}
where crown_level = 'genus';
datasource class_fits (taxon: fit_class, a: fit_a, b: fit_b) grain (fit_class)
query {_values([(c, *v) for c, v in class_fits.items()], "taxon, a, b", "varchar double double")};
"""
    rows = []
    for tree_id, city, sp, dbh, height_m, is_imagery in trees:
        genus, form = enriched.get(sp, (None, None))
        klass = (
            "all"
            if sp == UNKNOWN
            else "Gymnosperm" if form == "conifer" else "Angiosperm"
        )
        class_a, class_b = class_fits.get(klass, (None, None))
        tree_a = height.get(genus, class_a)
        tree_b = crown.get(genus, class_b)
        rows.append(
            {
                "tree_id": tree_id,
                "city": city,
                "species": sp,
                "genus": genus,
                "dbh": dbh,
                "height_m": height_m,
                "is_imagery": is_imagery,
                "fallback_class": klass,
                "class_a": class_a,
                "pred_h": None if tree_a is None else dbh * tree_a,
                "pred_c": None if tree_b is None else dbh * tree_b,
            }
        )
    tables = [len(trees), len(enriched), len(height), len(crown) + len(stray)]
    widest = max(*tables, len(class_fits), 1)
    return model, rows, widest


def _with_rare(rows: list[dict]) -> list[dict]:
    """A select's aggregate reads the rows its WHERE kept."""
    per = defaultdict(int)
    for r in rows:
        per[(r["city"], r["species"])] += 1
    return [{**r, "rare": per[(r["city"], r["species"])] < 2} for r in rows]


def _aggregate(agg: str, values: list):
    present = [v for v in values if v is not None]
    if agg == "count":
        return len(present)
    return {"sum": sum, "max": max}[agg](present) if present else None


def _query(rng: random.Random, rows: list[dict]) -> tuple[str, list[tuple]]:
    where = rng.choice([None, *WHERES])
    kept = _with_rare([r for r in rows if where is None or WHERES[where](r)])
    clause = f" where {where}" if where else ""
    if rng.random() < 0.7:
        columns = ["tree_id"] + rng.sample(TREE_COLUMNS + DERIVED, rng.randint(1, 4))
        if not set(columns) & set(DERIVED):
            columns.append(rng.choice(DERIVED))
        expected = [tuple(r[c] for c in columns) for r in kept]
        return f"select {', '.join(columns)}{clause};", expected
    keys = rng.choice(GROUP_KEYS)
    agg, measure = rng.choice(AGGS), rng.choice(["pred_h", "pred_c"])
    groups = defaultdict(list)
    for r in kept:
        groups[tuple(r[k] for k in keys)].append(r[measure])
    expected = [key + (_aggregate(agg, values),) for key, values in groups.items()]
    selected = keys + [f"{agg}({measure}) as v"]
    return f"select {', '.join(selected)}{clause};", expected


def shape(seed: int) -> tuple[str, str, list[tuple], int]:
    rng = random.Random(seed)
    model, rows, widest = _draw(rng)
    query, expected = _query(rng, rows)
    return model, query, expected, widest


@pytest.mark.parametrize("seed", range(SEEDS))
def test_partial_lookup_prediction_matches_raw_rows(seed: int):
    model, query, expected, widest = shape(seed)
    executor = Dialects.DUCK_DB.default_executor()
    try:
        executor.parse_text(model)
        sql = executor.generate_sql(query)[-1]
        assert normalize_numbers(fetch_rows(executor, query)) == normalize_numbers(
            expected
        ), f"{query}\n{sql}"
        rows, name = widest_cte(executor, sql)
        assert rows <= widest, f"{name} holds {rows} rows > {widest}\n{query}\n{sql}"
    finally:
        executor.close()
