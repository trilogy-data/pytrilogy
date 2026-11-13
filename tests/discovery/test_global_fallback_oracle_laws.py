"""Per-row picks between `by *` constants of a lookup table, against an oracle
over the raw rows (#953, #964). A `by *` aggregate is a constant of the query:
the rows join its one-row result, and a value it feeds at a coarser grain than
the rows (a class fallback per genus) is computed once per that grain and
joined back on it. Neither ever repeats a row, so no CTE of the plan holds more
rows than the largest table it reads.

Each case draws trees -> species -> genus -> tree class, genus fits for some
genera (`~genus`) and class fits for some classes, then a per-tree or grouped
select over the fallback chain, beside a flag read off a count by city and
species (a species dimension the rows reach the genus through)."""

import random
from collections import defaultdict

import pytest

from tests.helpers.rows import fetch_rows, normalize_numbers, widest_cte
from trilogy import Dialects

SEEDS = 300
CITIES = ["bos", "sfo"]
CLASSES = ["broadleaf", "conifer", "palm"]
TREE_COLUMNS = ["city", "species", "genus", "tree_class", "dbh"]
DERIVED = ["class_a", "class_h", "tree_a", "pred_dbh", "pred_height", "rare"]
GROUP_KEYS = [[], ["city"], ["species"], ["genus"], ["tree_class"], ["city", "genus"]]
# an aggregate reads a measure once per key of its grain
MEASURES = {
    "class_a": "genus",
    "tree_a": "genus",
    "pred_dbh": "tree_id",
    "pred_height": "tree_id",
}
AGGS = ["sum", "max", "min", "count"]

CONCEPTS = """
key tree_id string;
key species string;
key genus string;
property tree_id.city string;
property tree_id.species string;
property tree_id.dbh float;
property species.genus string;
property genus.tree_class string;
property genus.genus_a float;
property genus.genus_h float;
key fit_class string;
property fit_class.fit_a float;
property fit_class.fit_h float;

auto broadleaf_a <- max(fit_a ? fit_class = 'broadleaf') by *;
auto conifer_a <- max(fit_a ? fit_class = 'conifer') by *;
auto broadleaf_h <- max(fit_h ? fit_class = 'broadleaf') by *;
auto conifer_h <- max(fit_h ? fit_class = 'conifer') by *;
auto class_a <- case when tree_class = 'broadleaf' then broadleaf_a
    when tree_class = 'conifer' then conifer_a end;
auto class_h <- case when tree_class = 'broadleaf' then broadleaf_h
    when tree_class = 'conifer' then conifer_h end;
auto tree_a <- coalesce(genus_a, class_a);
auto tree_h <- coalesce(genus_h, class_h);
auto pred_dbh <- dbh * tree_a;
auto pred_height <- dbh * tree_h;
auto species_city_n <- count(tree_id) by city, species;
auto rare <- species_city_n < 2;
"""


def _lit(v) -> str:
    return "null" if v is None else f"'{v}'" if isinstance(v, str) else str(v)


def _values(rows: list[tuple], columns: str) -> str:
    body = ", ".join("(" + ", ".join(_lit(v) for v in row) + ")" for row in rows[1:])
    head = rows[0] if rows[0] is not None else ("_", 0.0, 0.0)
    first = "(" + ", ".join(_lit(v) for v in head) + ")"
    where = " WHERE false" if rows[0] is None else ""
    return (
        f"'''SELECT * FROM (VALUES {', '.join(filter(None, [first, body]))})"
        f" t({columns}){where}'''"
    )


def _or_empty(rows: list[tuple]) -> list:
    """An empty table: a placeholder row its WHERE drops."""
    return rows or [None]


def _covering(rng: random.Random, n: int, pool: list) -> list:
    """`n` picks from `pool`, each member at least once when `n` allows."""
    picks = [pool[i % len(pool)] for i in range(max(n, len(pool)))]
    rng.shuffle(picks)
    return picks


def _draw(rng: random.Random) -> tuple[str, list[dict], int]:
    genera = [f"g{i}" for i in range(rng.randint(1, 3))]
    names = [f"s{i}" for i in range(rng.randint(len(genera), 4))]
    species = dict(zip(names, _covering(rng, len(names), genera)))
    tree_class = {g: rng.choice(CLASSES) for g in genera}
    genus_fits = [
        (g, float(rng.randint(1, 4)), float(rng.randint(1, 4)))
        for g in genera
        if rng.random() < 0.4
    ]
    # a total over no rows is no row (GROUP BY never emits an empty group)
    fitted = [c for c in CLASSES if rng.random() < 0.8] or [rng.choice(CLASSES)]
    class_fits = {
        c: (float(rng.randint(5, 9)), float(rng.randint(5, 9))) for c in fitted
    }
    trees = [
        (f"t{i}", rng.choice(CITIES), sp, float(i + 1))
        for i, sp in enumerate(_covering(rng, rng.randint(1, 9), names))
    ]
    model = CONCEPTS + f"""
datasource trees (tree_id, city, species, dbh) grain (tree_id)
query {_values(trees, "tree_id, city, species, dbh")};
datasource species_info (species, genus) grain (species)
query {_values(list(species.items()), "species, genus")};
datasource genus_info (genus, tree_class) grain (genus)
query {_values(list(tree_class.items()), "genus, tree_class")};
datasource class_fits (fit_class, a: fit_a, h: fit_h) grain (fit_class)
query {_values([(c, *v) for c, v in class_fits.items()], "fit_class, a, h")};
datasource genus_fits (genus: ~genus, a: genus_a, h: genus_h) grain (genus)
query {_values(_or_empty(genus_fits), "genus, a, h")};
"""
    by_genus = {g: (a, h) for g, a, h in genus_fits}
    per_city = defaultdict(int)
    for _, city, sp, _ in trees:
        per_city[(city, sp)] += 1
    rows = []
    for tree_id, city, sp, dbh in trees:
        genus = species[sp]
        klass = tree_class[genus]
        class_fit = class_fits.get(klass) if klass != "palm" else None
        class_a, class_h = class_fit or (None, None)
        genus_a, genus_h = by_genus.get(genus, (None, None))
        tree_a = genus_a if genus_a is not None else class_a
        tree_h = genus_h if genus_h is not None else class_h
        rows.append(
            {
                "tree_id": tree_id,
                "city": city,
                "species": sp,
                "genus": genus,
                "tree_class": klass,
                "dbh": dbh,
                "class_a": class_a,
                "class_h": class_h,
                "tree_a": tree_a,
                "pred_dbh": None if tree_a is None else dbh * tree_a,
                "pred_height": None if tree_h is None else dbh * tree_h,
                "rare": per_city[(city, sp)] < 2,
            }
        )
    widest = max(len(trees), len(species), len(genera), len(class_fits), 1)
    return model, rows, widest


def _aggregate(agg: str, values: list):
    present = [v for v in values if v is not None]
    if agg == "count":
        return len(present)
    return {"sum": sum, "max": max, "min": min}[agg](present) if present else None


def _at_grain(group: list[dict], measure: str) -> list:
    return list({r[MEASURES[measure]]: r[measure] for r in group}.values())


def _query(rng: random.Random, rows: list[dict]) -> tuple[str, list[tuple]]:
    if rng.random() < 0.6:
        columns = ["tree_id"] + rng.sample(TREE_COLUMNS + DERIVED, rng.randint(1, 4))
        if not set(columns) & set(DERIVED):
            columns.append(rng.choice(DERIVED))
        expected = [tuple(r[c] for c in columns) for r in rows]
        return f"select {', '.join(columns)};", expected
    keys = rng.choice(GROUP_KEYS)
    targets = [
        (rng.choice(AGGS), m) for m in rng.sample(sorted(MEASURES), rng.randint(1, 2))
    ]
    groups = defaultdict(list)
    for r in rows:
        groups[tuple(r[k] for k in keys)].append(r)
    expected = [
        key + tuple(_aggregate(agg, _at_grain(group, m)) for agg, m in targets)
        for key, group in groups.items()
    ]
    selected = keys + [f"{agg}({m}) as {agg}_{m}" for agg, m in targets]
    return f"select {', '.join(selected)};", expected


def shape(seed: int) -> tuple[str, str, list[tuple], int]:
    rng = random.Random(seed)
    model, rows, widest = _draw(rng)
    query, expected = _query(rng, rows)
    return model, query, expected, widest


@pytest.mark.parametrize("seed", range(SEEDS))
def test_fallback_select_matches_raw_rows_and_never_fans_out(seed: int):
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
