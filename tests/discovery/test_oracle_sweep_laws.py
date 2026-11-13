"""`formal/Trilogy/SelectGrain.lean` (a select's keys are the keys of the rows
its WHERE keeps; a literal membership is a comparison; a `by` grain the select
grain determines is a value of it) and `formal/Trilogy/Rollup.lean` (the WHERE
filters every scan): random selects against an oracle written over the raw
rows, not against another plan. A plan with the summary in scope and one
without can share a bug; the oracle cannot.

Semantics the oracle follows: a WHERE aggregate reads every row, a select or
HAVING aggregate the rows the WHERE kept; `not` is two-valued; a membership
matches by identity; GROUP BY never emits an empty group, a total included."""

import random
from collections import defaultdict
from functools import cache

import pytest

from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY
from trilogy import Dialects
from trilogy.executor import Executor

FORM = {"Acer": "broadleaf", "Quercus": "broadleaf", "Ulmus": None, "Tilia": "linden"}
ROWS = [
    {
        "tree_id": t,
        "city": c,
        "species": s,
        "height": h,
        "tag": g,
        "tall": b,
        "tree_form": FORM[s],
    }
    for t, c, s, h, g, b in [
        ("a1", "A", "Acer", 3, "x", False),
        ("a2", "A", "Acer", None, "y", None),
        ("a3", "A", "Quercus", 5, "x", True),
        ("a4", "A", "Ulmus", None, "x", None),
        ("b1", "B", "Acer", 7, "y", True),
        ("b2", "B", "Tilia", 2, "x", False),
    ]
]
ARG = {
    "count": "tree_id",
    "count_distinct": "tag",
    "sum": "height",
    "min": "height",
    "max": "height",
    "avg": "height",
    "bool_or": "tall",
    "bool_and": "tall",
}
KEYSETS = [
    [],
    ["city"],
    ["species"],
    ["tree_form"],
    ["city", "species"],
    ["city", "tree_form"],
    ["species", "tree_form"],
    ["city", "species", "tree_form"],
]
GATE_GRAINS = KEYSETS[:6]
AGGS = list(ARG)
GATE_AGGS = ["count", "sum", "max", "min", "avg", "count_distinct"]
ROW_ATOMS = [
    ("row", "city", "=", "A"),
    ("row", "city", "=", "B"),
    ("row", "species", "!=", "Tilia"),
    ("row", "species", "=", "Acer"),
    ("row", "tree_form", "=", "broadleaf"),
    ("row", "tree_form", "=", "linden"),
    ("null", "tree_form", True),
    ("null", "height", False),
    ("row", "tag", "=", "x"),
    ("row", "height", ">", 2),
    ("in", "species", ["Acer", "Ulmus"]),
    ("in", "city", ["B"]),
]
THRESHOLDS = [0, 1, 2, 3, 5, 6, 8, 10]
SEEDS = 600


def closure(cols: list[str]) -> set[str]:
    return set(cols) | ({"tree_form"} if "species" in cols else set())


def lit(v) -> str:
    return f"'{v}'" if isinstance(v, str) else str(v)


def aggregate(agg: str, values: list):
    present = [v for v in values if v is not None]
    if agg == "count":
        return len(present)
    if agg == "count_distinct":
        return len(set(present))
    if not present:
        return None
    return {
        "sum": sum,
        "min": min,
        "max": max,
        "avg": lambda xs: round(sum(xs) / len(xs), 6),
        "bool_or": any,
        "bool_and": all,
    }[agg](present)


def group_values(rows: list[dict], agg: str, grain: list[str]) -> dict:
    groups = defaultdict(list)
    for r in rows:
        groups[tuple(r[c] for c in grain)].append(r[ARG[agg]])
    return {k: aggregate(agg, v) for k, v in groups.items()}


def compare(op: str, a, b):
    if a is None or b is None:
        return None
    return {"=": a == b, "!=": a != b, ">": a > b, "<": a < b}[op]


def both(op: str, a, b):
    if op == "and":
        if a is False or b is False:
            return False
        return None if a is None or b is None else True
    if a is True or b is True:
        return True
    return None if a is None or b is None else False


def cond_text(c, top: bool = False) -> str:
    kind = c[0]
    if kind == "row":
        return f"{c[1]} {c[2]} {lit(c[3])}"
    if kind == "null":
        return f"{c[1]} is {'' if c[2] else 'not '}null"
    if kind == "in":
        return f"{c[1]} in ({', '.join(lit(v) for v in c[2])})"
    if kind in ("gate", "gate_in"):
        by = ", ".join(c[2]) or "*"
        tail = (
            f"{c[3]} {lit(c[4])}"
            if kind == "gate"
            else f"in ({', '.join(lit(v) for v in c[3])})"
        )
        return f"{c[1]}({ARG[c[1]]}) by {by} {tail}"
    if kind == "not":
        return f"not ({cond_text(c[1])})"
    text = f"{cond_text(c[1])} {kind} {cond_text(c[2])}"
    return text if top else f"({text})"


def cond_eval(c, row: dict, scope: list[dict]):
    kind = c[0]
    if kind == "row":
        return compare(c[2], row[c[1]], c[3])
    if kind == "null":
        return (row[c[1]] is None) == c[2]
    if kind == "in":
        return None if row[c[1]] is None else row[c[1]] in c[2]
    if kind in ("gate", "gate_in"):
        value = group_values(scope, c[1], c[2]).get(tuple(row[x] for x in c[2]))
        if kind == "gate":
            return compare(c[3], value, c[4])
        return value in c[3]
    if kind == "not":
        return cond_eval(c[1], row, scope) is not True
    return both(kind, cond_eval(c[1], row, scope), cond_eval(c[2], row, scope))


def target_text(t) -> str:
    kind, agg = t[0], t[1]
    if kind == "agg":
        return f"{agg}({ARG[agg]})"
    if kind == "by":
        return f"{agg}({ARG[agg]}) by {', '.join(t[2]) or '*'}"
    if kind == "nested":
        return f"{agg}({t[2]}({ARG[t[2]]}) by {', '.join(t[3])})"
    return f"{agg}({ARG[agg]} ? {cond_text(t[2])})"


def target_eval(t, group: list[dict], kept: list[dict], row: dict):
    kind, agg = t[0], t[1]
    if kind == "agg":
        return aggregate(agg, [r[ARG[agg]] for r in group])
    if kind == "by":
        return group_values(kept, agg, t[2])[tuple(row[x] for x in t[2])]
    if kind == "nested":
        return aggregate(agg, list(group_values(group, t[2], t[3]).values()))
    return aggregate(
        agg, [r[ARG[agg]] for r in group if cond_eval(t[2], r, ROWS) is True]
    )


def having_text(h) -> str:
    return f"v{h[1]} {h[2]} {lit(h[3])}" if h[0] == "out" else cond_text(h)


def having_eval(h, values: list, row: dict, kept: list[dict]):
    if h[0] == "out":
        return compare(h[2], values[h[1]], h[3])
    return cond_eval(h, row, kept)


def oracle(keys, targets, where, having) -> list[tuple]:
    kept = [r for r in ROWS if where is None or cond_eval(where, r, ROWS) is True]
    groups = defaultdict(list)
    for r in kept:
        groups[tuple(r[k] for k in keys)].append(r)
    out = []
    for key, group in groups.items():
        values = [target_eval(t, group, kept, group[0]) for t in targets]
        if having and having_eval(having, values, group[0], kept) is not True:
            continue
        out.append(key + tuple(values))
    return out


def gate(rng: random.Random):
    agg, grain = rng.choice(GATE_AGGS), rng.choice(GATE_GRAINS)
    if rng.random() < 0.15:
        return ("gate_in", agg, grain, rng.sample([1, 2, 3, 4, 5, 7], 2))
    return ("gate", agg, grain, rng.choice([">", "<", "="]), rng.choice(THRESHOLDS))


def condition(rng: random.Random, depth: int, row_only: bool = False):
    roll = rng.random()
    if depth > 0 and roll < 0.35:
        return (
            rng.choice(["and", "or"]),
            condition(rng, depth - 1, row_only),
            condition(rng, depth - 1, row_only),
        )
    if depth > 0 and roll < 0.42:
        return ("not", condition(rng, depth - 1, row_only))
    if row_only or rng.random() < 0.45:
        return rng.choice(ROW_ATOMS)
    return gate(rng)


def target(rng: random.Random, keys: list[str]):
    roll = rng.random()
    if roll < 0.45:
        return ("agg", rng.choice(AGGS))
    if roll < 0.65:
        grains = [g for g in GATE_GRAINS if set(g) <= closure(keys)]
        return ("by", rng.choice(GATE_AGGS), rng.choice(grains))
    if roll < 0.85:
        grains = [
            g for g in GATE_GRAINS + [KEYSETS[-1]] if g and set(keys) <= closure(g)
        ]
        return (
            "nested",
            rng.choice(["sum", "max", "min", "count"]),
            rng.choice(["count", "sum", "max", "min"]),
            rng.choice(grains),
        )
    return (
        "filtered",
        rng.choice(["count", "sum", "max", "min"]),
        condition(rng, 1, row_only=True),
    )


def having(rng: random.Random, keys: list[str], targets: list):
    numeric = [
        i
        for i, t in enumerate(targets)
        if not (t[0] == "agg" and t[1].startswith("bool"))
    ]
    if numeric and rng.random() < 0.5:
        return (
            "out",
            rng.choice(numeric),
            rng.choice([">", "<"]),
            rng.choice(THRESHOLDS),
        )
    grains = [g for g in GATE_GRAINS if set(g) <= closure(keys)]
    return (
        "gate",
        rng.choice(GATE_AGGS),
        rng.choice(grains),
        rng.choice([">", "<"]),
        rng.choice(THRESHOLDS),
    )


def shape(seed: int) -> tuple[str, list[tuple]]:
    rng = random.Random(seed)
    keys = rng.choice(KEYSETS)
    targets = [target(rng, keys) for _ in range(rng.choice([1, 1, 2]))]
    where = condition(rng, rng.choice([0, 1, 2])) if rng.random() < 0.85 else None
    hv = having(rng, keys, targets) if rng.random() < 0.25 else None
    columns = list(keys) + [f"{target_text(t)} as v{i}" for i, t in enumerate(targets)]
    query = "select " + ", ".join(columns)
    if where:
        query += " where " + cond_text(where, top=seed % 2 == 1)
    if hv:
        query += " having " + having_text(hv)
    return query + ";", oracle(keys, targets, where, hv)


def normalize(rows) -> list[tuple]:
    def fix(v):
        if isinstance(v, bool) or v is None or isinstance(v, str):
            return v
        number = float(v)
        return int(number) if number.is_integer() else round(number, 6)

    return sorted((tuple(fix(v) for v in r) for r in rows), key=repr)


@cache
def executor(model: str) -> Executor:
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(BASE + (SUMMARY if model == "summary" else ""))
    return exec


# `by *` in both the select and the WHERE of a keyless select is rejected by
# design: the select has no grain to tell the two apart.
REJECTED = "is also computed in the SELECT"


# seeds past `SEEDS` that 0.3.492 fixed (each failed on main)
FOUND_SEEDS = [
    2401, 5269, 6850, 7162, 7340, 9309, 10155, 10212, 10414, 10838,
    12056, 12179, 13153, 13548, 13709, 14382, 14551, 18367, 19430, 19682,
]  # fmt: skip


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize("seed", [*range(SEEDS), *FOUND_SEEDS])
def test_select_matches_raw_row_oracle(seed: int, model: str):
    query, expected = shape(seed)
    try:
        sql = executor(model).generate_sql(query)[-1]
    except Exception as e:
        if REJECTED in str(e):
            return
        raise
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
