"""`formal/Trilogy/SelectGrain.lean` (`value_over_group_holds_no_finer_key`,
`keying_bridge_joined_whatever_the_islands`, `null_key_group_is_a_group`): a
third family of random selects against the raw-row oracle of
`test_oracle_sweep_laws.py`, in the expressions the first two never write:
arithmetic over aggregates at one or two grains (`sum(height) / count(tree_id)
by *`), an aggregate of a row expression (`sum(height * 2)`), a property or a
derived value as a group key (`tag`, `height > 2 as big`), and a totally
ordered `order by ... limit`.

Semantics as in the second family, plus: arithmetic over a NULL is NULL, `/`
is exact division, and a NULL key value is a group of its own."""

import random
from collections import defaultdict

import pytest

from tests.discovery.test_oracle_spellings_laws import (
    OPS,
    closure2,
    compare,
    cond_eval,
    cond_text,
    condition,
)
from tests.discovery.test_oracle_sweep_laws import (
    ARG,
    GATE_AGGS,
    GATE_GRAINS,
    KEYSETS,
    REJECTED,
    ROWS,
    THRESHOLDS,
    aggregate,
    executor,
    group_values,
    lit,
    normalize,
)

DERIVED = {
    "fc": (
        "case when tree_form = 'broadleaf' then 'b' else 'o' end",
        lambda r: "b" if r["tree_form"] == "broadleaf" else "o",
    ),
    "form0": (
        "coalesce(tree_form, 'none')",
        lambda r: r["tree_form"] if r["tree_form"] is not None else "none",
    ),
    "big": (
        "height > 2",
        lambda r: None if r["height"] is None else r["height"] > 2,
    ),
}
ROWS3 = [{**r, **{k: f(r) for k, (_, f) in DERIVED.items()}} for r in ROWS]
KEYSETS3 = KEYSETS + [
    ["tree_id"],
    ["tag"],
    ["tall"],
    ["city", "tag"],
    ["species", "tag"],
    ["fc"],
    ["form0"],
    ["city", "fc"],
    ["big"],
    ["city", "big"],
]
EXPRS = {
    "height + 1": lambda r: None if r["height"] is None else r["height"] + 1,
    "height * 2": lambda r: None if r["height"] is None else r["height"] * 2,
    "coalesce(height, 0)": lambda r: r["height"] or 0,
    "case when tag = 'x' then height else 0 end": lambda r: (
        r["height"] if r["tag"] == "x" else 0
    ),
}
ARITH = ["+", "-", "*"]
EXACT = ["count", "sum", "max", "min", "count_distinct"]


def key_text(k):
    return f"{DERIVED[k][0]} as {k}" if k in DERIVED else k


def target_text(t):
    k = t[0]
    if k == "agg":
        return f"{t[1]}({ARG[t[1]]})"
    if k == "expr":
        return f"{t[1]}({t[2]})"
    if k == "by":
        return f"{t[1]}({ARG[t[1]]}) by {', '.join(t[2]) or '*'}"
    if k == "lit":
        return str(t[1])
    if k == "ratio":
        return f"{target_text(t[1])} / {target_text(t[2])}"
    return f"{target_text(t[2])} {t[1]} {target_text(t[3])}"


def target_eval(t, group, kept, row):
    k = t[0]
    if k == "agg":
        return aggregate(t[1], [r[ARG[t[1]]] for r in group])
    if k == "expr":
        return aggregate(t[1], [EXPRS[t[2]](r) for r in group])
    if k == "by":
        return group_values(kept, t[1], t[2])[tuple(row[x] for x in t[2])]
    if k == "lit":
        return t[1]
    a, b = target_eval(t[-2], group, kept, row), target_eval(t[-1], group, kept, row)
    if a is None or b is None:
        return None
    if k == "ratio":
        return a / b
    return {"+": a + b, "-": a - b, "*": a * b}[t[1]]


def grains(keys):
    return [g for g in GATE_GRAINS if set(g) <= closure2(keys)]


def operand(rng, keys):
    roll = rng.random()
    if roll < 0.45:
        return ("agg", rng.choice(EXACT))
    if roll < 0.6:
        return ("expr", rng.choice(["sum", "max", "min"]), rng.choice(list(EXPRS)))
    if roll < 0.9:
        return ("by", rng.choice(EXACT), rng.choice(grains(keys)))
    return ("lit", rng.choice([1, 2, 10]))


def target(rng, keys):
    roll = rng.random()
    if roll < 0.2:
        return ("agg", rng.choice(list(ARG)))
    if roll < 0.4:
        return (
            "expr",
            rng.choice(["sum", "max", "min", "avg"]),
            rng.choice(list(EXPRS)),
        )
    if roll < 0.5:
        return ("by", rng.choice(GATE_AGGS), rng.choice(grains(keys)))
    # a count of a kept group's own rows is never zero
    if roll < 0.65:
        return ("ratio", operand(rng, keys), ("by", "count", rng.choice(grains(keys))))
    if roll < 0.75:
        return ("ratio", operand(rng, keys), ("agg", "count"))
    return ("arith", rng.choice(ARITH), operand(rng, keys), operand(rng, keys))


def ordered(rows, order, limit):
    for idx, descending, nulls_first in reversed(order):
        present = sorted(
            (r for r in rows if r[idx] is not None),
            key=lambda r: r[idx],
            reverse=descending,
        )
        absent = [r for r in rows if r[idx] is None]
        rows = absent + present if nulls_first else present + absent
    return rows[:limit]


def oracle(keys, targets, where, hv, order, limit):
    kept = [r for r in ROWS3 if where is None or cond_eval(where, r, ROWS3) is True]
    groups = defaultdict(list)
    for r in kept:
        groups[tuple(r[k] for k in keys)].append(r)
    out = []
    for key, group in groups.items():
        values = [target_eval(t, group, kept, group[0]) for t in targets]
        if hv and compare(hv[1], values[hv[0]], hv[2]) is not True:
            continue
        out.append(key + tuple(values))
    return ordered(out, order, limit) if order else out


def shape3(seed):
    rng = random.Random(seed)
    keys = rng.choice(KEYSETS3)
    n = rng.choice([1, 1, 2, 3])
    targets = [target(rng, keys) for _ in range(n)]
    where = condition(rng, rng.choice([0, 1, 2])) if rng.random() < 0.7 else None
    numeric = [
        i
        for i, t in enumerate(targets)
        if not (t[0] == "agg" and t[1].startswith("bool"))
    ]
    hv = (
        (rng.choice(numeric), rng.choice(OPS), rng.choice(THRESHOLDS))
        if numeric and rng.random() < 0.25
        else None
    )
    names = list(keys) + [f"v{i}" for i in range(n)]
    order = limit = None
    # every output is ordered on, so the rows a limit keeps are decided
    if rng.random() < 0.4:
        columns = list(range(len(names)))
        rng.shuffle(columns)
        order = [(i, rng.random() < 0.5, rng.random() < 0.5) for i in columns]
        limit = rng.choice([1, 2, 3])
    cols = [key_text(k) for k in keys] + [
        f"{target_text(t)} as v{i}" for i, t in enumerate(targets)
    ]
    q = "select " + ", ".join(cols)
    if where:
        q += " where " + cond_text(where, top=seed % 2 == 1)
    if hv:
        q += f" having v{hv[0]} {hv[1]} {lit(hv[2])}"
    if order:
        q += " order by " + ", ".join(
            f"{names[i]} {'desc' if d else 'asc'} nulls {'first' if nf else 'last'}"
            for i, d, nf in order
        )
        q += f" limit {limit}"
    return q + ";", oracle(keys, targets, where, hv, order, limit)


SEEDS = 400
# seeds past `SEEDS` that 0.3.511 fixed (each failed on main)
FOUND_SEEDS_511 = [
    1037, 1086, 1236, 1242, 1275, 1371, 1517, 1593, 1748, 1806, 1845, 1991,
    2039, 2097, 2214, 2245, 2293, 2336, 2341, 2362, 2449, 2506, 2664,
    2696, 2711, 2724, 2915, 2948, 3083, 3094, 3169, 3224, 3259, 3282, 3351,
    3382, 3516, 3787, 3913, 3980, 4010, 4104, 4140, 4195, 4212, 4248, 4317,
    4340, 4362, 4365, 4376, 4419, 4504, 4507, 4576, 4610, 4738, 4788, 4933,
    4996, 5161, 5340, 5512, 5608, 5624, 5655, 5708, 5777, 5911, 5921,
]  # fmt: skip
# seeds a first version of the round broke: a side grained by another's value
# (2207); a keying bridge joined under an aggregate (6388, 11891); a group at
# its rows' own grain read as a group (8482, 12370, 13591, 15070, 16217); a
# HAVING on a value over a re-summed partial (4274, open on main; 8040, 13140)
BROKEN_SEEDS_511 = [
    2207, 4274, 6388, 8040, 8482, 11891, 12370, 13140, 13591, 15070, 16217,
]  # fmt: skip
# seeds 0.3.512 fixed (each failed on main): a bare aggregate beside a `by`
# aggregate in one expression keyed only by the `by` (2171, 4244); a WHERE atom
# placed on the re-sum of a partial it already filtered (3102, 4180, 5715, 5780,
# 48214); a bridge or a derived key's lookup off rows the WHERE never filtered
# (33164, 9445, 58034); a `by *` total of a table the WHERE's table joins (30258,
# 31752, 33606); two bare aggregates keyed by an input's key (43598)
FOUND_SEEDS_512 = [
    2171, 3102, 4180, 4244, 5715, 5780, 9445, 30258, 31752, 33164, 33606,
    43598, 48214, 58034,
]  # fmt: skip
# leads, failing on main too (docs/handoff_formalization_next.md, area 1):
# what the planner raises or DuckDB rejects, or None for wrong rows
KEYLESS = "keyless join"
OPEN: dict[tuple[int, str], str | None] = {
    # a gate FINAL re-applies beside a value whose side dropped its keys
    (38531, "base"): None,
    (38531, "summary"): None,
    **{(s, m): KEYLESS for s in (15826, 47126) for m in ("base", "summary")},
    (17715, "summary"): KEYLESS,
}


def cases():
    seeds = dict.fromkeys(
        (
            *range(SEEDS),
            *FOUND_SEEDS_511,
            *BROKEN_SEEDS_511,
            *FOUND_SEEDS_512,
            *(s for s, _ in OPEN),
        )
    )
    for seed in seeds:
        for model in ("base", "summary"):
            marks = [pytest.mark.xfail(strict=True)] if (seed, model) in OPEN else []
            yield pytest.param(seed, model, marks=marks)


@pytest.mark.parametrize("seed, model", list(cases()))
def test_expression_matches_raw_row_oracle(seed: int, model: str):
    query, expected = shape3(seed)
    try:
        sql = executor(model).generate_sql(query)[-1]
        rows = executor(model).execute_raw_sql(sql).fetchall()
    except Exception as e:
        if REJECTED in str(e):
            return
        # an open lead fails the way it is recorded to, or it is a new bug
        if (reason := OPEN.get((seed, model))) is not None and reason not in str(e):
            pytest.fail(f"{query}\n{e}", pytrace=False)
        raise
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
