"""`formal/Trilogy/SelectGrain.lean` (`split_keys_pair_unkept`,
`keys_off_two_groups_pair_unheld`, `conjunct_read_past_its_host_passes_absent`,
`by_value_is_no_grain_key`) and `formal/Trilogy/Rollup.lean`
(`gate_partial_resummed_off_its_grain`): a second family of random selects
against the raw-row oracle of `test_oracle_sweep_laws.py`, in spellings the
first family never writes: keys-only selects, row-grain selects (`tree_id`),
`>=`/`<=`/`!=`, a gate `is null`, two gates compared or subtracted, `between`,
`coalesce`, a gate inside `agg(x ? c)`, and a compound HAVING.

Semantics as in the first family, plus: an aggregate inside a select target's
`?` condition is select-phase, so it reads the rows the WHERE kept."""

import random
from collections import defaultdict

import pytest

from tests.discovery.test_oracle_sweep_laws import (
    ARG,
    GATE_AGGS,
    GATE_GRAINS,
    KEYSETS,
    REJECTED,
    ROW_ATOMS,
    ROWS,
    THRESHOLDS,
    aggregate,
    both,
    closure,
    executor,
    group_values,
    lit,
    normalize,
)

KEYSETS2 = KEYSETS + [["tree_id"], ["tree_id", "species"]]
OPS = [">", "<", "=", ">=", "<=", "!="]


def closure2(cols):
    return (
        set(ARG.values()) | {"city", "species", "tree_form", "tree_id"}
        if "tree_id" in cols
        else closure(cols)
    )


def compare(op, a, b):
    if a is None or b is None:
        return None
    return {
        "=": a == b,
        "!=": a != b,
        ">": a > b,
        "<": a < b,
        ">=": a >= b,
        "<=": a <= b,
    }[op]


def gtext(agg, grain):
    return f"{agg}({ARG[agg]}) by {', '.join(grain) or '*'}"


def gval(agg, grain, row, scope):
    return group_values(scope, agg, grain).get(tuple(row[x] for x in grain))


def cond_text(c, top=False):
    k = c[0]
    if k == "row":
        return f"{c[1]} {c[2]} {lit(c[3])}"
    if k == "null":
        return f"{c[1]} is {'' if c[2] else 'not '}null"
    if k == "in":
        return f"{c[1]} in ({', '.join(lit(v) for v in c[2])})"
    if k == "between":
        return f"height between {c[1]} and {c[2]}"
    if k == "coalesce":
        return f"coalesce(tree_form, 'none') = {lit(c[1])}"
    if k == "gate":
        return f"{gtext(c[1], c[2])} {c[3]} {lit(c[4])}"
    if k == "gate_null":
        return f"{gtext(c[1], c[2])} is {'' if c[3] else 'not '}null"
    if k == "gate2":
        return f"{gtext(c[1], c[2])} {c[5]} {gtext(c[3], c[4])}"
    if k == "gate_diff":
        return f"{gtext(c[1], c[2])} - {gtext(c[3], c[4])} {c[5]} {c[6]}"
    if k == "not":
        return f"not ({cond_text(c[1])})"
    text = f"{cond_text(c[1])} {k} {cond_text(c[2])}"
    return text if top else f"({text})"


def cond_eval(c, row, scope):
    k = c[0]
    if k == "row":
        return compare(c[2], row[c[1]], c[3])
    if k == "null":
        return (row[c[1]] is None) == c[2]
    if k == "in":
        return None if row[c[1]] is None else row[c[1]] in c[2]
    if k == "between":
        h = row["height"]
        return None if h is None else c[1] <= h <= c[2]
    if k == "coalesce":
        return (row["tree_form"] if row["tree_form"] is not None else "none") == c[1]
    if k == "gate":
        return compare(c[3], gval(c[1], c[2], row, scope), c[4])
    if k == "gate_null":
        return (gval(c[1], c[2], row, scope) is None) == c[3]
    if k == "gate2":
        return compare(c[5], gval(c[1], c[2], row, scope), gval(c[3], c[4], row, scope))
    if k == "gate_diff":
        a, b = gval(c[1], c[2], row, scope), gval(c[3], c[4], row, scope)
        return None if a is None or b is None else compare(c[5], a - b, c[6])
    if k == "not":
        return cond_eval(c[1], row, scope) is not True
    return both(k, cond_eval(c[1], row, scope), cond_eval(c[2], row, scope))


NUMERIC = ["count", "sum", "max", "min", "count_distinct"]
ROW_ATOMS2 = ROW_ATOMS + [
    ("between", 2, 5),
    ("between", 3, 7),
    ("coalesce", "none"),
    ("coalesce", "broadleaf"),
]


def gate(rng):
    roll = rng.random()
    agg, grain = rng.choice(GATE_AGGS), rng.choice(GATE_GRAINS)
    if roll < 0.15:
        return (
            "gate_null",
            rng.choice(["sum", "max", "min", "avg"]),
            grain,
            rng.random() < 0.5,
        )
    if roll < 0.35:
        return (
            "gate2",
            rng.choice(NUMERIC),
            grain,
            rng.choice(NUMERIC),
            rng.choice(GATE_GRAINS),
            rng.choice(OPS),
        )
    if roll < 0.5:
        return (
            "gate_diff",
            rng.choice(NUMERIC),
            grain,
            rng.choice(NUMERIC),
            rng.choice(GATE_GRAINS),
            rng.choice([">", "<", "="]),
            rng.choice([0, 1, 2, 3]),
        )
    return ("gate", agg, grain, rng.choice(OPS), rng.choice(THRESHOLDS))


def condition(rng, depth, row_only=False):
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
        return rng.choice(ROW_ATOMS2)
    return gate(rng)


def target_text(t):
    k, agg = t[0], t[1]
    if k == "col":
        return agg
    if k == "agg":
        return f"{agg}({ARG[agg]})"
    if k == "by":
        return gtext(agg, t[2])
    if k == "nested":
        return f"{agg}({t[2]}({ARG[t[2]]}) by {', '.join(t[3])})"
    return f"{agg}({ARG[agg]} ? {cond_text(t[2])})"


def target_eval(t, group, kept, row):
    k, agg = t[0], t[1]
    if k == "col":
        return row[agg]
    if k == "agg":
        return aggregate(agg, [r[ARG[agg]] for r in group])
    if k == "by":
        return group_values(kept, agg, t[2])[tuple(row[x] for x in t[2])]
    if k == "nested":
        return aggregate(agg, list(group_values(group, t[2], t[3]).values()))
    return aggregate(
        agg, [r[ARG[agg]] for r in group if cond_eval(t[2], r, kept) is True]
    )


def target(rng, keys):
    roll = rng.random()
    if "tree_id" in keys and roll < 0.4:
        return ("col", rng.choice(["height", "tag", "tall", "city"]))
    if roll < 0.4:
        return ("agg", rng.choice(list(ARG)))
    if roll < 0.6:
        grains = [g for g in GATE_GRAINS if set(g) <= closure2(keys)]
        return ("by", rng.choice(GATE_AGGS), rng.choice(grains))
    if roll < 0.75 and "tree_id" not in keys:
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
        condition(rng, 1, row_only=rng.random() < 0.5),
    )


def having(rng, keys, targets, depth=2):
    numeric = [
        i
        for i, t in enumerate(targets)
        if t[0] in ("agg", "by", "nested", "filtered") and t[1] in NUMERIC + ["avg"]
    ]
    roll = rng.random()
    key_atoms = [a for a in ROW_ATOMS if a[1] in keys]
    if roll < 0.2 and key_atoms:
        return rng.choice(key_atoms)
    if roll < 0.45 and numeric:
        return ("out", rng.choice(numeric), rng.choice(OPS), rng.choice(THRESHOLDS))
    if roll < 0.65 and depth:
        return (
            rng.choice(["and", "or"]),
            having(rng, keys, targets, depth - 1),
            having(rng, keys, targets, depth - 1),
        )
    grains = [g for g in GATE_GRAINS if set(g) <= closure2(keys)]
    return (
        "gate",
        rng.choice(GATE_AGGS),
        rng.choice(grains),
        rng.choice(OPS),
        rng.choice(THRESHOLDS),
    )


def having_text(h, top=True):
    if h[0] == "out":
        return f"v{h[1]} {h[2]} {lit(h[3])}"
    if h[0] in ("and", "or"):
        text = f"{having_text(h[1], False)} {h[0]} {having_text(h[2], False)}"
        return text if top else f"({text})"
    return cond_text(h)


def having_eval(h, values, row, kept):
    if h[0] == "out":
        return compare(h[2], values[h[1]], h[3])
    if h[0] in ("and", "or"):
        return both(
            h[0],
            having_eval(h[1], values, row, kept),
            having_eval(h[2], values, row, kept),
        )
    return cond_eval(h, row, kept)


def oracle(keys, targets, where, hv):
    kept = [r for r in ROWS if where is None or cond_eval(where, r, ROWS) is True]
    groups = defaultdict(list)
    for r in kept:
        groups[tuple(r[k] for k in keys)].append(r)
    out = []
    for key, group in groups.items():
        values = [target_eval(t, group, kept, group[0]) for t in targets]
        if hv and having_eval(hv, values, group[0], kept) is not True:
            continue
        out.append(key + tuple(values))
    return out


def shape2(seed):
    rng = random.Random(seed)
    keys = rng.choice(KEYSETS2)
    n = rng.choice([0, 1, 1, 2, 3]) if keys else rng.choice([1, 2, 3])
    targets = [target(rng, keys) for _ in range(n)]
    where = condition(rng, rng.choice([0, 1, 2])) if rng.random() < 0.85 else None
    hv = having(rng, keys, targets) if rng.random() < 0.3 else None
    cols = list(keys) + [f"{target_text(t)} as v{i}" for i, t in enumerate(targets)]
    q = "select " + ", ".join(cols)
    if where:
        q += " where " + cond_text(where, top=seed % 2 == 1)
    if hv:
        q += " having " + having_text(hv)
    return q + ";", oracle(keys, targets, where, hv)


SEEDS = 400
# seeds past `SEEDS` that 0.3.493 fixed (each failed on main)
FOUND_SEEDS = [
    401, 878, 1087, 1091, 1172, 1350, 1390, 1401, 1599, 1606, 1612,
    2472, 2961, 3245, 3389, 3694, 4309, 4623, 4965, 5374, 5975, 6695,
]  # fmt: skip
# seeds past `SEEDS` that 0.3.494 fixed (each failed on 0.3.493)
FOUND_SEEDS_494 = [
    563, 748, 900, 1132, 1184, 1266, 1391, 1438, 1662, 1689, 1836, 1866,
    2049, 2103, 2105, 2174, 2443, 2505, 2634, 2762, 2788, 2946, 3216, 3246,
    3283, 3689, 3709, 3723, 3747, 3865, 3938, 3971, 4114, 4124, 4245, 4263,
    4264, 4969, 5302, 5346, 5406, 5415, 5568, 5699, 5971, 6015, 6154, 6324,
    6587, 6646, 6720, 6812, 6877, 6886, 6962, 7067, 7194, 7542, 7762, 7794,
    7920, 7964, 8433, 8903, 8948, 9009, 9049, 9062, 9114, 9184, 9280, 9576,
    9938,
]  # fmt: skip
# seeds 10,000-20,000 that 0.3.495 fixed (each failed on main)
FOUND_SEEDS_495 = [
    10018, 10108, 10128, 10612, 10980, 10995, 11030, 11410, 11435, 11454,
    11520, 11884, 11977, 12365, 12369, 12518, 12855, 13164, 13655, 13705,
    14274, 14553, 14587, 14677, 14815, 15345, 16075, 16556, 16563, 16604,
    16605, 16691, 16795, 17382, 17432, 17614, 17642, 18126, 18282, 18456,
    18466, 18536, 18600, 18640, 19422, 19463, 19465, 19529, 19582, 19791,
    19809,
]  # fmt: skip
# seeds 20,000-30,000 that 0.3.496 fixed (each failed on main), and four leads
FOUND_SEEDS_496 = [10132, 11682, 19186, 19236, 20686, 21548, 22018, 24647, 25526, 28671]
# leads, failing on main too (docs/handoff_formalization_next.md, area 1)
OPEN = {
    1117: "FINAL renders `tree_form` off a `?` filter's row stream the WHERE "
    "never filtered, and joins the hosting aggregates onto it preserving",
}


def _seed(seed: int):
    if seed in OPEN:
        return pytest.param(
            seed, marks=pytest.mark.xfail(strict=True, reason=OPEN[seed])
        )
    return seed


@pytest.mark.parametrize("model", ["base", "summary"])
@pytest.mark.parametrize(
    "seed",
    [
        _seed(s)
        for s in (
            *range(SEEDS),
            *FOUND_SEEDS,
            *FOUND_SEEDS_494,
            *FOUND_SEEDS_495,
            *FOUND_SEEDS_496,
            *OPEN,
            8920,
        )
    ],
)
def test_spelling_matches_raw_row_oracle(seed: int, model: str):
    query, expected = shape2(seed)
    try:
        sql = executor(model).generate_sql(query)[-1]
    except Exception as e:
        if REJECTED in str(e):
            return
        raise
    rows = executor(model).execute_raw_sql(sql).fetchall()
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
