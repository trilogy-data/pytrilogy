"""`formal/Trilogy/SelectGrain.lean`, `formal/Trilogy/Windows.lean` and
`formal/Trilogy/ValueSet.lean`: a fourth family of random selects against the
raw-row oracle of `test_oracle_sweep_laws.py`, composing what the first three
write with what none of them do: a membership in a filtered set beside the
WHERE's own atoms (`species in (species ? c)`), `/` by a value that can be 0,
an aggregate `by` a derived key, a nested aggregate inside arithmetic, a rank
over the select's groups, and an `order by` an expression.

Semantics as in the third family, plus: a set's condition reads every row; a
membership matches by identity, so a NULL is in a set that holds NULL; `x / 0`
is NULL; a window ranks the groups the WHERE kept, before HAVING."""

import random
from collections import defaultdict

import pytest

from tests.discovery.test_oracle_expressions_laws import (
    DERIVED,
    EXACT,
    KEYSETS3,
    ROWS3,
    grains,
    key_text,
    ordered,
)
from tests.discovery.test_oracle_expressions_laws import target as target3
from tests.discovery.test_oracle_expressions_laws import target_eval as eval3
from tests.discovery.test_oracle_expressions_laws import target_text as text3
from tests.discovery.test_oracle_spellings_laws import (
    OPS,
    closure2,
    compare,
)
from tests.discovery.test_oracle_spellings_laws import cond_eval as cond_eval2
from tests.discovery.test_oracle_spellings_laws import cond_text as cond_text2
from tests.discovery.test_oracle_spellings_laws import condition as condition2
from tests.discovery.test_oracle_sweep_laws import (
    ARG,
    GATE_GRAINS,
    KEYSETS,
    REJECTED,
    THRESHOLDS,
    aggregate,
    both,
    executor,
    group_values,
    lit,
    normalize,
)

MEMBER_COLUMNS = ["species", "city", "tag", "tree_form", "tree_id"]
# denominators that are 0 on some group
ZERO_DENOMS = {
    "count(tree_id) - 1": lambda g: aggregate("count", [r["tree_id"] for r in g]) - 1,
    "max(height) - min(height)": lambda g: _sub(
        aggregate("max", [r["height"] for r in g]),
        aggregate("min", [r["height"] for r in g]),
    ),
    "sum(height) - 5": lambda g: _sub(aggregate("sum", [r["height"] for r in g]), 5),
    "count_distinct(tag) - 1": lambda g: aggregate(
        "count_distinct", [r["tag"] for r in g]
    )
    - 1,
}
WINDOW_AGGS = ["count", "sum", "max", "min"]


def _sub(a, b):
    return None if a is None or b is None else a - b


def is_composite(c) -> bool:
    return c[0] in ("and", "or", "not", "mem")


def cond_text(c, top: bool = False) -> str:
    k = c[0]
    if k == "mem":
        text = f"{c[1]} in ({c[1]} ? {cond_text(c[2], top=True)})"
        return f"not ({text})" if c[3] else text
    if k == "not":
        return f"not ({cond_text(c[1])})"
    if k in ("and", "or"):
        text = f"{cond_text(c[1])} {k} {cond_text(c[2])}"
        return text if top else f"({text})"
    return cond_text2(c, top)


def member_set(col: str, inner) -> set:
    return {r[col] for r in ROWS3 if cond_eval(inner, r, ROWS3) is True}


def cond_eval(c, row: dict, scope: list[dict]):
    k = c[0]
    if k == "mem":
        hit = row[c[1]] in member_set(c[1], c[2])
        return not hit if c[3] else hit
    if k == "not":
        return cond_eval(c[1], row, scope) is not True
    if k in ("and", "or"):
        return both(k, cond_eval(c[1], row, scope), cond_eval(c[2], row, scope))
    return cond_eval2(c, row, scope)


def membership(rng: random.Random):
    return (
        "mem",
        rng.choice(MEMBER_COLUMNS),
        condition2(rng, rng.choice([0, 0, 1])),
        rng.random() < 0.2,
    )


def condition(rng: random.Random, depth: int):
    roll = rng.random()
    if depth > 0 and roll < 0.4:
        return (
            rng.choice(["and", "or"]),
            condition(rng, depth - 1),
            condition(rng, depth - 1),
        )
    if roll < 0.7:
        return membership(rng)
    return condition2(rng, 0)


def plain(keys: list[str]) -> bool:
    return not any(k in DERIVED for k in keys)


def nested_grains(keys: list[str]) -> list[list[str]]:
    return [
        g
        for g in GATE_GRAINS + [KEYSETS[-1]]
        if g and set(keys) <= closure2(g) and plain(keys)
    ]


def operand(rng: random.Random, keys: list[str]):
    roll = rng.random()
    nested = nested_grains(keys)
    if roll < 0.3 and nested:
        return (
            "nest",
            rng.choice(["sum", "max", "min", "count"]),
            rng.choice(["count", "sum", "max", "min"]),
            rng.choice(nested),
        )
    derived = [k for k in keys if k in DERIVED]
    if roll < 0.55 and derived:
        grain = [k for k in keys if k in derived or rng.random() < 0.5]
        return ("by", rng.choice(EXACT), grain)
    if roll < 0.75:
        return ("agg", rng.choice(EXACT))
    return ("by", rng.choice(EXACT), rng.choice(grains(keys)))


def target(rng: random.Random, keys: list[str]):
    roll = rng.random()
    if roll < 0.2:
        return target3(rng, keys)
    if roll < 0.45:
        return ("zdiv", operand(rng, keys), rng.choice(list(ZERO_DENOMS)))
    if roll < 0.6 and keys and plain(keys):
        k = rng.choice(keys)
        return (
            "rank",
            k,
            [x for x in keys if x != k],
            rng.choice(WINDOW_AGGS),
            rng.random() < 0.5,
            rng.random() < 0.5,
        )
    if roll < 0.8:
        return operand(rng, keys)
    return (
        "arith",
        rng.choice(["+", "-", "*"]),
        operand(rng, keys),
        operand(rng, keys),
    )


def by_text(agg: str, grain: list[str]) -> str:
    return f"{agg}({ARG[agg]}) by {', '.join(grain) or '*'}"


def target_text(t, keys: list[str]) -> str:
    k = t[0]
    if k == "nest":
        return f"{t[1]}({by_text(t[2], t[3])})"
    if k == "zdiv":
        return f"{target_text(t[1], keys)} / ({t[2]})"
    if k == "rank":
        over = f" over {', '.join(t[2])}" if t[2] else ""
        order = "desc" if t[4] else "asc"
        nulls = "first" if t[5] else "last"
        return f"rank {t[1]}{over} order by {by_text(t[3], keys)} {order} nulls {nulls}"
    if k == "arith":
        return f"{target_text(t[2], keys)} {t[1]} {target_text(t[3], keys)}"
    return text3(t)


def target_eval(t, group: list[dict], kept: list[dict], row: dict):
    k = t[0]
    if k == "nest":
        return aggregate(t[1], list(group_values(group, t[2], t[3]).values()))
    if k == "zdiv":
        a, b = target_eval(t[1], group, kept, row), ZERO_DENOMS[t[2]](group)
        return None if a is None or b is None or b == 0 else a / b
    if k == "arith":
        a = target_eval(t[2], group, kept, row)
        b = target_eval(t[3], group, kept, row)
        if a is None or b is None:
            return None
        return {"+": a + b, "-": a - b, "*": a * b}[t[1]]
    return eval3(t, group, kept, row)


def rank(t, keys: list[str], groups: dict) -> dict:
    """Rank each group within its partition: 1 + the groups strictly ahead."""
    _, _, over, agg, desc, nulls_first = t
    value = {
        key: aggregate(agg, [r[ARG[agg]] for r in group])
        for key, group in groups.items()
    }

    def ahead(a, b) -> bool:
        if a is None or b is None:
            return (
                a is None and b is not None
                if nulls_first
                else b is None and a is not None
            )
        return a > b if desc else a < b

    part = [keys.index(x) for x in over]
    out = {}
    for key in groups:
        peers = [o for o in groups if all(o[i] == key[i] for i in part)]
        out[key] = 1 + sum(ahead(value[o], value[key]) for o in peers)
    return out


def oracle(keys, targets, where, hv, order, limit):
    kept = [r for r in ROWS3 if where is None or cond_eval(where, r, ROWS3) is True]
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in kept:
        groups[tuple(r[k] for k in keys)].append(r)
    ranks = {i: rank(t, keys, groups) for i, t in enumerate(targets) if t[0] == "rank"}
    out = []
    for key, group in groups.items():
        values = [
            ranks[i][key] if i in ranks else target_eval(t, group, kept, group[0])
            for i, t in enumerate(targets)
        ]
        if hv and compare(hv[1], values[hv[0]], hv[2]) is not True:
            continue
        out.append(key + tuple(values))
    if not order:
        return out
    expr, columns = order
    rows = [r + (_order_value(expr, r),) for r in out]
    width = len(out[0]) if out else 0
    terms = [(width, expr[2], expr[3])] + columns
    return [r[:-1] for r in ordered(rows, terms, limit)]


def _order_value(expr, row: tuple):
    v = row[expr[0]]
    if expr[1] == "neg":
        return None if v is None else -v
    return v if v is not None else -1


def order_text(expr, names: list[str]) -> str:
    name = names[expr[0]]
    text = f"{name} * -1" if expr[1] == "neg" else f"coalesce({name}, -1)"
    return (
        f"{text} {'desc' if expr[2] else 'asc'} nulls {'first' if expr[3] else 'last'}"
    )


def shape4(seed: int) -> tuple[str, list[tuple]]:
    rng = random.Random(seed)
    keys = rng.choice(KEYSETS3)
    n = rng.choice([1, 1, 2, 3])
    targets = [target(rng, keys) for _ in range(n)]
    where = condition(rng, rng.choice([0, 1, 2])) if rng.random() < 0.8 else None
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
    if numeric and rng.random() < 0.4:
        expr = (
            len(keys) + rng.choice(numeric),
            rng.choice(["neg", "coalesce"]),
            rng.random() < 0.5,
            rng.random() < 0.5,
        )
        columns = list(range(len(names)))
        rng.shuffle(columns)
        order = (expr, [(i, rng.random() < 0.5, rng.random() < 0.5) for i in columns])
        limit = rng.choice([1, 2, 3])
    cols = [key_text(k) for k in keys] + [
        f"{target_text(t, keys)} as v{i}" for i, t in enumerate(targets)
    ]
    q = "select " + ", ".join(cols)
    if where:
        q += " where " + cond_text(where, top=seed % 2 == 1)
    if hv:
        q += f" having v{hv[0]} {hv[1]} {lit(hv[2])}"
    if order:
        expr, columns = order
        q += " order by " + ", ".join(
            [order_text(expr, names)]
            + [
                f"{names[i]} {'desc' if d else 'asc'} nulls {'first' if nf else 'last'}"
                for i, d, nf in columns
            ]
        )
        q += f" limit {limit}"
    return q + ";", oracle(keys, targets, where, hv, order, limit)


SEEDS = 400
# seeds 0-30,000 that failed on 0.3.521 and plan right now
FOUND_SEEDS = [
    337, 728, 829, 876, 1023, 1077, 1105, 1128, 1237, 1255, 1258, 1277, 1481,
    1497, 1683, 1693, 1742, 1875, 2081, 2087, 2234, 2403, 2724, 2885, 2909,
    2940, 3062, 3065, 3094, 3124, 3316, 3558, 3566, 3623, 3736, 3754, 3852,
    3888, 4011, 4048, 4069, 4142, 4160, 4314, 4338, 4394, 4614, 4634, 4660,
    4704, 5094, 5111, 5134, 5207, 5465, 5544, 5625, 5694, 5770, 6450, 6500,
    6502, 6503, 6530, 6533, 6633, 6639, 6645, 6723, 7089, 7176, 7402, 7446,
    7464, 7483, 7670, 7690, 7744, 7879, 7902, 7962, 8084, 8394, 8611, 8684,
    8861, 8994, 9034, 9088, 9202, 9406, 9542, 9559, 9578, 9591, 9660, 9676,
    9747, 9783, 9833, 9887, 10082, 10180, 10362, 10445, 10499, 10546, 10587,
    10843, 10964, 11398, 11556, 11733, 11855, 11860, 12147, 12542, 12558,
    12563, 12776, 12987, 13005, 13273, 13392, 13409, 13609, 13805, 13919,
    13950, 14087, 14093, 14129, 14241, 14337, 14599, 14620, 14865, 14896,
    14982, 15155, 15334, 15490, 15590, 15782, 16041, 16116, 16117, 16284,
    16300, 16464, 16512, 16574, 16584, 16875, 16922, 17184, 17414, 17801,
    17964, 17979, 18091, 18147, 18254, 18394, 18549, 18629, 18731, 18901,
    18936, 18975, 19131, 19372, 19419, 19434, 19556, 19558, 19602, 19818,
    19896, 20016, 20259, 20379, 20405, 20906, 21123, 21230, 21242, 21400,
    21425, 21442, 21459, 21527, 21537, 21595, 21632, 21752, 21761, 22062,
    22071, 22166, 22386, 22486, 22548, 22696, 22889, 23087, 23131, 23229,
    23328, 23506, 23552, 23573, 23668, 23746, 23857, 23901, 23908, 23961,
    23966, 23983, 24086, 24131, 24176, 24253, 24368, 24457, 24658, 24705,
    24751, 25444, 25475, 25481, 25513, 25560, 25784, 25935, 26063, 26115,
    26495, 26715, 26784, 26911, 27067, 27099, 27130, 27235, 27484, 27621,
    27917, 28071, 28139, 28525, 28526, 28552, 28597, 28636, 28714, 29092,
    29277, 29485, 29534, 29586, 29674, 29742, 29827, 30219, 30222, 30852,
    32428, 32766, 33591, 33686, 33803, 34115, 34232, 34864, 35052,
    35053, 35491, 35546, 36405, 36698, 37402, 37848, 39387, 42244, 42280,
    42999, 46282, 46508, 47245, 47374, 47802, 47934, 49109, 53435, 53723,
    56071, 57200, 58246
]  # fmt: skip
# leads, failing on main too (docs/handoff_formalization_next.md, area 1):
# what the planner raises, or None for wrong rows
OPEN: dict[tuple[int, str], str | None] = {
    (5754, "summary"): "Merge sources",
    (9016, "base"): None,
    (9016, "summary"): None,
    (13009, "summary"): "Missing source reference",
    (15155, "summary"): None,
    (15258, "base"): "keyless join",
    (15258, "summary"): "keyless join",
    (23531, "summary"): None,
    (24246, "base"): None,
    (24246, "summary"): None,
    (26698, "base"): "keyless join",
    (26698, "summary"): "keyless join",
    (30553, "base"): "keyless join",
    (30553, "summary"): "keyless join",
    (32125, "summary"): "Merge sources",
    (32239, "summary"): "Ambiguous reference",
    (34415, "summary"): None,
    (37601, "summary"): None,
    (38002, "summary"): None,
    (39273, "base"): "keyless join",
    (39273, "summary"): "keyless join",
    (41277, "base"): "Graph contains a cycle",
    (41277, "summary"): "Graph contains a cycle",
    (44364, "base"): None,
    (44364, "summary"): None,
    (46333, "base"): None,
    (46333, "summary"): None,
    (47456, "summary"): None,
    (48060, "base"): None,
    (48060, "summary"): None,
    (49126, "base"): "Attempting to merge two ctes",
    (49126, "summary"): "Attempting to merge two ctes",
    (49263, "base"): None,
    (49263, "summary"): None,
    (50635, "base"): "keyless join",
    (50635, "summary"): "keyless join",
    (50815, "base"): None,
    (50815, "summary"): None,
    (54254, "base"): None,
    (54254, "summary"): None,
    (55114, "base"): None,
    (55114, "summary"): None,
    (55868, "base"): None,
    (55868, "summary"): None,
    (57225, "base"): "keyless join",
    (57225, "summary"): "keyless join",
    (59820, "base"): "Attempting to merge two ctes",
    (59820, "summary"): "Attempting to merge two ctes",
    (59941, "base"): "keyless join",
    (59941, "summary"): "keyless join",
}


def cases():
    for seed in dict.fromkeys((*range(SEEDS), *FOUND_SEEDS, *(s for s, _ in OPEN))):
        for model in ("base", "summary"):
            marks = [pytest.mark.xfail(strict=True)] if (seed, model) in OPEN else []
            yield pytest.param(seed, model, marks=marks)


@pytest.mark.parametrize("seed, model", list(cases()))
def test_composition_matches_raw_row_oracle(seed: int, model: str):
    query, expected = shape4(seed)
    try:
        sql = executor(model).generate_sql(query)[-1]
        rows = executor(model).execute_raw_sql(sql).fetchall()
    except Exception as e:
        if REJECTED in str(e):
            return
        if (reason := OPEN.get((seed, model))) is not None and reason not in str(e):
            pytest.fail(f"{query}\n{e}", pytrace=False)
        raise
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
