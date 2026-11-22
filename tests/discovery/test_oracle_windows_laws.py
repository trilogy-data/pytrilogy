"""`formal/Trilogy/Windows.lean` (`window_over_groups_reads_groups`,
`property_key_rows_repeat_groups`) and `formal/Trilogy/SelectGrain.lean`
(`select_set_reads_kept_rows`): a fifth family of random selects against the
raw-row oracle of `test_oracle_sweep_laws.py`, in what the fourth names as
left: windows other than a single-term `rank` over the select's groups
(`row_number`, `dense_rank`, `lag`, `lead`, running and partition aggregates,
in both spellings), a membership inside a select target's `?`, a membership in
a HAVING, and `/` by a `by` aggregate that can be 0.

Semantics as in the fourth family, plus: a window reads one row per group the
WHERE kept, before HAVING; an ordered aggregate window's frame runs to the
current group's last peer; a set written in the select or the HAVING reads the
rows the WHERE kept, as an aggregate there does."""

import random
from collections import defaultdict
from copy import deepcopy
from functools import cache

import pytest

from tests.discovery.test_oracle_compositions_laws import (
    MEMBER_COLUMNS,
    by_text,
    cond_text,
    condition,
    membership,
    operand,
    order_text,
    plain,
)
from tests.discovery.test_oracle_compositions_laws import (
    _order_value as order_value,
)
from tests.discovery.test_oracle_compositions_laws import target as target4
from tests.discovery.test_oracle_compositions_laws import target_eval as eval4
from tests.discovery.test_oracle_compositions_laws import target_text as text4
from tests.discovery.test_oracle_expressions_laws import (
    KEYSETS3,
    ROWS3,
    grains,
    key_text,
    ordered,
)
from tests.discovery.test_oracle_spellings_laws import OPS, closure2, compare
from tests.discovery.test_oracle_spellings_laws import cond_eval as cond_eval2
from tests.discovery.test_oracle_sweep_laws import (
    ARG,
    REJECTED,
    THRESHOLDS,
    aggregate,
    both,
    group_values,
    lit,
    normalize,
)
from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY
from trilogy import Dialects, Environment
from trilogy.executor import Executor

NUMBERING = ["row_number", "dense_rank", "rank"]
NAVIGATION = ["lag", "lead"]
FRAMED = ["sum", "count", "max", "min", "avg"]
WINDOW_AGGS = ["count", "sum", "max", "min"]
# (aggregate, what is taken off it): 0 on some group at some grain
BY_DENOMS = [
    ("count", 1),
    ("count", 2),
    ("count_distinct", 1),
    ("sum", 5),
    ("sum", 7),
    ("max", 5),
]


def scoped_eval(c, row: dict, scope: list[dict]):
    """A condition whose sets and gates read `scope`."""
    k = c[0]
    if k == "mem":
        held = {r[c[1]] for r in scope if scoped_eval(c[2], r, scope) is True}
        hit = row[c[1]] in held
        return not hit if c[3] else hit
    if k == "not":
        return scoped_eval(c[1], row, scope) is not True
    if k in ("and", "or"):
        return both(k, scoped_eval(c[1], row, scope), scoped_eval(c[2], row, scope))
    return cond_eval2(c, row, scope)


def order_terms(rng: random.Random, fn: str, keys: list[str], over: list[str]):
    """Order terms as (column or None for the ordering aggregate, desc, nulls
    first). `row_number`, `lag` and `lead` get a total order."""
    rest = [k for k in keys if k not in over]
    rng.shuffle(rest)
    tie = [(k, rng.random() < 0.5, rng.random() < 0.5) for k in rest]
    lead = [(None, rng.random() < 0.5, rng.random() < 0.5)]
    if fn in ("row_number", *NAVIGATION):
        return (lead if rng.random() < 0.5 else []) + tie
    if fn in ("dense_rank", "rank"):
        return lead + (tie if rng.random() < 0.3 else [])
    return rng.choice([[], lead, tie, lead + tie])


def window(rng: random.Random, keys: list[str]):
    fn = rng.choice(NUMBERING + NAVIGATION + NAVIGATION + FRAMED + FRAMED)
    over = [k for k in keys if rng.random() < 0.4]
    if len(over) == len(keys) and rng.random() < 0.8:
        over = over[:-1]
    return (
        "win",
        fn,
        rng.choice(WINDOW_AGGS),
        over,
        order_terms(rng, fn, keys, over),
        rng.choice(WINDOW_AGGS),
        rng.choice([1, 1, 2]),
        rng.random() < 0.5,
        plain(keys) and rng.random() < 0.5,
    )


def target(rng: random.Random, keys: list[str]):
    roll = rng.random()
    if roll < 0.15:
        return target4(rng, keys)
    if roll < 0.5 and keys:
        return window(rng, keys)
    if roll < 0.7:
        inner = membership(rng) if rng.random() < 0.7 else condition(rng, 1)
        return ("fmem", rng.choice(["count", "sum", "max", "min"]), inner)
    if roll < 0.87:
        agg, less = rng.choice(BY_DENOMS)
        return ("bydiv", operand(rng, keys), agg, rng.choice(grains(keys)), less)
    return operand(rng, keys)


def over_text(fn: str, over: list[str], order: str, sql: bool) -> str:
    if sql:
        parts = [f"partition by {', '.join(over)}"] if over else []
        return f" over ({' '.join(parts + ([f'order by {order}'] if order else []))})"
    return (f" over {', '.join(over)}" if over else "") + (
        f" order by {order}" if order else ""
    )


def window_text(t, keys: list[str]) -> str:
    _, fn, agg, over, terms, order_agg, offset, sql, explicit = t

    def measure(a: str) -> str:
        return by_text(a, keys) if explicit else f"{a}({ARG[a]})"

    order = ", ".join(
        f"{measure(order_agg) if c is None else c} {'desc' if d else 'asc'}"
        f" nulls {'first' if nf else 'last'}"
        for c, d, nf in terms
    )
    # the legacy spelling names an argument, and an unordered aggregate none
    free = [k for k in keys if k not in over]
    sql = sql or (fn in NUMBERING and not free) or (fn in FRAMED and not terms)
    tail = over_text(fn, over, order, sql)
    if fn in NUMBERING:
        return f"{fn}(){tail}" if sql else f"{fn} {free[0]}{tail}"
    if fn in NAVIGATION:
        if sql:
            step = f", {offset}" if offset != 1 else ""
            return f"{fn}({measure(agg)}{step}){tail}"
        return f"{fn} {f'{offset} ' if offset != 1 else ''}{measure(agg)}{tail}"
    return f"{fn}({measure(agg)}){tail}" if sql else f"{fn} {measure(agg)}{tail}"


def target_text(t, keys: list[str]) -> str:
    k = t[0]
    if k == "win":
        return window_text(t, keys)
    if k == "fmem":
        return f"{t[1]}({ARG[t[1]]} ? {cond_text(t[2], top=True)})"
    if k == "bydiv":
        return f"{text4(t[1], keys)} / ({by_text(t[2], t[3])} - {t[4]})"
    return text4(t, keys)


def target_eval(t, group: list[dict], kept: list[dict], row: dict):
    k = t[0]
    if k == "fmem":
        return aggregate(
            t[1],
            [r[ARG[t[1]]] for r in group if scoped_eval(t[2], r, kept) is True],
        )
    if k == "bydiv":
        a = eval4(t[1], group, kept, row)
        b = group_values(kept, t[2], t[3])[tuple(row[x] for x in t[3])]
        if a is None or b is None or b - t[4] == 0:
            return None
        return a / (b - t[4])
    return eval4(t, group, kept, row)


def window_values(t, keys: list[str], groups: dict) -> dict:
    """The window's value per group: partitioned, ordered, then read."""
    _, fn, agg, over, terms, order_agg, offset, _, _ = t

    def measure(a: str, key: tuple):
        return aggregate(a, [r[ARG[a]] for r in groups[key]])

    def sort_tuple(key: tuple) -> tuple:
        return tuple(
            measure(order_agg, key) if c is None else key[keys.index(c)]
            for c, _, _ in terms
        )

    parts: dict[tuple, list[tuple]] = defaultdict(list)
    for key in groups:
        parts[tuple(key[keys.index(c)] for c in over)].append(key)
    out = {}
    for members in parts.values():
        rows = [(key, *sort_tuple(key)) for key in members]
        spec = [(i + 1, d, nf) for i, (_, d, nf) in enumerate(terms)]
        rows = ordered(rows, spec, None) if spec else rows
        values = [measure(agg, r[0]) for r in rows]
        peers = [r[1:] for r in rows]
        for i, r in enumerate(rows):
            first = peers.index(peers[i])
            if fn == "row_number":
                v = i + 1
            elif fn == "rank":
                v = first + 1
            elif fn == "dense_rank":
                v = len(dict.fromkeys(peers[: first + 1]))
            elif fn in NAVIGATION:
                j = i - offset if fn == "lag" else i + offset
                v = values[j] if 0 <= j < len(rows) else None
            else:
                # the frame runs to the current group's last peer
                last = len(rows) - 1 - peers[::-1].index(peers[i])
                v = aggregate(fn, values[: last + 1] if terms else values)
            out[r[0]] = v
    return out


def having(rng: random.Random, keys: list[str], numeric: list[int]):
    members = [
        c for c in MEMBER_COLUMNS if c in closure2(keys) and (c in keys or plain(keys))
    ]
    roll = rng.random()
    if numeric and (roll < 0.4 or not members):
        return ("out", rng.choice(numeric), rng.choice(OPS), rng.choice(THRESHOLDS))
    if not members:
        return None
    col = rng.choice(members)
    mem = ("mem", col, membership(rng)[2], rng.random() < 0.2)
    if numeric and roll < 0.6:
        out = ("out", rng.choice(numeric), rng.choice(OPS), rng.choice(THRESHOLDS))
        return (rng.choice(["and", "or"]), mem, out)
    return mem


def having_text(h) -> str:
    if h[0] == "out":
        return f"v{h[1]} {h[2]} {lit(h[3])}"
    if h[0] == "mem":
        return cond_text(h)
    return f"{having_text(h[1])} {h[0]} {having_text(h[2])}"


def having_eval(h, values: list, row: dict, kept: list[dict]):
    if h[0] == "out":
        return compare(h[2], values[h[1]], h[3])
    if h[0] == "mem":
        return scoped_eval(h, row, kept)
    return both(
        h[0],
        having_eval(h[1], values, row, kept),
        having_eval(h[2], values, row, kept),
    )


def oracle(keys, targets, where, hv, order, limit):
    kept = [r for r in ROWS3 if where is None or scoped_eval(where, r, ROWS3) is True]
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in kept:
        groups[tuple(r[k] for k in keys)].append(r)
    wins = {
        i: (
            window_values(t, keys, groups) if t[0] == "win" else _rank4(t, keys, groups)
        )
        for i, t in enumerate(targets)
        if t[0] in ("win", "rank")
    }
    out = []
    for key, group in groups.items():
        values = [
            wins[i][key] if i in wins else target_eval(t, group, kept, group[0])
            for i, t in enumerate(targets)
        ]
        if hv and having_eval(hv, values, group[0], kept) is not True:
            continue
        out.append(key + tuple(values))
    if not order:
        return out
    expr, columns = order
    rows = [r + (order_value(expr, r),) for r in out]
    width = len(out[0]) if out else 0
    return [r[:-1] for r in ordered(rows, [(width, expr[2], expr[3])] + columns, limit)]


def _rank4(t, keys: list[str], groups: dict) -> dict:
    from tests.discovery.test_oracle_compositions_laws import rank

    return rank(t, keys, groups)


def shape5(seed: int) -> tuple[str, list[tuple]]:
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
    hv = having(rng, keys, numeric) if rng.random() < 0.3 else None
    names = list(keys) + [f"v{i}" for i in range(n)]
    order = limit = None
    if numeric and rng.random() < 0.3:
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
        q += " having " + having_text(hv)
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
# seeds past `SEEDS` 0.3.528 fixed, a sample of the 585 in 400-12,000 (each
# failed on main), and six a first version of the round broke (1378, 5596,
# 6526, 6539, 8656, 10171)
FOUND_SEEDS = [
    446, 567, 689, 744, 746, 883, 1057, 1081, 1248, 1250, 1378, 1465, 1485,
    1527, 1659, 1779, 1868, 1869, 1894, 2006, 2084, 2123, 2156, 2257, 2270,
    2350, 2408, 2414, 2447, 2471, 2515, 2522, 2680, 2720, 2756, 2852, 2950,
    2996, 3009, 3112, 3339, 3362, 3364, 3424, 3445, 3459, 3521, 3647, 3769,
    3826, 3835, 3871, 3978, 4156, 4189, 4198, 4232, 4273, 4337, 4513, 4529,
    4603, 4691, 4888, 4954, 5075, 5324, 5389, 5451, 5542, 5596, 5603, 5807,
    5813, 5832, 5859, 5980, 6050, 6314, 6389, 6405, 6435, 6436, 6465, 6484,
    6496, 6526, 6539, 6589, 6602, 6743, 6832, 6850, 6879, 6949, 6951, 7111,
    7214, 7283, 7291, 7378, 7410, 7580, 7615, 7663, 7664, 7778, 7955, 8178,
    8295, 8410, 8564, 8634, 8656, 8780, 8781, 8931, 9000, 9043, 9090, 9129,
    9133, 9190, 9299, 9312, 9330, 9452, 9557, 9668, 9783, 9785, 9796, 9852,
    9865, 10141, 10153, 10171, 10176, 10199, 10313, 10369, 10393, 10497,
    10583, 10589, 10592, 10594, 10663, 10809, 10942, 11000, 11009, 11023,
    11112, 11171, 11237, 11239, 11368, 11395, 11397, 11488, 11653, 11800,
    11832, 11864, 11948, 11968,
    # the seeds 0.3.528 left open that 0.3.529 fixed, and the ones it fixed in
    # 12,000-60,000 (each failed on main)
    1955, 1977, 5168, 6336, 7437, 8156, 9809, 10654, 11606, 12251, 13439,
    14447, 15356, 15534, 15956, 16332, 17119, 17259, 18045, 18618, 20956,
    25292, 27042, 28270, 28511, 29526, 29683, 30611, 32266, 32317, 33186,
    33367, 35060, 35211, 35289, 35483, 36074, 37068, 37134, 38522, 39292,
    40448, 40508, 41945, 42617, 43518, 44181, 45750, 45855, 46435, 48381,
    49376, 49516, 49903, 50693, 51718, 53113, 54531, 55680,
    56554, 57430, 59727,
    # open until 0.3.530: a re-sum beside a window (summary model)
    58721,
]  # fmt: skip
# leads, failing on main too (docs/handoff_formalization_next.md, area 4):
# what the planner raises or DuckDB rejects, or None for wrong rows
OPEN: dict[tuple[int, str], str | None] = {
    (1210, "base"): "Ambiguous reference to table",
    (1210, "summary"): "Ambiguous reference to table",
    (3018, "base"): "Missing source reference to local.tag",
    (3018, "summary"): "Missing source reference to local.tag",
    (10318, "base"): "cannot be related to the query outputs",
    (10318, "summary"): "cannot be related to the query outputs",
    (11457, "summary"): "does not have a column",
    (15366, "base"): "The plan renders no column",
    (16181, "base"): "Missing source CTE",
    (16181, "summary"): "Missing source CTE",
    (18040, "base"): None,
    (18040, "summary"): None,
    (24973, "base"): None,
    (24973, "summary"): None,
    (28981, "base"): "keyless join",
    (28981, "summary"): "keyless join",
    (34305, "base"): None,
    (34305, "summary"): None,
    (40037, "summary"): None,
    (42711, "base"): None,
    (42711, "summary"): None,
    (43504, "summary"): None,
    (47444, "summary"): None,
    (51466, "base"): None,
    (51466, "summary"): None,
    (54409, "base"): None,
    (54409, "summary"): None,
    (58098, "base"): "Missing source CTE",
    (58098, "summary"): "Missing source CTE",
    (58115, "base"): "Missing source reference to local.tree_id",
    (58115, "summary"): "Missing source reference to local.tree_id",
}


def cases():
    for seed in dict.fromkeys((*range(SEEDS), *FOUND_SEEDS, *(s for s, _ in OPEN))):
        for model in ("base", "summary"):
            marks = [pytest.mark.xfail(strict=True)] if (seed, model) in OPEN else []
            yield pytest.param(seed, model, marks=marks)


@cache
def fresh(model: str) -> tuple[Executor, Environment]:
    exec = Dialects.DUCK_DB.default_executor()
    exec.parse_text(BASE + (SUMMARY if model == "summary" else ""))
    return exec, deepcopy(exec.environment)


@pytest.mark.parametrize("seed, model", list(cases()))
def test_window_select_matches_raw_row_oracle(seed: int, model: str):
    """Each case plans on a copy of the parsed model, as the sweep does: a
    select defines its outputs, and a case must not depend on the ones before
    it (`--carry-state` sweeps that)."""
    query, expected = shape5(seed)
    exec, pristine = fresh(model)
    exec.environment = deepcopy(pristine)
    try:
        sql = exec.generate_sql(query)[-1]
        rows = exec.execute_raw_sql(sql).fetchall()
    except Exception as e:
        if REJECTED in str(e):
            return
        if (reason := OPEN.get((seed, model))) is not None and reason not in str(e):
            pytest.fail(f"{query}\n{e}", pytrace=False)
        raise
    assert normalize(rows) == normalize(expected), f"{query}\n{sql}"
