"""`formal/Trilogy/Filters.lean` (`filtered_value_on_its_keys`,
`filtered_key_groups_rows`, `alone_keeps_passing_rows`): a sixth family of
random selects against the raw-row oracle of `test_oracle_sweep_laws.py`, in
what the fifth names as left: a `?` as a select target outside an aggregate.
A filtered column (`species ? tree_form = 'broadleaf' as f`) is written alone,
beside the select's keys, as a group key of aggregates and windows, in a
HAVING and in an `order by`; a filtered aggregate (`sum(height) ? c`) beside
the keys that decide its condition.

Semantics as in the fifth family, plus: `x ? c` is `x` on the rows `c` holds
on and NULL on the others, a value of each row, so it groups the rows as any
key does and a rejected row is a NULL group; its condition's aggregates and
sets read the rows the WHERE kept; a select of filtered values alone returns
the rows the condition keeps.

Every condition here is a function of the keys of the value it filters
(`species ? tag = 'x'` is not written): where it is not, what one species
shows for trees that disagree is undecided
(docs/handoff_formalization_next.md, area 4)."""

import random
from collections import defaultdict
from copy import deepcopy
from functools import cache

import pytest

from tests.discovery.test_oracle_compositions_laws import (
    by_text,
    cond_text,
    condition,
    membership,
)
from tests.discovery.test_oracle_expressions_laws import (
    EXACT,
    KEYSETS3,
    ROWS3,
    grains,
    key_text,
    ordered,
)
from tests.discovery.test_oracle_spellings_laws import OPS, closure2, compare
from tests.discovery.test_oracle_sweep_laws import (
    ARG,
    REJECTED,
    THRESHOLDS,
    lit,
    normalize,
)
from tests.discovery.test_oracle_windows_laws import (
    scoped_eval,
    target_eval,
    target_text,
    window,
    window_values,
)
from tests.discovery.test_rollup_resolution_laws import BASE, SUMMARY
from trilogy import Dialects, Environment
from trilogy.executor import Executor

ROW_COLUMNS = {"tree_id", "city", "species", "tree_form", "height", "tag", "tall"}
# what the keys of a filtered column determine: the columns its condition reads
DETERMINED = {
    "tree_id": ROW_COLUMNS,
    "height": ROW_COLUMNS,
    "tag": ROW_COLUMNS,
    "tall": ROW_COLUMNS,
    "species": {"species", "tree_form"},
    "tree_form": {"species", "tree_form"},
    "city": {"city"},
}
FILTERED = ["tree_id", "species", "species", "city", "tree_form", "height", "tag"]
FILTERED_AGGS = ["count", "sum", "max", "min"]
FILTERS = ("fcol", "fagg")


def reads(c) -> set[str]:
    """The columns a condition's value depends on, per row."""
    k = c[0]
    if k in ("row", "null", "in", "mem"):
        return {c[1]}
    if k == "between":
        return {"height"}
    if k == "coalesce":
        return {"tree_form"}
    if k in ("gate", "gate_null", "gate_in"):
        return set(c[2])
    if k in ("gate2", "gate_diff"):
        return set(c[2]) | set(c[4])
    if k == "not":
        return reads(c[1])
    return reads(c[1]) | reads(c[2])


def decided_by(rng: random.Random, allowed: set[str]):
    """A condition reading only `allowed`: constant where those are."""
    for _ in range(200):
        c = condition(rng, rng.choice([0, 0, 1]))
        if reads(c) <= allowed:
            return c
    return ("gate", "count", [], ">", rng.choice(THRESHOLDS))


def keyed_grains(keys: list[str]) -> list[list[str]]:
    """A filtered total beside rows is open: `(min(height) by *) ? tag = 'x'`
    is read as one value for the statement."""
    return [g for g in grains(keys) if g] or [keys]


def filtered(rng: random.Random, keys: list[str]):
    roll = rng.random()
    if roll < 0.75 or not keys:
        col = rng.choice(FILTERED)
        return ("fcol", col, decided_by(rng, DETERMINED[col]))
    inner = (
        ("agg", rng.choice(FILTERED_AGGS))
        if roll < 0.9
        else ("by", rng.choice(FILTERED_AGGS), rng.choice(keyed_grains(keys)))
    )
    return ("fagg", inner, decided_by(rng, closure2(keys) & ROW_COLUMNS))


def other(rng: random.Random, keys: list[str], names: list[str]):
    """A value beside the filtered ones; `names` are the select's group keys,
    the filtered columns among them."""
    roll = rng.random()
    if roll < 0.3 and names:
        return window(rng, names)
    if roll < 0.5:
        inner = membership(rng) if rng.random() < 0.6 else condition(rng, 1)
        return ("fmem", rng.choice(FILTERED_AGGS), inner)
    if roll < 0.75:
        return ("agg", rng.choice(EXACT))
    return ("by", rng.choice(EXACT), rng.choice(grains(keys)))


def filter_text(t, parens: bool) -> str:
    cond = cond_text(t[2], top=not parens)
    if t[0] == "fcol":
        return f"{t[1]} ? {cond}"
    inner = t[1]
    if inner[0] == "agg":
        return f"{inner[1]}({ARG[inner[1]]}) ? {cond}"
    return f"({by_text(inner[1], inner[2])}) ? {cond}"


def having(rng: random.Random, targets: list):
    numeric = [
        i
        for i, t in enumerate(targets)
        if t[0] not in FILTERS or t[0] == "fagg" or t[1] == "height"
    ]
    nullable = [i for i, t in enumerate(targets) if t[0] in FILTERS]
    if numeric and rng.random() < 0.5:
        return ("out", rng.choice(numeric), rng.choice(OPS), rng.choice(THRESHOLDS))
    return ("isnull", rng.choice(nullable), rng.random() < 0.5)


def having_text(h) -> str:
    if h[0] == "out":
        return f"v{h[1]} {h[2]} {lit(h[3])}"
    return f"v{h[1]} is {'' if h[2] else 'not '}null"


def having_holds(h, values: list) -> bool:
    if h[0] == "out":
        return compare(h[2], values[h[1]], h[3]) is True
    return (values[h[1]] is None) == h[2]


def oracle(keys, targets, where, hv, order, limit, alone: bool):
    kept = [r for r in ROWS3 if where is None or scoped_eval(where, r, ROWS3) is True]
    columns = [i for i, t in enumerate(targets) if t[0] == "fcol"]
    passes = {
        i: [scoped_eval(targets[i][2], r, kept) is True for r in kept] for i in columns
    }
    if alone and columns:
        # a select of a filtered column alone is the rows its condition keeps
        held = [r for r, ok in zip(kept, passes[0]) if ok]
        out = list(dict.fromkeys((r[targets[0][1]],) for r in held))
        return ordered(out, order, limit) if order else out
    rows = [
        {**r, **{f"v{i}": r[targets[i][1]] if passes[i][n] else None for i in columns}}
        for n, r in enumerate(kept)
    ]
    names = keys + [f"v{i}" for i in columns]
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[tuple(r[k] for k in names)].append(r)
    wins = {
        i: window_values(t, names, groups)
        for i, t in enumerate(targets)
        if t[0] == "win"
    }
    out = []
    for key, group in groups.items():
        values = []
        for i, t in enumerate(targets):
            if t[0] == "fcol":
                values.append(group[0][f"v{i}"])
            elif t[0] == "fagg":
                held = scoped_eval(t[2], group[0], kept) is True
                values.append(
                    target_eval(t[1], group, kept, group[0]) if held else None
                )
                if alone and not held:
                    break
            elif i in wins:
                values.append(wins[i][key])
            else:
                values.append(target_eval(t, group, kept, group[0]))
        else:
            if hv is None or having_holds(hv, values):
                out.append(key[: len(keys)] + tuple(values))
    return ordered(out, order, limit) if order else out


def shape6(seed: int) -> tuple[str, list[tuple]]:
    rng = random.Random(seed)
    keys = rng.choice(KEYSETS3)
    filters = [filtered(rng, keys) for _ in range(rng.choice([1, 1, 1, 2]))]
    columns = [f"v{i}" for i, t in enumerate(filters) if t[0] == "fcol"]
    rest = [other(rng, keys, keys + columns) for _ in range(rng.choice([0, 0, 1, 2]))]
    alone = not keys and not rest
    targets = (filters[:1] if alone else filters) + rest
    where = condition(rng, rng.choice([0, 1, 2])) if rng.random() < 0.6 else None
    hv = having(rng, targets) if not alone and rng.random() < 0.25 else None
    names = list(keys) + [f"v{i}" for i in range(len(targets))]
    order = limit = None
    # every output is ordered on, so the rows a limit keeps are decided
    if rng.random() < 0.3:
        cols = list(range(len(names)))
        rng.shuffle(cols)
        order = [(i, rng.random() < 0.5, rng.random() < 0.5) for i in cols]
        limit = rng.choice([1, 2, 3])
    group_keys = keys + columns
    texts = [key_text(k) for k in keys] + [
        (
            filter_text(t, seed % 3 == 0)
            if t[0] in FILTERS
            else target_text(t, group_keys if t[0] == "win" else keys)
        )
        + f" as v{i}"
        for i, t in enumerate(targets)
    ]
    q = "select " + ", ".join(texts)
    if where:
        q += " where " + cond_text(where, top=seed % 2 == 1)
    if hv:
        q += " having " + having_text(hv)
    if order:
        q += " order by " + ", ".join(
            f"{names[i]} {'desc' if d else 'asc'} nulls {'first' if nf else 'last'}"
            for i, d, nf in order
        )
        q += f" limit {limit}"
    return q + ";", oracle(keys, targets, where, hv, order, limit, alone)


SEEDS = 400
# a sample of the 1,810 seeds in 0-20,000 that 0.3.531 fixed (each failed on
# main), the ones a first version of the round broke (5741, 7585, 11897,
# 12260, 17195, 19498), and 14276, which planned right or wrong by hash seed
FOUND_SEEDS = [
    460, 543, 651, 763, 1074, 1088, 1216, 1230, 1604, 1725, 1733, 1734, 1882,
    1939, 2071, 2093, 2219, 2256, 2267, 2285, 2339, 2424, 2463, 2470, 2669,
    2671, 2680, 2802, 2991, 3099, 3270, 3272, 3297, 3332, 3417, 3489, 4224,
    4402, 4491, 4560, 4598, 4910, 4924, 5168, 5221, 5331, 5736, 5741, 5801,
    5867, 5918, 6021, 6065, 6153, 6192, 6246, 6663, 6672, 6771, 6923, 6974,
    6984, 7207, 7277, 7478, 7585, 7636, 7680, 7742, 7846, 7849, 7970, 7991,
    7995, 8377, 8457, 8471, 9101, 9235, 9363, 9500, 9760, 9780, 10099, 10292,
    10366, 10426, 10457, 10913, 11020, 11036, 11172, 11174, 11255, 11319,
    11326, 11684, 11800, 11897, 11916, 12202, 12212, 12260, 12272, 12402,
    12535, 12551, 12588, 12673, 12675, 12686, 12753, 12769, 13083, 13273,
    13548, 13680, 13765, 13815, 13819, 13849, 13989, 14140, 14174, 14276,
    14300, 14306, 14323, 14475, 14587, 14951, 14968, 15277, 15367, 15521,
    15735, 15736, 15833, 15836, 16088, 16097, 16163, 16168, 16243, 16317,
    16519, 16735, 16767, 17195, 17283, 17313, 17324, 17910, 17918, 18157,
    18271, 18303, 18386, 18389, 18422, 18526, 18543, 18623, 18788, 18876,
    18880, 18986, 19039, 19042, 19229, 19291, 19331, 19374, 19498, 19500,
    19552, 19841, 19868, 19935,
    # 0.3.532: a filter stream paired below its grain, a collapse past the
    # select's other key, a property grouped past by its hidden key
    6561, 8237, 8819, 8983, 12692, 14687, 16397, 22124, 23264, 25756, 26633,
    28080, 29139, 30277, 30681, 32425, 33862, 35245, 37814, 38603,
    # 0.3.534: a filter's keys and condition columns kept on its rows, a gate
    # under its condition re-applied on them, a decided filter deduplicated
    # past its hidden key
    12986, 13488, 20181, 23888, 30937, 41909, 42287, 43265, 44631, 45444,
    46449, 48542, 48567, 49997, 52847, 54424, 55172, 57937, 59782,
    # 0.3.535: a shown filter an aggregate answers read with the select's
    # keys beside it
    20025, 20955, 24114, 25247, 27088, 30256, 30456, 31900, 36448, 36977,
    39296, 40546, 41916, 47769, 50507, 57793, 59771, 62466, 69047, 70342,
    71319, 73639, 76828, 79687,
    # 0.3.536: a set holds no select keys, a set's input parents no derived
    # key, a shown filter meets the gate on its grain, and a gate beside a
    # WHERE membership is backfilled onto the outputs
    854, 930, 1306, 1820, 2356, 3456, 3594, 3634, 3777, 3884, 3915, 4122,
    4425, 4586, 4639, 5085, 5213, 5690, 5706, 5953, 6484, 6854, 7404, 8126,
    8768, 9068, 9582, 9743, 10503, 11518, 11600, 11741, 12337, 12460, 12508,
    12927, 12928, 13179, 13920, 14317, 15308, 15706, 16447, 16504, 16930,
    17435, 17591, 17759, 17815, 18285, 18628, 18809, 18864, 19226,
    19292, 19423, 80568, 82423, 82889, 83127, 84440, 85135, 85660, 85675,
    85723, 90166, 91873, 92007, 93028, 93577, 93700, 93784, 95193, 95283,
    96257, 96425, 96630, 97546, 98204, 99771,
]  # fmt: skip
# leads, failing on main too (docs/handoff_formalization_next.md, area 4b):
# what the planner raises or DuckDB rejects, or None for wrong rows. 9683,
# 9919 and 12294 raised on main and return wrong rows.
OPEN: dict[tuple[int, str], str | None] = {
    (349, "base"): "maximum recursion",
    (349, "summary"): "maximum recursion",
    (667, "base"): "maximum recursion",
    (667, "summary"): "maximum recursion",
    (841, "base"): None,
    (841, "summary"): None,
    (855, "base"): "Graph contains a cycle",
    (855, "summary"): "Graph contains a cycle",
    (963, "base"): None,
    (963, "summary"): None,
    (1097, "base"): "cannot be related to the query outputs",
    (1097, "summary"): "cannot be related to the query outputs",
    (2486, "base"): "keyless join",
    (2486, "summary"): "keyless join",
    (2922, "base"): None,
    (2922, "summary"): None,
    (3730, "base"): "maximum recursion",
    (3730, "summary"): "maximum recursion",
    (3796, "base"): None,
    (3796, "summary"): None,
    (4180, "base"): "keyless join",
    (4180, "summary"): "keyless join",
    (5105, "base"): "maximum recursion",
    (5105, "summary"): "maximum recursion",
    (5172, "base"): "keyless join",
    (5172, "summary"): "keyless join",
    (5381, "base"): None,
    (5381, "summary"): None,
    (5962, "base"): "keyless join",
    (5962, "summary"): "keyless join",
    (6905, "base"): "keyless join",
    (6905, "summary"): "keyless join",
    (7232, "base"): "Could not resolve connections",
    (7232, "summary"): "Could not resolve connections",
    (7424, "base"): "Missing source reference",
    (7424, "summary"): "Missing source reference",
    (7739, "base"): "Could not resolve connections",
    (7739, "summary"): "Could not resolve connections",
    (8474, "base"): None,
    (8474, "summary"): None,
    (8790, "base"): "keyless join",
    (8790, "summary"): "keyless join",
    (8880, "base"): "cannot be related to the query outputs",
    (8880, "summary"): "cannot be related to the query outputs",
    (8962, "base"): None,
    (8962, "summary"): None,
    (9683, "base"): None,
    (9683, "summary"): None,
    (9759, "base"): "keyless join",
    (9759, "summary"): "keyless join",
    (9884, "base"): "cannot be related to the query outputs",
    (9884, "summary"): "cannot be related to the query outputs",
    (9904, "base"): "Missing source reference",
    (9904, "summary"): "Missing source reference",
    (9919, "base"): None,
    (9919, "summary"): None,
    (10213, "base"): None,
    (10213, "summary"): None,
    (10902, "base"): "Binder Error",
    (10902, "summary"): "Binder Error",
    (10955, "base"): "maximum recursion",
    (10955, "summary"): "maximum recursion",
    (12138, "base"): "keyless join",
    (12138, "summary"): "keyless join",
    (12294, "base"): None,
    (12294, "summary"): None,
    (12376, "base"): None,
    (12376, "summary"): None,
    (12386, "base"): "Graph contains a cycle",
    (12386, "summary"): "Graph contains a cycle",
    (12425, "base"): "renders no column",
    (12425, "summary"): "renders no column",
    (12695, "base"): "maximum recursion",
    (12695, "summary"): "maximum recursion",
    (12841, "base"): None,
    (13411, "base"): "Could not resolve connections",
    (13411, "summary"): "Could not resolve connections",
    (14427, "base"): "keyless join",
    (14427, "summary"): "keyless join",
    (14626, "base"): "Graph contains a cycle",
    (14626, "summary"): "Graph contains a cycle",
    (14670, "summary"): "aggregate function calls cannot",
    (16538, "base"): None,
    (16538, "summary"): None,
    (17067, "base"): "Missing source reference",
    (17067, "summary"): "Missing source reference",
    (17186, "base"): "maximum recursion",
    (17186, "summary"): "maximum recursion",
    (17527, "base"): "cannot be related to the query outputs",
    (17527, "summary"): "cannot be related to the query outputs",
    (17550, "base"): None,
    (17550, "summary"): None,
    (17742, "base"): "Could not resolve connections",
    (17742, "summary"): "Could not resolve connections",
    (17748, "base"): "keyless join",
    (17748, "summary"): "keyless join",
    (18255, "base"): None,
    (18255, "summary"): None,
    (18617, "base"): None,
    (18617, "summary"): None,
    (18629, "base"): None,
    (18629, "summary"): None,
    (18960, "base"): "Graph contains a cycle",
    (18960, "summary"): "Graph contains a cycle",
    (19214, "base"): "cannot be related to the query outputs",
    (19214, "summary"): "cannot be related to the query outputs",
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
def test_filtered_select_matches_raw_row_oracle(seed: int, model: str):
    """Each case plans on a copy of the parsed model, as the sweep does."""
    query, expected = shape6(seed)
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
