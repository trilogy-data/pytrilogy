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
# a sample of the 1,758 seeds in 0-20,000 that 0.3.530 fixed (each failed on
# main), and the ones a first version of the round broke (5741, 7585, 11897,
# 12260, 17195)
FOUND_SEEDS = [
    460, 651, 767, 823, 1074, 1113, 1225, 1264, 1604, 1725, 1747, 1760, 1900,
    1979, 2096, 2113, 2277, 2285, 2304, 2364, 2403, 2470, 2564, 2669, 2727,
    2860, 3038, 3134, 3297, 3357, 3366, 3410, 3493, 3554, 4224, 4503, 4588,
    4638, 4670, 4681, 5015, 5025, 5255, 5361, 5525, 5741, 5830, 5923, 5972,
    5985, 6002, 6108, 6361, 6380, 6390, 6769, 6771, 6941, 7033, 7098, 7109,
    7396, 7478, 7585, 7695, 7845, 7847, 7904, 7985, 7995, 8090, 8122, 8127,
    8165, 8576, 8629, 8634, 9322, 9425, 9576, 9685, 9959, 9995, 10366, 10519,
    10560, 10628, 10685, 11123, 11230, 11241, 11484, 11492, 11607, 11689,
    11769, 11897, 12046, 12212, 12260, 12283, 12471, 12472, 12521, 12673,
    12834, 12901, 12993, 13121, 13141, 13154, 13254, 13264, 13440, 13713,
    13859, 13954, 14003, 14150, 14172, 14183, 14337, 14475, 14511, 14688,
    14760, 14766, 14861, 14970, 15040, 15314, 15340, 15659, 15733, 15865,
    16087, 16088, 16163, 16167, 16555, 16609, 16659, 16688, 16698, 16794,
    17129, 17195, 17238, 17243, 17773, 17839, 17840, 18459, 18471, 18708,
    18807, 18871, 18941, 18986, 19039, 19193, 19215, 19256, 19409, 19500,
    19514, 19590, 19631, 19635, 19789, 19866, 19978,
]  # fmt: skip
# leads, failing on main too (docs/handoff_formalization_next.md, area 4b):
# what the planner raises or DuckDB rejects, or None for wrong rows. 19498
# planned on main: its window's group lost a column a later value reads.
OPEN: dict[tuple[int, str], str | None] = {
    (251, "base"): "Merge sources",
    (251, "summary"): "Merge sources",
    (349, "base"): "maximum recursion",
    (349, "summary"): "maximum recursion",
    (543, "base"): None,
    (543, "summary"): None,
    (667, "base"): "maximum recursion",
    (667, "summary"): "maximum recursion",
    (841, "base"): None,
    (841, "summary"): None,
    (854, "base"): "cannot be related to the query outputs",
    (854, "summary"): "cannot be related to the query outputs",
    (855, "base"): "Graph contains a cycle",
    (855, "summary"): "Graph contains a cycle",
    (860, "base"): "keyless join",
    (860, "summary"): "keyless join",
    (930, "base"): "cannot be related to the query outputs",
    (930, "summary"): "cannot be related to the query outputs",
    (963, "base"): None,
    (963, "summary"): None,
    (1097, "base"): "cannot be related to the query outputs",
    (1097, "summary"): "cannot be related to the query outputs",
    (1306, "base"): None,
    (1306, "summary"): None,
    (1377, "base"): "keyless join",
    (1377, "summary"): "keyless join",
    (1820, "base"): "cannot be related to the query outputs",
    (1820, "summary"): "cannot be related to the query outputs",
    (1899, "base"): None,
    (1899, "summary"): None,
    (1938, "base"): None,
    (1938, "summary"): None,
    (2356, "base"): "cannot be related to the query outputs",
    (2356, "summary"): "cannot be related to the query outputs",
    (2486, "base"): "keyless join",
    (2486, "summary"): "keyless join",
    (2680, "base"): None,
    (2680, "summary"): None,
    (2922, "base"): None,
    (2922, "summary"): None,
    (3236, "base"): None,
    (3236, "summary"): None,
    (3238, "base"): "Ambiguous reference",
    (3238, "summary"): "Ambiguous reference",
    (3456, "base"): "cannot be related to the query outputs",
    (3456, "summary"): "cannot be related to the query outputs",
    (3594, "base"): "cannot be related to the query outputs",
    (3594, "summary"): "cannot be related to the query outputs",
    (3634, "base"): "cannot be related to the query outputs",
    (3634, "summary"): "cannot be related to the query outputs",
    (3730, "base"): "maximum recursion",
    (3730, "summary"): "maximum recursion",
    (3777, "base"): "cannot be related to the query outputs",
    (3777, "summary"): "cannot be related to the query outputs",
    (3796, "base"): None,
    (3796, "summary"): None,
    (3884, "base"): "Binder Error",
    (3884, "summary"): "Binder Error",
    (3915, "base"): "cannot be related to the query outputs",
    (4122, "base"): "cannot be related to the query outputs",
    (4122, "summary"): "cannot be related to the query outputs",
    (4180, "base"): "keyless join",
    (4180, "summary"): "keyless join",
    (4365, "base"): None,
    (4365, "summary"): None,
    (4425, "base"): "cannot be related to the query outputs",
    (4425, "summary"): "cannot be related to the query outputs",
    (4560, "base"): "keyless join",
    (4560, "summary"): "keyless join",
    (4586, "base"): "cannot be related to the query outputs",
    (4586, "summary"): "cannot be related to the query outputs",
    (4639, "base"): "cannot be related to the query outputs",
    (4639, "summary"): "cannot be related to the query outputs",
    (4729, "base"): "keyless join",
    (4729, "summary"): "keyless join",
    (5085, "base"): "keyless join",
    (5085, "summary"): "keyless join",
    (5105, "base"): "maximum recursion",
    (5105, "summary"): "maximum recursion",
    (5172, "base"): "keyless join",
    (5172, "summary"): "keyless join",
    (5213, "base"): "cannot be related to the query outputs",
    (5213, "summary"): "cannot be related to the query outputs",
    (5381, "base"): None,
    (5381, "summary"): None,
    (5434, "base"): "keyless join",
    (5434, "summary"): "keyless join",
    (5468, "base"): "keyless join",
    (5468, "summary"): "keyless join",
    (5572, "base"): None,
    (5572, "summary"): None,
    (5690, "base"): None,
    (5690, "summary"): None,
    (5706, "base"): "keyless join",
    (5706, "summary"): "keyless join",
    (5825, "base"): "keyless join",
    (5825, "summary"): "keyless join",
    (5867, "base"): "keyless join",
    (5867, "summary"): "keyless join",
    (5953, "base"): "keyless join",
    (5953, "summary"): "keyless join",
    (5962, "base"): "keyless join",
    (5962, "summary"): "keyless join",
    (6065, "base"): None,
    (6065, "summary"): None,
    (6259, "base"): None,
    (6259, "summary"): None,
    (6484, "base"): "cannot be related to the query outputs",
    (6484, "summary"): "cannot be related to the query outputs",
    (6561, "summary"): None,
    (6854, "base"): "keyless join",
    (6854, "summary"): "keyless join",
    (6905, "base"): "keyless join",
    (6905, "summary"): "keyless join",
    (6987, "base"): None,
    (6987, "summary"): None,
    (7016, "base"): None,
    (7016, "summary"): None,
    (7094, "base"): None,
    (7094, "summary"): None,
    (7232, "base"): "Could not resolve connections",
    (7232, "summary"): "Could not resolve connections",
    (7404, "base"): "cannot be related to the query outputs",
    (7404, "summary"): "cannot be related to the query outputs",
    (7424, "base"): "Missing source reference",
    (7424, "summary"): "Missing source reference",
    (7515, "base"): None,
    (7515, "summary"): None,
    (7559, "base"): "keyless join",
    (7739, "base"): "Could not resolve connections",
    (7739, "summary"): "Could not resolve connections",
    (8126, "base"): "cannot be related to the query outputs",
    (8126, "summary"): "cannot be related to the query outputs",
    (8142, "base"): "keyless join",
    (8142, "summary"): "keyless join",
    (8237, "base"): None,
    (8237, "summary"): None,
    (8310, "base"): None,
    (8310, "summary"): None,
    (8368, "base"): "keyless join",
    (8368, "summary"): "keyless join",
    (8474, "base"): None,
    (8474, "summary"): None,
    (8641, "base"): None,
    (8641, "summary"): None,
    (8768, "base"): "cannot be related to the query outputs",
    (8768, "summary"): "cannot be related to the query outputs",
    (8790, "base"): "keyless join",
    (8790, "summary"): "keyless join",
    (8819, "base"): None,
    (8819, "summary"): None,
    (8880, "base"): "cannot be related to the query outputs",
    (8880, "summary"): "cannot be related to the query outputs",
    (8920, "base"): None,
    (8920, "summary"): None,
    (8962, "base"): None,
    (8962, "summary"): None,
    (8983, "base"): None,
    (8983, "summary"): None,
    (9068, "base"): "cannot be related to the query outputs",
    (9068, "summary"): "cannot be related to the query outputs",
    (9547, "base"): "Ambiguous reference",
    (9547, "summary"): "Ambiguous reference",
    (9582, "base"): "cannot be related to the query outputs",
    (9582, "summary"): "cannot be related to the query outputs",
    (9646, "base"): "keyless join",
    (9646, "summary"): "keyless join",
    (9683, "base"): None,
    (9683, "summary"): None,
    (9743, "base"): "cannot be related to the query outputs",
    (9743, "summary"): "cannot be related to the query outputs",
    (9759, "base"): "keyless join",
    (9759, "summary"): "keyless join",
    (9787, "base"): "keyless join",
    (9787, "summary"): "keyless join",
    (9884, "base"): "cannot be related to the query outputs",
    (9884, "summary"): "cannot be related to the query outputs",
    (9904, "base"): "Missing source reference",
    (9904, "summary"): "Missing source reference",
    (9919, "base"): "keyless join",
    (9919, "summary"): "keyless join",
    (10213, "base"): None,
    (10213, "summary"): None,
    (10382, "base"): None,
    (10382, "summary"): None,
    (10396, "base"): "keyless join",
    (10396, "summary"): "keyless join",
    (10503, "base"): "Ambiguous reference",
    (10503, "summary"): "Ambiguous reference",
    (10710, "base"): "keyless join",
    (10710, "summary"): "keyless join",
    (10749, "base"): "keyless join",
    (10749, "summary"): "keyless join",
    (10902, "base"): "Binder Error",
    (10902, "summary"): "Binder Error",
    (10955, "base"): "maximum recursion",
    (10955, "summary"): "maximum recursion",
    (11099, "base"): None,
    (11099, "summary"): None,
    (11202, "base"): None,
    (11202, "summary"): None,
    (11389, "base"): None,
    (11389, "summary"): None,
    (11518, "base"): "cannot be related to the query outputs",
    (11518, "summary"): "cannot be related to the query outputs",
    (11600, "base"): "cannot be related to the query outputs",
    (11600, "summary"): "cannot be related to the query outputs",
    (11642, "base"): None,
    (11642, "summary"): None,
    (11684, "base"): None,
    (11684, "summary"): None,
    (11741, "base"): None,
    (11741, "summary"): None,
    (12138, "base"): "keyless join",
    (12138, "summary"): "keyless join",
    (12191, "base"): "keyless join",
    (12191, "summary"): "keyless join",
    (12294, "base"): None,
    (12294, "summary"): None,
    (12337, "base"): None,
    (12337, "summary"): None,
    (12376, "base"): None,
    (12376, "summary"): None,
    (12386, "base"): "Graph contains a cycle",
    (12386, "summary"): "Graph contains a cycle",
    (12411, "base"): "keyless join",
    (12411, "summary"): "keyless join",
    (12425, "base"): "renders no column",
    (12425, "summary"): "renders no column",
    (12460, "base"): "cannot be related to the query outputs",
    (12460, "summary"): "cannot be related to the query outputs",
    (12470, "base"): None,
    (12470, "summary"): None,
    (12508, "base"): "cannot be related to the query outputs",
    (12508, "summary"): "cannot be related to the query outputs",
    (12692, "base"): None,
    (12692, "summary"): None,
    (12695, "base"): "maximum recursion",
    (12695, "summary"): "maximum recursion",
    (12841, "base"): None,
    (12880, "base"): None,
    (12880, "summary"): None,
    (12927, "base"): "cannot be related to the query outputs",
    (12927, "summary"): "cannot be related to the query outputs",
    (12928, "base"): "cannot be related to the query outputs",
    (12928, "summary"): "cannot be related to the query outputs",
    (12986, "base"): None,
    (12986, "summary"): None,
    (13179, "base"): "keyless join",
    (13179, "summary"): "keyless join",
    (13221, "base"): "cannot be related to the query outputs",
    (13221, "summary"): "cannot be related to the query outputs",
    (13411, "base"): "Could not resolve connections",
    (13411, "summary"): "Could not resolve connections",
    (13488, "base"): None,
    (13488, "summary"): None,
    (13920, "base"): "cannot be related to the query outputs",
    (13920, "summary"): "cannot be related to the query outputs",
    (14073, "base"): None,
    (14073, "summary"): None,
    (14300, "base"): "keyless join",
    (14300, "summary"): "keyless join",
    (14317, "base"): "cannot be related to the query outputs",
    (14317, "summary"): "cannot be related to the query outputs",
    (14323, "base"): "keyless join",
    (14323, "summary"): "keyless join",
    (14427, "base"): "keyless join",
    (14427, "summary"): "keyless join",
    (14626, "base"): "Graph contains a cycle",
    (14626, "summary"): "Graph contains a cycle",
    (14670, "summary"): "aggregate function calls cannot",
    (14687, "base"): None,
    (14687, "summary"): None,
    (14730, "base"): "Merge sources",
    (14730, "summary"): "Merge sources",
    (14744, "base"): "keyless join",
    (14744, "summary"): "keyless join",
    (15164, "base"): "keyless join",
    (15243, "base"): None,
    (15243, "summary"): None,
    (15308, "base"): "keyless join",
    (15308, "summary"): "keyless join",
    (15352, "base"): "renders no column",
    (15352, "summary"): "renders no column",
    (15641, "base"): None,
    (15673, "base"): "keyless join",
    (15673, "summary"): "keyless join",
    (15706, "base"): "cannot be related to the query outputs",
    (15706, "summary"): "cannot be related to the query outputs",
    (15735, "base"): "keyless join",
    (15735, "summary"): "keyless join",
    (15837, "base"): "keyless join",
    (15837, "summary"): "keyless join",
    (15852, "base"): None,
    (15852, "summary"): None,
    (15958, "base"): None,
    (15958, "summary"): None,
    (16097, "base"): "keyless join",
    (16097, "summary"): "keyless join",
    (16166, "base"): "keyless join",
    (16166, "summary"): "keyless join",
    (16397, "base"): None,
    (16397, "summary"): None,
    (16447, "base"): "cannot be related to the query outputs",
    (16447, "summary"): "cannot be related to the query outputs",
    (16504, "base"): "cannot be related to the query outputs",
    (16504, "summary"): "cannot be related to the query outputs",
    (16538, "base"): None,
    (16538, "summary"): None,
    (16601, "base"): "keyless join",
    (16601, "summary"): "keyless join",
    (16606, "base"): None,
    (16606, "summary"): None,
    (16930, "base"): "cannot be related to the query outputs",
    (16930, "summary"): "cannot be related to the query outputs",
    (16989, "base"): "keyless join",
    (16989, "summary"): "keyless join",
    (17067, "base"): "Missing source reference",
    (17067, "summary"): "Missing source reference",
    (17095, "base"): "keyless join",
    (17095, "summary"): "keyless join",
    (17186, "base"): "maximum recursion",
    (17186, "summary"): "maximum recursion",
    (17435, "base"): "cannot be related to the query outputs",
    (17435, "summary"): "cannot be related to the query outputs",
    (17527, "base"): "cannot be related to the query outputs",
    (17527, "summary"): "cannot be related to the query outputs",
    (17550, "base"): None,
    (17550, "summary"): None,
    (17591, "base"): "cannot be related to the query outputs",
    (17591, "summary"): "cannot be related to the query outputs",
    (17742, "base"): "Could not resolve connections",
    (17742, "summary"): "Could not resolve connections",
    (17748, "base"): "keyless join",
    (17748, "summary"): "keyless join",
    (17759, "base"): "cannot be related to the query outputs",
    (17783, "base"): "keyless join",
    (17783, "summary"): "keyless join",
    (17815, "base"): "cannot be related to the query outputs",
    (17815, "summary"): "cannot be related to the query outputs",
    (17910, "base"): None,
    (17910, "summary"): None,
    (18255, "base"): None,
    (18255, "summary"): None,
    (18285, "base"): "Ambiguous reference",
    (18285, "summary"): "Ambiguous reference",
    (18408, "base"): "keyless join",
    (18408, "summary"): "keyless join",
    (18543, "base"): "keyless join",
    (18543, "summary"): "keyless join",
    (18576, "base"): "keyless join",
    (18576, "summary"): "keyless join",
    (18617, "base"): None,
    (18617, "summary"): None,
    (18628, "base"): "cannot be related to the query outputs",
    (18628, "summary"): "cannot be related to the query outputs",
    (18629, "base"): None,
    (18629, "summary"): None,
    (18768, "base"): "cannot be related to the query outputs",
    (18768, "summary"): "cannot be related to the query outputs",
    (18809, "base"): None,
    (18809, "summary"): None,
    (18864, "base"): "cannot be related to the query outputs",
    (18864, "summary"): "cannot be related to the query outputs",
    (18960, "base"): "Graph contains a cycle",
    (18960, "summary"): "Graph contains a cycle",
    (19214, "base"): "cannot be related to the query outputs",
    (19214, "summary"): "cannot be related to the query outputs",
    (19226, "base"): "cannot be related to the query outputs",
    (19226, "summary"): "cannot be related to the query outputs",
    (19227, "base"): "keyless join",
    (19227, "summary"): "keyless join",
    (19276, "base"): "keyless join",
    (19276, "summary"): "keyless join",
    (19292, "base"): "cannot be related to the query outputs",
    (19292, "summary"): "cannot be related to the query outputs",
    (19423, "base"): "cannot be related to the query outputs",
    (19423, "summary"): "cannot be related to the query outputs",
    (19498, "base"): "Missing source reference",
    (19498, "summary"): "Missing source reference",
    (19841, "base"): None,
    (19841, "summary"): None,
    (19887, "base"): "keyless join",
    (19887, "summary"): "keyless join",
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
