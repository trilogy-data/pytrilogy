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
FOUND_SEEDS: list[int] = []
# leads, failing on main too (docs/handoff_formalization_next.md, area 4):
# what the planner raises or DuckDB rejects, or None for wrong rows
OPEN: dict[tuple[int, str], str | None] = {}


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
