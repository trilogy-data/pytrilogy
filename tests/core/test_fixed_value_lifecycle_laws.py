"""`formal/Trilogy/SelectGrain.lean` "An aggregate of a fixed value", and the
rest of a fixed aggregate's life after it is selected. `s <- agg(x) by K` is a
value of `K` (`SelectGrain.fixed_on_keys`), so wherever it goes it keeps that
value and is read once per key:

- filtered: a WHERE reads it over every row, a HAVING over the projected tuple
  (`Staged.lean`), and a rowset holds the distinct tuples, so an outer select
  groups by its member (`SelectGrain.rowset_member_dropped_totals`);
- aggregated: `agg(s)`, and `agg` of any scalar of it in any spelling (`s + 0`,
  `t <- s`, inline), reads it once per distinct `K` in each select tuple
  (`SelectGrain.ofValue`, `SelectGrain.scalar_of_fixed_on_keys`), never once
  per row (`SelectGrain.fixed_value_per_row_double_counts`);
- persisted: a table storing it beside a finer grain repeats the value, so a
  read back is the value, not a partial to re-sum, and a stored partial beside
  it still answers the totals.

Every case runs on DuckDB, SQLite and ClickHouse (persistence on DuckDB) and must agree
with the reference."""

import itertools
from collections import defaultdict
from typing import Any

import pytest

from tests.core.test_fixed_aggregate_select_laws import (
    FIXED,
    MEASURE_OPERAND,
    MEASURE_SQL,
    Rows,
    aggregate_value,
    fixture,
    group_rows,
    make_rows,
)
from tests.windows.test_window_laws import DIALECTS, new_executor, seeds_for
from trilogy import Dialects, Environment
from trilogy.core.enums import ValidationScope
from trilogy.core.validation.environment import validate_environment

SEEDS = range(2)
DIMS: tuple[tuple[str, ...], ...] = ((), ("g",), ("h",), ("g", "h"))
FILTERS = ("where_s", "having_s", "where_dim", "where_s_having_m")
WRAPS = ("plain", "rowset", "rowset_drop", "rowset_where")
OF_VALUE = ("sum", "count", "max", "avg", "count_distinct")
# spelling -> (definitions, expression); `{F}` is the fixed aggregate itself
SPELLINGS = {
    "direct": ("", "s"),
    "alias": ("auto t <- s;", "t"),
    "named_scalar": ("auto t <- s + 0;", "t"),
    "inline_scalar": ("", "(s + 0)"),
    "inline": ("", "({F})"),
}


def svalued(rows: Rows, fixed: str) -> Rows:
    _, fn, operand, keys = FIXED[fixed]
    per_key = {
        k: aggregate_value(fn, [r[operand] for r in m])
        for k, m in group_rows(rows, keys).items()
    }
    return [{**r, "s": per_key[tuple(r[c] for c in keys)]} for r in rows]


def gt(value: Any, bound: int) -> bool:
    return value is not None and value > bound


def normalized(rows: list) -> list[tuple]:
    return sorted(
        (
            tuple(round(v, 6) if isinstance(v, float) else v for v in row)
            for row in rows
        ),
        key=repr,
    )


def run(dialect: Dialects, rows: Rows, text: str) -> list[tuple]:
    executor = new_executor(dialect)
    return normalized(executor.execute_text(fixture(rows) + text)[-1].fetchall())


def filtered_reference(
    rows: Rows, fixed: str, dims: tuple[str, ...], measure: str | None, filt: str
) -> list[tuple]:
    """A WHERE over `s` reads it over every row; the projected `s` and the
    measure are computed over the rows the WHERE keeps; a HAVING reads the
    projected tuple."""
    kept = rows
    if filt in ("where_s", "where_s_having_m"):
        kept = [r for r, v in zip(rows, svalued(rows, fixed)) if gt(v["s"], 2)]
    elif filt == "where_dim":
        kept = [r for r in rows if r["h"] == "Q" or r["g"] == "a"]
    out = []
    for t, members in group_rows(svalued(kept, fixed), [*dims, "s"]).items():
        m = None
        if measure:
            m = aggregate_value(measure, [r[MEASURE_OPERAND[measure]] for r in members])
            t = (*t, m)
        if filt == "having_s" and not gt(t[len(dims)], 2):
            continue
        if filt == "where_s_having_m" and measure and not gt(m, 1):
            continue
        out.append(t)
    return out


def wrapped_reference(inner: list[tuple], dims: tuple[str, ...], wrap: str) -> list:
    if wrap == "rowset_drop":
        return list(dict.fromkeys(t[len(dims) :] for t in inner))
    if wrap == "rowset_where":
        return [t for t in inner if gt(t[len(dims)], 3)]
    return inner


def filtered_query(
    fixed: str,
    dims: tuple[str, ...],
    measure: str | None,
    filt: str,
    wrap: str,
    inline: bool,
) -> str:
    spelled = FIXED[fixed][0]
    value = f"{spelled} as s" if inline else "s"
    outputs = [*dims, value] + ([MEASURE_SQL[measure]] if measure else [])
    where_value = spelled if inline else "s"
    clause = {
        "where_s": f" where {where_value} > 2",
        "having_s": " having s > 2",
        "where_dim": " where h = 'Q' or g = 'a'",
        "where_s_having_m": f" where {where_value} > 2"
        + (" having m > 1" if measure else ""),
    }[filt]
    prefix = "" if inline else f"auto s <- {spelled}; "
    inner = f"select {', '.join(outputs)}{clause}"
    if wrap == "plain":
        return f"{prefix}{inner};"
    names = [*dims, "s"] + (["m"] if measure else [])
    kept = names[len(dims) :] if wrap == "rowset_drop" else names
    outer = f"select {', '.join(f'r.{n}' for n in kept)}"
    if wrap == "rowset_where":
        outer += " where r.s > 3"
    return f"{prefix}rowset r <- {inner}; {outer};"


FILTER_CASES = [
    (fixed, dims, measure, filt, wrap, inline)
    for fixed, dims, measure, filt, wrap in itertools.product(
        FIXED, DIMS, (None, "count", "sum"), FILTERS, WRAPS
    )
    for inline in ((False, True) if wrap == "plain" else (False,))
]


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize(
    "fixed, dims, measure, filt, wrap, inline", FILTER_CASES, ids=repr
)
def test_filtered_and_wrapped_value(
    dialect: Dialects,
    fixed: str,
    dims: tuple[str, ...],
    measure: str | None,
    filt: str,
    wrap: str,
    inline: bool,
) -> None:
    text = filtered_query(fixed, dims, measure, filt, wrap, inline)
    for seed in seeds_for(dialect, SEEDS):
        rows = make_rows(seed)
        inner = filtered_reference(rows, fixed, dims, measure, filt)
        expected = normalized(wrapped_reference(inner, dims, wrap))
        assert run(dialect, rows, text) == expected, (seed, text)


def of_value(fn: str, values: list[Any]) -> Any:
    present = [v for v in values if v is not None]
    if fn == "count_distinct":
        return len(set(present))
    if fn == "avg":
        return sum(present) / len(present) if present else None
    return aggregate_value(fn, values)


def of_value_reference(
    rows: Rows, fixed: str, dims: tuple[str, ...], fn: str
) -> list[tuple]:
    """`SelectGrain.ofValue`: the value once per distinct key in each tuple."""
    keys = FIXED[fixed][3]
    per_key = {tuple(r[c] for c in keys): r["s"] for r in svalued(rows, fixed)}
    seen = {(tuple(r[c] for c in dims), tuple(r[c] for c in keys)) for r in rows}
    values: dict[tuple, list] = defaultdict(list)
    for d, k in seen:
        values[d].append(per_key[k])
    return normalized([(*d, of_value(fn, v)) for d, v in values.items()])


OF_VALUE_CASES = list(itertools.product(FIXED, DIMS, OF_VALUE, SPELLINGS))


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fixed, dims, fn, spelling", OF_VALUE_CASES, ids=repr)
def test_aggregate_of_value(
    dialect: Dialects, fixed: str, dims: tuple[str, ...], fn: str, spelling: str
) -> None:
    definitions, expression = SPELLINGS[spelling]
    expression = expression.replace("{F}", FIXED[fixed][0])
    outputs = ", ".join([*dims, f"{fn}({expression}) as m"])
    text = f"auto s <- {FIXED[fixed][0]}; {definitions} select {outputs};"
    for seed in seeds_for(dialect, SEEDS):
        rows = make_rows(seed)
        expected = of_value_reference(rows, fixed, dims, fn)
        assert run(dialect, rows, text) == expected, (seed, text)


@pytest.mark.parametrize("dialect", DIALECTS, ids=lambda d: d.value)
@pytest.mark.parametrize("fixed", FIXED)
def test_rowset_member_is_outer_key(dialect: Dialects, fixed: str) -> None:
    """`rowset r <- select s as v` holds the distinct values; an outer count
    of the member is per value, 0 for NULL."""
    text = (
        f"auto s <- {FIXED[fixed][0]}; rowset r <- select s as v; "
        "select r.v, count(r.v) as m;"
    )
    for seed in seeds_for(dialect, SEEDS):
        rows = make_rows(seed)
        values = {r["s"] for r in svalued(rows, fixed)}
        expected = normalized([(v, 0 if v is None else 1) for v in values])
        assert run(dialect, rows, text) == expected, (seed, text)


PERSISTED = (
    "{d}, s",
    "{d}, s, count(id) as c",
    "{d}, s, sum(y) as c",
    "s, count(id) as c",
)
READS = (
    "select {d}, s;",
    "select {d}, s, count(id) as m;",
    "select {d}, count(id) as m;",
    "select {d}, sum(y) as m;",
    "select s, count(id) as m;",
    "select h, sum(s) as m;",
    "select g, max(s) as m;",
    "select {d}, s where s > 2;",
    "select count(id) as m;",
    "select sum(y) as m;",
)


@pytest.mark.parametrize(
    "fixed, d, persisted",
    list(itertools.product(FIXED, ("g", "h", "g, h", "id"), PERSISTED)),
    ids=repr,
)
def test_persisted_value_reads_back(fixed: str, d: str, persisted: str) -> None:
    """Persisting a select of the value changes no answer, and the table it
    writes passes validation."""
    reads = [r.format(d=d) for r in READS]
    for seed in SEEDS:
        executor = Dialects.DUCK_DB.default_executor(environment=Environment())
        executor.parse_text(fixture(make_rows(seed)) + f"auto s <- {FIXED[fixed][0]};")
        before = [normalized(executor.execute_text(q)[-1].fetchall()) for q in reads]
        executor.execute_text(
            f"persist p into p_table from select {persisted.format(d=d)};"
        )
        after = [normalized(executor.execute_text(q)[-1].fetchall()) for q in reads]
        assert after == before, (seed, persisted)
        results = validate_environment(
            executor.environment,
            ValidationScope.DATASOURCES,
            exec=executor,
            generate_only=True,
        )
        assert not [r for r in results if r.result], (seed, persisted)
