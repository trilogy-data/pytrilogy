"""Parse cost grows linearly with expression nesting.

pest has no memoization, so an alternation whose second branch re-reads what
the first one read parses that text twice, and when the text is a nested
expression the doubling compounds at every level: three such alternations in
the grammar (`expr = between_comparison | comparison`, the `X ? cond` form of
`filter_item` tried first in `_atom`, `window_item_sql_aggregate` tried
before `aggregate_functions`) made one `def` line of a predictions model take
7 s to parse. Each is now a left-factored rule the adapter renames.

The measure is pest rule calls (`parse_trilogy_syntax_calls`), which is
deterministic, so the bounds are tight; only the motivating line is also
timed. `rule_calls` in the parser crate is the same measure for `cargo test`.
"""

from __future__ import annotations

from collections.abc import Callable
from time import perf_counter

import pytest
from _preql_import_resolver import parse_trilogy_syntax_calls

from trilogy import Environment

MAX_CALLS = 5_000_000

# Each wraps the expression one level deeper; `x` is the leaf.
LINEAR_SHAPES: dict[str, Callable[[str], str]] = {
    "call": lambda e: f"coalesce({e}, 1)",
    "custom_call": lambda e: f"@f({e}, 1)",
    "aggregate": lambda e: f"sum({e})",
    "aggregate_by": lambda e: f"(avg({e}) by grp)",
    "aggregate_over": lambda e: f"sum({e}) over (partition by g)",
    "filter": lambda e: f"sum({e} ? y > 1)",
    "in_list": lambda e: f"coalesce({e} in (1, 2, 3), true)",
    "comparison": lambda e: f"coalesce({e} = 1, true)",
    "between": lambda e: f"coalesce({e} between 1 and 2, true)",
    "case": lambda e: f"case when y > 1 then {e} else 0 end",
    "cast": lambda e: f"cast({e} as int)",
    "attribute": lambda e: f"coalesce({e}, 1).a",
    "legacy_window": lambda e: f"rank {e} by y",
    "not": lambda e: f"not {e}",
    "negate": lambda e: f"-({e})",
}
# In a condition position (`where`, a derivation body) a parenthesis is read
# as a parenthesized condition first and as an expression only when that
# fails on the text after `)`, so each paren level costs the levels below it
# twice: quadratic, where doubling the depth at most quadruples the calls.
# The bound keeps that from becoming exponential. `not (...)` is a condition
# only, so it has no expression-position row.
QUADRATIC_SHAPES: dict[str, Callable[[str], str]] = {
    "paren_arith": lambda e: f"({e} + 1)",
    "tuple": lambda e: f"({e}, 1)",
    "paren_filter": lambda e: f"(({e}) ? y > 1)",
}
NOT_PAREN: Callable[[str], str] = lambda e: f"not ({e} and y)"
CONTEXTS: dict[str, Callable[[str], str]] = {
    "derivation": lambda e: f"const x <- 1;\nauto y <- {e};",
    "where": lambda e: f"const x <- 1;\nselect x where {e} = 1;",
    "select": lambda e: f"const x <- 1;\nselect {e} -> y;",
}


def nested(
    shape: Callable[[str], str], depth: int, context: Callable[[str], str]
) -> str:
    expression = "x"
    for _ in range(depth):
        expression = shape(expression)
    return context(expression)


def calls(text: str) -> int:
    return parse_trilogy_syntax_calls(text, MAX_CALLS)


@pytest.mark.parametrize("context", CONTEXTS)
@pytest.mark.parametrize("shape", LINEAR_SHAPES)
def test_nesting_cost_is_linear(shape: str, context: str) -> None:
    shallow = calls(nested(LINEAR_SHAPES[shape], 8, CONTEXTS[context]))
    deep = calls(nested(LINEAR_SHAPES[shape], 16, CONTEXTS[context]))
    assert deep <= 2.5 * shallow, (shallow, deep)


@pytest.mark.parametrize(
    "shape, context",
    [
        (shape, context)
        for shape in QUADRATIC_SHAPES
        for context in ("derivation", "where")
    ]
    + [("not_paren", "derivation")],
)
def test_paren_in_condition_cost_is_at_most_quadratic(shape: str, context: str) -> None:
    wrap = QUADRATIC_SHAPES.get(shape, NOT_PAREN)
    shallow = calls(nested(wrap, 8, CONTEXTS[context]))
    deep = calls(nested(wrap, 16, CONTEXTS[context]))
    assert deep <= 5 * shallow, (shallow, deep)


@pytest.mark.parametrize("shape", QUADRATIC_SHAPES)
def test_paren_in_expression_cost_is_linear(shape: str) -> None:
    shallow = calls(nested(QUADRATIC_SHAPES[shape], 8, CONTEXTS["select"]))
    deep = calls(nested(QUADRATIC_SHAPES[shape], 16, CONTEXTS["select"]))
    assert deep <= 2.5 * shallow, (shallow, deep)


# The line that took 7 s: aggregates under `by`, parenthesized arithmetic and
# a case, four calls deep.
FIT_LINE = (
    "def fit_s(x, y, b, n, grp) -> case when n > 2 then sqrt(greatest("
    "n / (n - 2) * ((avg(y * y) by grp) - (avg(y) by grp) ** 2 - b * "
    "((avg(x * y) by grp) - (avg(x) by grp) * (avg(y) by grp))), 0.0)) "
    "else null end;"
)


def test_the_motivating_def_line_parses_in_one_pass() -> None:
    # 10,586 calls for 210 characters when this was written; a second read
    # of the arguments at any level puts it in the hundreds of thousands.
    assert calls(FIT_LINE) < 30_000
    start = perf_counter()
    Environment().parse(FIT_LINE)
    assert perf_counter() - start < 2.0


# Absolute budgets on representative statements, at 1.5x the count when
# written, so a constant-factor regression (a statement read twice at the
# top level, say) is seen too. Re-record a budget only with the reason the
# grammar now needs the calls.
BUDGETS: dict[str, tuple[str, int]] = {
    "select": (
        (
            "select order.id, sum(order.amount) -> total, count(order.id) by customer.id -> orders "
            "where order.date between '2020-01-01' and '2020-12-31' and customer.region = 'west' "
            "order by total desc limit 10;"
        ),
        6_500,  # 4,213 when written
    ),
    "multi_select": (
        "select a.x -> k, sum(a.v) -> v merge select b.y -> k, sum(b.w) -> w align k: k, k;",
        5_500,  # 3,607 when written
    ),
    "datasource": (
        (
            "datasource orders (id: order.id, amount: order.amount, customer_id: customer.id) "
            "grain (order.id) address orders where order.amount > 0;"
        ),
        2_400,  # 1,539 when written
    ),
    "derivation": (
        "auto margin <- case when revenue is null then null else (revenue - cost) / nullif(revenue, 0) end;",
        5_600,  # 3,729 when written
    ),
}


@pytest.mark.parametrize("name", BUDGETS)
def test_statement_call_budget(name: str) -> None:
    text, budget = BUDGETS[name]
    assert calls(text) <= budget, calls(text)
