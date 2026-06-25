# Bug: unary minus on an expression (`-sum(x)`, `-col`) is unsupported, with an unhelpful error (q05)

**Status:** FIXED 2026-06-25 (grammar option 1). `-expr` now parses on both backends
as `expr * -1`; precedence is `(-a) + b`. Negative numeric literals (`-1`, `-1.5`) are
unaffected — a `UNARY_MINUS` terminal with negative lookahead `/-(?![0-9.>])/` only fires
when `-` isn't glued to a number. Tests: tests/complex/test_unary_minus_expression.py.

**Status (original):** OPEN — confirmed, deterministic.
**Surfaced by:** TPC-DS q05 enriched eval — agent wanted to negate a returns aggregate
(`-sum(return_net_loss)`) and burned turns finding a workaround.
**Severity:** Medium (ergonomic grammar gap + unhelpful parse error). Not blocking — workarounds
exist — but it's friction and the error gives no hint.

## Symptom

Prefixing `-` onto an expression (a function call or a column reference) fails to parse:

```trilogy
select s.channel, -sum(s.net_profit) as m;
                  ^--- expected select_item, limit, order_by, where, having, or JOIN_TYPE
```

```trilogy
select s.channel, sum(-s.net_profit) as m;   -- unary minus on a column ALSO fails
```

A negative numeric **literal** (`-1`) is fine (lexed as a number); only unary minus applied to an
**expression** is unsupported. The grammar has no prefix unary-minus operator for expressions.

## What works (confirmed)

| Form | Result |
|---|---|
| `-sum(x)` | **ERR** (InvalidSyntaxException) |
| `-s.col` (unary minus on a column) | **ERR** |
| `sum(x) * -1` | OK |
| `0 - sum(x)` | OK |
| `-1 * sum(x)` | OK |

## Impact

The error message ("expected select_item, limit, order_by, ...") gives no hint that the problem is
the leading `-` or that `0 - expr` / `expr * -1` is the workaround. The q05 agent eventually found
`sum(x) * -1` on its own, but only after several rewrites.

## Suggested fix (pick one)

1. **Grammar (better):** add a prefix unary-minus production for expressions so `-expr` parses as
   `negate(expr)`. Touches both backends (`trilogy.lark` + `trilogy.pest`) and the round-trip/render
   paths — keep the two grammars in sync (see `reference_parser_backends`).
2. **Error message (cheap):** when a `-` is seen where a `select_item`/expression is expected,
   emit a targeted hint: *"unary minus on an expression isn't supported; write `0 - expr` or
   `expr * -1` (a negative literal like `-1` is fine)."* — in `trilogy/parsing/v2/errors.py`,
   alongside the existing GROUP BY / FROM / join hints.

## Repro

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260625-164230/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql('import raw.all_sales as s;\nselect s.channel, -sum(s.net_profit) as m;')  # ERR
```
