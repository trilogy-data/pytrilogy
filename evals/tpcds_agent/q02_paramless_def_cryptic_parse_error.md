# Improve the parse error for a paramless `def` macro (q02)

**Status:** FIXED (2026-06-26). The `def` parameter list is now **fully optional**: both
`def name -> body` (no parens) and `def name() -> body` (empty parens) parse as valid zero-arg
macros, invoked `@name()`. Grammar change: `("(" function_binding_list? ")")?` in both
`raw_function` rules (lark + pest; pest rebuilt via `maturin develop --release`); the
`raw_function` hydrator handles the no-binding-list case. No error code is needed — the form just
works now. Tests: `tests/complex/test_paramless_def.py`. (An earlier iteration emitted a
`Syntax [223]` error for the no-parens form; superseded by treating it as `def()`.)

---
**Original report (for context):**
**Surfaced by:** TPC-DS q02 enriched eval — the agent wrote `def sun_sales -> sum(...)` (a no-arg
named expression) and got a cryptic whole-file rejection.
**Severity:** Low-Medium (agent-efficiency / error clarity).

## Symptom

A `def` macro with **no parameters** fails to parse with an unhelpful message:

```trilogy
def sun_sales -> sum(sales.ext_sales_price ? sales.date.day_of_week = 0);
```
→
```
Parse error: --> 9:1
9 | def sun_sales -> sum(sales.ext_sales_price ? sales.date.day_of_week = 0);
  | ^---
  = expected EOI, block, or show_statement
```

The message gives no hint that the real issue is "`def` requires ≥1 parameter."

## Confirmed behavior

| Form | Result |
|---|---|
| `def sun_sales -> sum(...)` (no params, no parens) | **parse error** at the `def` |
| `def sun_sales() -> sum(...)` (empty parens) | **parse error** (fails further in, at `->`) |
| `def sun_sales(d) -> sum(... = d)` (≥1 param) | OK |
| `auto sun_sales <- sum(...)` (paramless named expr) | OK |

So `def` is strictly a *parameterized* macro; a no-arg named expression is what `auto` is for.

## Suggested fix

When a `def <name>` is not followed by `(<params>)`, or has empty `()`, emit a targeted error in
`trilogy/parsing/v2/errors.py` (alongside the existing GROUP BY / FROM / join hints):

> `def` defines a *parameterized* macro and needs at least one parameter, e.g.
> `def day_sales(d) -> sum(x ? dow = d)` called as `@day_sales(0)`. For a named expression with no
> parameters, use `auto`: `auto sun_sales <- sum(x ? dow = 0)`.

That single hint converts a dead-end ("whole file rejected, no idea why") into a one-line fix and
points the agent at the right tool (`auto`).

## Note

Not a planner bug — requiring a parameter on `def` is a reasonable design choice (`auto` covers the
paramless case). This is purely about surfacing an actionable message instead of
`expected EOI, block, or show_statement`.

## Why this one matters extra: it masked a clearer error

The same q02 file *also* had a second, independent error further down — a SQL-style subselect in
HAVING (`wk.week_seq in (select s.date.week_seq where s.date.year = 2001)`). That one has a **good**
message: `Syntax [102]: Trilogy does not support SQL-style subqueries`, which would have pointed the
agent at the membership idiom (`auto ws_2001 <- s.date.week_seq ? s.date.year = 2001; having
wk.week_seq in ws_2001`). But the parser stops at the **first** error — the paramless `def` on line
9 — so the agent only ever saw the cryptic def message and never reached the clear subselect one.
Fixing the def message has outsized value here: it's the gate that hides a well-messaged error
behind it.


## Repro

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-125555/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql('import raw.all_sales as s;\n'
                 'def sun_sales -> sum(s.ext_sales_price ? s.date.day_of_week=0);\n'
                 'select s.date.week_seq, @sun_sales as x limit 3;')   # parse error
```
