# Bug: `year(<integer date surrogate key>)` raises an uncaught `TypeError` (surfaces as "Unexpected error" not a clean syntax error) (q22)

**Status:** FIXED 2026-06-27 — function arg-type validation now raises
`FunctionArgumentException` (a `TypeError` subclass, so existing `except TypeError` handlers/tests
keep working) instead of a bare `TypeError`; the CLI harness (`handle_execution_exception`) routes it
to a clean `Type error:` label (NOT `Syntax error`, NOT `Unexpected error`). No helpful-hint nudge
added (per request). `year()` still rejects integer args. Test:
`tests/scripts/test_trilogy.py::test_function_argument_type_reported_as_type_error`.
Was: OPEN — deterministic minimal repro. Surfaced in the full-99 rebaseline.
**Surfaced by:** TPC-DS q22 (run `20260627-181845`). Thrown during **`generate_sql`** as a raw
`TypeError` → the agent harness wraps it as `Unexpected error` (the catch-all), so it reads as a
crash rather than a clean author error.
**Severity:** LOW/MED — the **message text is already informative**; the bug is the **exception
class**. A typed function-arg mismatch should be a clean `InvalidSyntaxException`/author error so the
harness reports it as a `Syntax error`, not an uncaught crash.

## Symptom

```
TypeError: Invalid argument type 'INTEGER' passed into YEAR function in position 1
from concept: inventory.date.id. Valid: 'DATE', 'DATETIME', 'TIMESTAMP'.
```

## Trigger (bisected; q22 as written)

The agent reached for `inventory.date.id` — the **integer surrogate key** of the date dimension —
and called `year()` on it, instead of `inventory.date.date` (the actual DATE column):

```trilogy
import raw.inventory as inventory;
WHERE year(inventory.date.id) = 2000          -- date.id is INTEGER, not DATE
SELECT inventory.item.product_name, avg(inventory.quantity_on_hand) as avg_qty_on_hand
BY ROLLUP (inventory.item.product_name)
ORDER BY avg_qty_on_hand ASC NULLS FIRST
LIMIT 100;
```

## Likely fix area

The function arg-type validation that builds this message raises a bare `TypeError`. Route it through
the same clean-error path the rest of the author-time validators use (e.g. `InvalidSyntaxException`)
so it's caught and reported as a syntax/validation error. Bonus: the message could nudge toward the
DATE-typed sibling concept — here `inventory.date.date` — since the integer-`.id`-vs-`.date` mixup is
a recurring agent trap.

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-181845/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
q = '''import raw.inventory as inventory;
WHERE year(inventory.date.id) = 2000
SELECT inventory.item.product_name, avg(inventory.quantity_on_hand) as avg_qty_on_hand
BY ROLLUP (inventory.item.product_name)
ORDER BY avg_qty_on_hand ASC NULLS FIRST
LIMIT 100;'''
eng.generate_sql(q)   # raises bare TypeError (should be a clean InvalidSyntaxException)
```
