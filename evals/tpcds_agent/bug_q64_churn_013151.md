# q64 churn — run 20260629-013151 (1.24M tokens)

Agent eventually SUCCEEDED (2 rows, matching canonical) on its final attempt, so the
1.24M tokens were pure churn driven by misleading framework signals, not an
unsolvable query. Canonical `tests/modeling/tpc_ds_duckdb/query64.preql` builds and
passes (`pytest -k sixty_four` → 1 passed). The canonical deliberately uses
`ss.is_returned` for the "has a matching store return" clause and never writes a
correlated return-membership — exactly the construct that traps the agent below.

Three distinct signals drove the churn. Only #1 is a genuine framework bug.

---

## Bug 1 (REAL, NEW) — correlated `=` inside a `?` filter used as a membership RHS leaks an internal `_virt_filter` repr

### Symptom (verbatim, run idx 37)
```
Resolution error: Could not resolve connections for query with output
['local._virt_filter_ticket_number_8426183738541935<Purpose.PROPERTY>Derivation.FILTER>']
from current model.
```
The message names an internal hashed virtual-filter concept — useless to an author.
This is what made the agent abandon the entire "store sale that also has a matching
store return" membership approach and pivot to `is_returned` + rowsets.

### Minimal repro (runs against the run's `workspace/`)
```trilogy
import raw.store_sales as store_sales;
import raw.store_returns as store_returns;
where store_sales.ticket_number in (
    store_returns.ticket_number ? store_returns.ticket_number = store_sales.ticket_number
  )
select store_sales.item.id, count(store_sales.line_item) as sale_lines;
```
→ same `Could not resolve connections ... _virt_filter_ticket_number ...` error.

### Trigger matrix (membership `ss.ticket_number in ( sr.ticket_number ? <cond> )`)
| `<cond>` inside the `?` filter | result |
|---|---|
| non-correlated: `store_returns.return_amount > 0` | OK (18000 rows) |
| **correlated equality: `store_returns.ticket_number = store_sales.ticket_number`** | **ERROR (`_virt_filter` could not resolve)** |
| correlated membership: `store_returns.item.id in (store_sales.item.id)` | OK |
| original agent form (both correlated conds AND-ed) | ERROR (same) |

So the trigger is specifically a **correlated equality** to an outer concept inside the
`?` filter condition. (The condition is in fact redundant — the outer `in` already
joins on `ticket_number` — but the agent had no way to know that from the error.)

### Root cause (file:line)
The `store_sales.ticket_number in ( <filtered sr concept> )` membership mints a virtual
FILTER concept `_virt_filter_ticket_number_<hash>`. Its `? <cond>` correlates the
filter's grain to the *outer* query grain, so the concept is genuinely unsourceable.
`source_query_concepts` then hits the dead-end raise at
`trilogy/core/query_processor.py:689` / `trilogy/core/processing/concept_strategies_v3.py:672-675`.
The guard meant to convert this into a clean author-facing message —
`raise_if_filter_disconnected` at `trilogy/core/processing/discovery_utility.py:986`
— is a **no-op here**: it only fires when `disconnected_components(...)` splits the
surfaced filter-condition concepts into >1 group. store_sales and store_returns are
graph-connected (shared ticket_number/item), so the set does NOT split, the guard
returns, and execution falls through to the generic `UnresolvableQueryException` that
prints the raw internal `<Derivation.FILTER>` address. The correlated-subquery case is
neither handled nor cleanly rejected.

### Classification
Real framework bug — at minimum a poor-error bug (internal concept repr leaked; no
"correlated filter condition not supported / use a non-correlated membership" hint).
NEW: distinct from `bug_q64_invalid_reference_eligible_items_membership.md` (FIXED,
INVALID_REFERENCE_BUG on the catalog eligible-items membership) and from
`bug_q64_self_pair_resolution_churn.md` (cross-import disconnect on the self-pair).
This one is the *store-return* membership with a correlated `?` equality.

---

## Bug 2 (NOT a bug — correct, clear error) — `Ambiguous reference 'store_agg.year'`

### Symptom (run idx 85)
```
Ambiguous reference 'store_agg.year': matches
['store_agg.store_sales.customer.first_sales_date.year',
 'store_agg.store_sales.customer.first_shipto_date.year',
 'store_agg.store_sales.date.year']. Qualify the full path to disambiguate.
```

### Repro / trigger matrix (rowset `store_agg`, then `select store_agg.year`)
| rowset `.year` outputs (unaliased) | `store_agg.year` shorthand |
|---|---|
| three (`date.year`, `first_sales_date.year`, `first_shipto_date.year`) | ERROR — ambiguous (correct) |
| one (`date.year` only) | resolves fine |

The leaf-shorthand is **genuinely** ambiguous — three distinct outputs end in `.year`.
This is NOT the false-ambiguity family of `bug_q02_self_join_ambiguous_week_side.md`
(there the candidates collapsed to one real output). The message is clear and already
tells the author to qualify. The agent just never aliased the three year columns.
Classification: agent error with a correct, actionable message (guidance at most).

---

## Bug 3 (minor UX / churn contributor) — silent `Executing 0 statements` no-op

Runs idx 52, 67, 79 each returned `ok:true, statements:0, rows:0`. Cause: the file
contained ONLY rowset/`with ... ;` definitions and no trailing executable `select`, so
zero statements ran. Repro:
```trilogy
import raw.store_sales as store_sales;
with foo as select store_sales.item.id, count(store_sales.line_item) as cnt;
```
→ "Executing 0 statements..." → Summary `Statements: 0`, exit 0, no warning.

A file of pure definitions reports success with no output and no "no executable query
found" warning, so the agent couldn't tell a broken query from an empty/no-op one.
Classification: minor UX gap (guidance), not a correctness bug. Worth a one-line
"0 executable statements — did you forget a final select?" notice.

### `Undefined concept: catalog_returns` (run idx 43)
Plain agent mistake: referenced the `catalog_returns` namespace in a file that imported
`store_returns`/`catalog_sales` but not `catalog_returns`. Clear error + correct
suggestions. Not a bug.

---

## Summary of classification
- Bug 1 (correlated `?`-filter equality → leaked `_virt_filter` resolve error): REAL, NEW. Do not fix here.
- Bug 2 (`store_agg.year` ambiguous): correct/clear error; agent should alias.
- Bug 3 (silent 0-statement no-op): minor UX gap.
- `Undefined concept: catalog_returns`: agent error, clear message.

Root-cause anchor for Bug 1: `discovery_utility.py:986 raise_if_filter_disconnected`
(no-op when sources are graph-connected) → falls through to
`concept_strategies_v3.py:672` / `query_processor.py:689` generic raise that prints the
internal FILTER concept address.
