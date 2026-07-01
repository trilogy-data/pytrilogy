# BUG: global aggregate over a literal (`count(1)`, `sum(1)`, `count(1) by *`) crashes with IndexError

**Classification:** framework bug — unguarded `IndexError: list index out of range`, surfaced to
the CLI/agent as the opaque `Unexpected error: list index out of range`. Reads as an internal
crash, not a fixable authoring issue. Not silent (it errors), but the message is useless.

**Note on the construct (corrected):** these are all aggregates over a LITERAL that reference no
concept, so there is no source to anchor the count/sum — the query is arguably ill-defined, not
just uncomputed. The docs do NOT endorse this: `constants.py:89` reserves `by *` for aggregating a
real MEASURE to a whole-table total (`sum(x) by *`, `avg(x) by *`), and `constants.py:95`
explicitly says "`sum(1)`/`count(1)` is only meaningful grouped by a grain field (e.g.
`sum(1) by x`)" — i.e. the reference steers AWAY from ungrouped `count(1)`, not toward it. The
idiomatic global row count is `count(<key>)` (verified: resolves fine). So the defect is narrow:
`count(1)`/`sum(1)` with no anchoring concept should give a clean error (or be disallowed), not an
unguarded `IndexError`.

**Status:** root-caused, minimal repro. NOT fixed.

---

## Symptom

Any global aggregate whose only input is a **literal** and which has **no grain key** crashes:

```trilogy
import sales as s;
select count(1) as c;          -- IndexError: list index out of range
select sum(1) as c;            -- IndexError
select count(1) by * as c;     -- IndexError  (the documented global-scalar idiom!)
```

Real-column aggregates and grain-keyed literal aggregates are fine:

```trilogy
select count(s.sid) as c;          -- OK
select count(1) by s.item as c;    -- OK
```

## Minimal repro

Self-contained (synthetic model). Run:

```
.venv/Scripts/python.exe evals/tpcds_agent/repro_global_literal_aggregate_indexerror.py
```

## Trigger matrix

| query | result |
|---|---|
| `count(1)` | IndexError |
| `sum(1)` | IndexError |
| `count(1) by *` | IndexError |
| `count(<key>)` / `sum(<real col>)` | OK |
| `count(1) by <key>` | OK |

Trigger = aggregate over a pure literal (no real-column dependency) with no grain component. Such
a node's query-datasource has an **empty** `datasources` list.

## Root cause

`calculate_effective_parent_grain` (`trilogy/core/processing/discovery_utility.py:58`):

```python
if not qds.joins:
    base = qds.base_datasource
    if base is not None:
        return base.grain
    return qds.datasources[0].grain     # <-- line 72: IndexError when datasources == []
```

A literal-only aggregate produces a `QueryDatasource` with no joins, no `base_datasource`, and an
**empty** `datasources` list (nothing real is scanned). The `[0]` index is unguarded.

Stack: `search_concepts` → `generate_loop_completion` → `group_if_required_v2`
(`discovery_utility.py:260`) → `check_if_group_required` (`:130`) →
`calculate_effective_parent_grain` (`:72`).

## Fix direction

Two layers, and the FIRST is the real question:

1. **Decide the semantics of a sourceless literal aggregate.** `count(1)` / `sum(1)` reference no
   concept, so nothing says which table's rows to count — the query is ill-defined (it would count
   a single synthetic literal row → `1`, not a meaningful total). The idiomatic global row count is
   `count(<key>)`, which already resolves. Preferred fix: raise a **clean author-facing error** at
   validation ("`count(1)`/`sum(1)` needs a grain (`by <dim>`) or a concept to count — for a total
   row count use `count(<key>)`"), rather than silently resolving to a meaningless value.

2. **Regardless, kill the crash.** Even if the decision is to resolve, the bare `[0]` index at
   `discovery_utility.py:72` must be guarded so nothing reaches an unguarded `IndexError`:
   ```python
   if not qds.datasources:
       return BuildGrain()   # only if (1) decides to resolve; otherwise error before here
   ```
   Do NOT ship the guard alone as "the fix" if it makes `count(1)` silently return `1` — that
   trades a loud crash for a silent wrong number. Confirm whatever path is chosen also covers
   `count(1) by *` (same empty-datasources landing).

## Discovered on / impact

TPC-DS **q97** and **q38** in the 20260630-235635 rebaseline, both surfaced as
`Unexpected error ... list index out of range`. q97 recovered on retry; q38 failed. Agents
naturally reach for `count(1)` to sanity-check row counts, so this crash shows up as exploration
churn. (The fix is to teach them / the engine that `count(<key>)` is the row-count idiom.)

## Guardrail / test

Test that `generate_sql("select count(1) as c;")` (and `sum(1)`, `count(1) by *`) no longer raises
`IndexError` — asserting whichever behavior (1) decides: a clean `InvalidSyntaxException`/validation
error, or a correct single-row result. Also assert `count(<key>)` and `count(1) by <key>` still
resolve (the working idioms).

## File pointers

- `trilogy/core/processing/discovery_utility.py:72` — `calculate_effective_parent_grain` (the crash; guard here).
- `trilogy/core/processing/discovery_utility.py:130` — `check_if_group_required` (caller).
- `trilogy/ai/constants.py:89` / `:95` — the `by *` (real-measure) and `sum(1)/count(1)` (grain-field) guidance; already correct, no change needed.
