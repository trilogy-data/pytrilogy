# q44: bare AssertionError from inline-datasource fold, rendered as an EMPTY "Unexpected error"

Two framework defects stacked. The first is a planner/optimizer crash (a bare
`AssertionError` with no message); the second is the CLI error path printing
`str(exc)` of that message-less exception, handing the agent literally zero
signal. In run `results/20260820-031800_ingest_deepseek_deepseek-v4-flash`,
the q44 agent hit the empty error SIX times, retried blind each time, and
burned 769,996 tokens (735k prompt / 35k completion) before escaping by
rewriting the query with `then where` pipelining instead of `having`.

## Symptom (what the agent saw)

```json
{"event": "error", "message": "Unexpected error in probe_374591292.preql:"}
```

Exit code 1, nothing on stderr, nothing after the colon. No exception type, no
hint whether the query or the engine was at fault, so the model treated it as
a transient crash and re-issued near-identical probes.

The run also produced one genuine authored-error (`Output column
'bench_profit' renames 'local.bench_profit' back to the name of an existing
concept 'bench_profit'`); that one carried a real message and is not part of
this report.

## Minimal repro (3 statements, reproduces on the run's own workspace copy)

```preql
import root.store_sales as ss;
auto bench <- avg(ss.net_profit ? ss.customer_address.address_sk is null) by *;
select ss.store.store_sk as d, bench as v;
```

Engine: `AssertionError` (empty `str`) at `trilogy/core/models/execute.py:1702`.
CLI (`trilogy file write probe.preql --run` from the workspace):
`Unexpected error in probe.preql: ` and exit 1.

All six probe bodies from `agent_log.q44.jsonl` (events 23, 29, 32, 35, 44,
47) fail at the same assert. The rowset/`having` variants fail through the
same mechanism with the `item` scan instead of the `store` scan (join pairs
there are `all_rows = all_rows` plus a real item key, and the inlined parent
is the `item` DatasourceCTE), so this is one bug category, not two.

## Trigger matrix (harness on the run's workspace, current tree)

Shape: `auto bench <- avg(ss.net_profit ? <filter>) by <grain>; select <dim> as d, bench as v;`

| filter | grain | co-selected dim | outputs aliased | result |
|---|---|---|---|---|
| addr_sk is null | * | store.store_sk | both | CRASH |
| addr_sk is not null | * | store.store_sk | both | CRASH |
| addr_sk is null | * | date_dim.date_sk | both | CRASH |
| addr_sk is null | * | promotion.promo_sk | both | CRASH |
| addr_sk is null | * | store.store_sk + store_name | both | CRASH |
| addr_sk is null | * | store.store_sk | dim only | ok |
| addr_sk is null | * | store.store_sk | agg only | ok |
| addr_sk is null | * | store.store_sk | none | ok |
| addr_sk is null | * | item.item_sk | both | ok (item FK is mandatory; no separate dim scan) |
| addr_sk is null | * | customer.customer_sk | both | ok (customer carries its own address FK; join uses a real key) |
| addr_sk is null | * | customer_address.address_sk | both | ok (same dim as the filter) |
| addr_sk is null | * | store.store_name (attr only) | both | ok |
| addr_sk is null | * | ss.ticket_number (local key) | both | ok |
| net_profit > 0 (local) | * | store.store_sk | both | ok |
| store.store_sk = 1 (FK equality) | * | store.store_sk | both | ok |
| hdemo.demo_sk is null (`?` FK, not `~?`) | * | store.store_sk | both | ok |
| addr_sk is null | ss.store.store_sk | store.store_sk | both | ok (non-global grain) |
| addr_sk is null | * | store.store_sk, inline agg instead of `auto` | both | ok |
| addr_sk is null | * | store.store_sk, plus `where store_sk = 1` | both | ok |

The alias/dim/filter axes are not the mechanism, they are just what steers the
planner into the vulnerable plan shape: a standalone dimension scan CTE
broadcast-joined (`__preql_internal.all_rows = __preql_internal.all_rows`) to
the global filtered-aggregate CTE, with the scan eligible for inlining.

## Root cause, half (a): InlineDatasource folds away the only producer of a broadcast join key

Plan for the minimal repro: consumer CTE has two parents, a `store`
DatasourceCTE (scan, outputs `[ss.store.store_sk, local.d,
__preql_internal.all_rows]` where `all_rows` is a synthesized constant) and
the global-aggregate CTE, INNER joined on the pair
`__preql_internal.all_rows = __preql_internal.all_rows` whose left leg reads
from the scan.

`InlineDatasource.optimize` decides the scan is foldable at
`trilogy/core/optimizations/inline_datasource.py:145-172`. Its demand set is

```python
inherited = {x for x, v in cte.source_map.items() if v and parent_cte.name in v}
```

(`inline_datasource.py:149-151`, re-checked at apply time `:201-203`). Two
gaps combine:

1. The consumer's `source_map` attributes `__preql_internal.all_rows` to the
   aggregate CTE only (observed: `__preql_internal.all_rows -> ['wakeful']`),
   even though the join's `CTEConceptPair.cte` reads the left leg from the
   scan. Join-key demand per parent is never consulted.
2. Constants routinely carry empty source lists, and the `if v` filter drops
   them from `inherited` entirely.

So `inherited ⊆ root_outputs` passes, the scan is folded into
`inlined_parents`, and the raw `store` table obviously has no
`__preql_internal.all_rows` column.

The crash then fires on the first post-fold render, which happens to be
HideUnusedConcept's used-map probe:
`trilogy/core/optimizations/hide_unused_concept.py:108 -> :40` ->
`trilogy/core/optimizations/utils.py:27` (`render_cte_used_map`) ->
`trilogy/dialect/base.py:3076` (`render_cte`) ->
`trilogy/dialect/common.py:363/317/220` (join key rendering) ->
`trilogy/core/models/execute.py:900` (`CTE.column_for`, inlined branch) ->
`DatasourceCTE.consumer_column` `trilogy/core/models/execute.py:1702`:

```python
assert alias is not None  # concept is an output of this datasource
```

Final rendering would hit the same assert even without HideUnusedConcept; the
optimizer probe is just first in line. Secondary observation: the fallback
loop at `execute.py:1695-1701` recovers non-raw outputs only via
`output.address in concept.pseudonyms`; `all_rows` IS an output of the folded
CTE with a renderable constant `BuildFunction` lineage, but an exact-address
match is never tried, so the recovery path that saves cross-namespace merges
cannot save it. Per the standing placement rule, the real defect is the
rule's eligibility check (post-multi-node state, so it belongs in the
optimizer), and the assert should at minimum carry a message.

## Root cause, half (b): the CLI renders `str(exc)` of a message-less exception

Call chain for `trilogy file write probe.preql --run`:
`trilogy/scripts/file.py:510` (`_run_written_file`, `ctx.invoke(run_command)`)
-> run command -> `trilogy/scripts/parallel_execution.py:654-655`
(`handle_execution_exception(e, debug=debug, source=source)`) ->
`trilogy/scripts/common.py:944` (`handle_execution_exception`). The function
special-cases a dozen known exception types, then falls through at
`trilogy/scripts/common.py:1015`:

```python
print_error(f"Unexpected error{location}: {e}")
```

For a bare `AssertionError()`, `str(e)` is `""`, so the JSON error event
(`trilogy/scripts/display_core.py:358` `print_error` -> `_emit_or_style`)
ends at the colon. Nothing appends the exception class name, and the
traceback is printed only under `debug` (`common.py:1017-1018`). Any
message-less exception (bare assert, bare `KeyError`-style raises) reaches
the agent as pure silence; the function already documents the same
readability hazard for `RecursionError` (`common.py:999-1008`) but has no
generic floor such as including `type(e).__name__` when `str(e)` is empty.

## Repro assets

Workspace copy + harness + all matrix cases:
`scratchpad/probe_q44/` (harness.py, cases.py, instrument*.py, probe_bodies.json
with the six exact probe bodies recovered from the agent log).

## Relation to existing reports

Not a duplicate of anything in the stack rank: the closed keyless-join /
q30 NULL-key reports were discovery/demand-stage defects; this one is an
optimizer eligibility gap plus an error-surfacing gap. The empty-message half
is new; no prior bug_*.md covers CLI rendering of message-less exceptions.
