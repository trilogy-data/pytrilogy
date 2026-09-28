# Plan debugger

A step-through viewer for one statement's discovery, built to read the keyspace
implementation in human form (`docs/keyspace_phase_plan.md`, "Visual debugger").
The planner records each phase into a JSON trace
(`trilogy/core/processing/plan_trace.py`); `viewer.html` renders the trace and
lets you step through it.

```bash
# record the last SELECT of a file, write <stem>.trace.json + <stem>.trace.html, open it
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py local_scripts/plan_debugger/examples/customers_orders.preql --rows --open

# an ad-hoc statement over a model file
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py --model m.preql --text "select name, count(order_id) as n;"

# a model whose tables come from a fixture: seed them before --rows executes
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py tests/modeling/thelook_duckdb/adhoc04.preql --rows --setup tests.modeling.thelook_duckdb.db_build:seed

# a specific SELECT (0-based) and another dialect
.venv/Scripts/python.exe local_scripts/plan_debugger/trace_query.py q.preql --index 2 --dialect bigquery

# any process_query, anywhere (tests included): one trace file per top-level statement
TRILOGY_PLAN_TRACE=plan.json .venv/Scripts/python.exe -m pytest tests/engine/test_derived_key_domain.py -k "some_case"
```

`*.trace.html` is the viewer with the trace embedded; it opens from disk with
no server. `viewer.html` alone loads any trace through its file picker, by drag
and drop, or with `?trace=<url>` when served.

## What is recorded, in order

| phase | one step per | shows |
|---|---|---|
| `request` | plan | the requested concepts (purpose, derivation, keys, lineage), the WHERE, the span scope entering the plan, every datasource with its `~`/`?` columns |
| `concept_graph` | plan | the concept DAG: nodes colored by depth label, edges by kind (lineage / constraint / existence / relation) |
| `keyspace` | plan | the regions (present keys, spans, own rows, witnesses, reach, completions, emptied-by), the outputs × regions matrix (defined / carried / absent), demanded and in-play spans, families, span reach |
| `grouping` | grouping pass | the buckets after assignment, after the dimension split, after the region domain buckets, after d1 roots; each card diffed against the previous pass (added / changed / removed, member-level); then the condition placements (atom, hosts, reason) |
| `group_graph` | group-graph pass | the group DAG after materialization, after FINAL + concept sets + regraft, and after conditions + phases + contracts + extent owners; nodes and edges diffed against the previous pass; region domains outlined |
| `source` | source request | the network search (candidates × terminals binding table with the chosen cover, cost axes, assignments, join keys, partial terminals, completions) and the planned node for each `plan_source` call with its span scope |
| `node` | built group | the group's outputs, needed set, atoms injected, parent groups, join keys, the span scope during the build, and the node tree |
| `final` | plan | the FINAL contract, the extent ownership, the built groups and the assembled tree |
| `strategy` | plan | the plan's returned node |
| `resolve` | statement | the resolved `QueryDatasource` tree: every join with its type and pairs, `region_spans`, `zero_filled`, `extent_free_spans` |
| `ctes` | before / after optimization | every CTE with outputs, parents, joins, WHERE, GROUP BY; after optimization each has its rendered SQL |
| `sql` | statement | the compiled statement and, with `--rows`, the result |

A rowset body, a multiselect arm or a condition feeder is planned as its own
plan: the plan selector in the header filters to one, and the step list indents
by nesting depth. Steps after discovery (`resolve`, `ctes`, `sql`) are
statement-level.

Click any graph node, tree node, bucket card or CTE card to see all of its
fields in the inspector. `←`/`→` (or `k`/`j`) step; the URL hash holds the
current step, so a link to a step can be pasted into a note.

## Reading a `~` plan

The story a keyspace plan should tell, phase by phase:

1. `keyspace`: the extension region exists, has own rows, and the outputs that
   should be NULL on it read "absent"; its span is output-demanded.
2. `grouping` ("region domain buckets added"): a `grp:root:root:∅:extent:<span>`
   bucket appears (outlined), carrying the members the domain owns.
3. `group_graph`: the domain feeds the aggregate (lineage) and FINAL (merge).
4. `node` for the domain: a `SelectNode` on the dimension scan; its span scope
   shows the other groups built `extent_free` of the span.
5. `final` / `resolve`: the merge holding the region carries `region <span>`,
   and the join preserves it (LEFT toward the holder), never INNER.
6. `ctes` after optimization: the `LEFT OUTER JOIN` survives the optimizer, a
   padded COUNT is `0-fill`.

## Files

- `trace_query.py`: records a statement and writes the JSON and the embedded viewer.
- `viewer.html`: the viewer. `trace_query.py` replaces its `<!--TRACE-->` marker.
- `examples/`: `customers_orders.preql` (the oracle model, one `~` region),
  `rowset_region.preql` (the same model through a rowset: a nested plan and a
  rowset witness). Generated `*.trace.*` files are git-ignored.
- `source_repeats.py`: counts `plan_source` requests a statement repeats
  verbatim across a corpus (`docs/handoff_duplicate_source_requests.md`).
- `dead_groups.py`: counts built groups whose node never reaches their plan's
  FINAL tree (the viewer's strike-through) across a corpus, classified by
  derivation and by why (same doc, "Dead groups").
- Guards: `tests/core/processing/test_plan_trace.py`.
- `check_viewer.js`: renders every step of a trace through the viewer's code
  under a stub DOM (`node check_viewer.js viewer.html <trace.json>`); run it
  after editing `viewer.html`.
