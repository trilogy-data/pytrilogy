# v4 correctness blocker: existence-source attachment recursion (+ one invalid-reference crash)

Status: **A FIXED (2026-06-25)**, B still open. See "Resolution" at the bottom for A.
These are **correctness** blockers, not size/shape. They
were mis-bucketed in `v4_known_failing.py` (`_TPCDS_SIZE` / `_MODELING`) so they read
as cosmetic; they are actually hard crashes under v4. **q02 is NOT one of these — q02
(`test_two`) is already fixed** (see `v4_q02_invalid_alias_handoff.md`); this doc
replaces the stale "q02 is the correctness blocker" framing in `v4_audit.md`.

## A. `RecursionError` from existence-source attachment (3 tests, one root cause)

Affected (all v4-only, `TRILOGY_V4_DISCOVERY=1`):

- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_ten` (q10)
- `tests/modeling/tpc_ds_duckdb/test_queries.py::test_two_one` (q2.1)
- `tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py::test_rowset_arithmetic_argument_keeps_precedence`

Repro (any of them, stable — reproduced 2/2 and 8/8 on q10):

```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  "tests/modeling/tpc_ds_duckdb/test_queries.py::test_ten" --runxfail -q
```

### Symptom

```
RecursionError: maximum recursion depth exceeded
!!! Recursion detected (same locals & position)
```

Shared stack (identical across all three):

```
strategy_builder.py:340  _attach_existence_sources
strategy_builder.py:322  _attach_existence_to_node      -> node.rebuild_cache()
nodes/base_node.py:483   rebuild_cache                  -> self.resolve()
nodes/base_node.py:488   resolve                        -> self._resolve()
nodes/base_node.py:444   _resolve                       -> [p.resolve() for p in self.parents]
nodes/base_node.py:488   resolve  ... (infinite)
```

### Root cause

`_attach_existence_to_node` (`strategy_builder.py:300`) appends existence parents to
`node.parents` and calls `node.rebuild_cache()`. The parents come from
`_existence_parents_for(concepts, built, skip=node)` (`:219`), which scans every node
in `built`, and for each existence concept attaches the **first built node that
outputs it** — via `source_node.copy()`.

Two facts combine into a cycle:

1. `skip=node` only excludes the *exact* node object. It does not exclude a candidate
   whose parent subtree already contains `node` (i.e. `node`'s own descendant or a
   sibling that transitively reaches `node`).
2. `StrategyNode.copy()` does `parents=list(self.parents)` — a shallow copy that
   **shares the original parent objects**. So the copied existence parent's subtree
   still contains the real `node`.

Result: `node -> existence_parent_copy -> ... -> node`. `_resolve` walks
`[p.resolve() for p in self.parents]` with no in-progress guard (the resolution cache
is only set *after* `_resolve` returns — `base_node.py:485-490`), so a cyclic parent
chain recurses until the interpreter limit.

The second loop in `_attach_existence_sources` (`:338-340`) makes this likely: it
attaches existence concepts/parents to **every** node under **every** built root, so
any node that outputs a concept another node needs as an existence arg can become a
mutual parent.

### Where to fix (suggestions, pick one)

- **Preferred — cycle guard at attach time.** In `_attach_existence_to_node` /
  `_existence_parents_for`, skip any candidate whose subtree contains `node` (walk
  `_strategy_nodes(candidate)` — already defined at `:286` — and reject if `node` /
  `id(node)` appears, or if `node` appears in the candidate's transitive parents).
  Existence sources are a side channel for subselect ordering (see the Stage-2 note in
  `v4_audit.md` "Existence edges must stay side-channel-only"), so dropping a candidate
  that would create a row-stream cycle is safe — it should never have been wired as a
  resolvable parent.
- **Defensive — in-progress guard in `resolve()`.** Set a sentinel on entry to
  `_resolve` and raise a typed planner error if re-entered, so a cycle fails loudly at
  build time instead of as a `RecursionError`. Good as a belt regardless, but the
  attach-time fix is the real cure.

### Acceptance

- All three tests build valid SQL with rows matching the reference, no `RecursionError`.
- (q10/q2.1 then revert to plain `_TPCDS_SIZE` if still over their length ceiling;
  the rowset test asserts shape.)
- Full v4 sweep stays at 0 failed.

## B. `ValueError: Invalid reference string` (1 test, separate cause)

Affected: `tests/modeling/usa_names/test_names.py::test_filter_constant` (currently
mis-bucketed `_MODELING`).

```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  "tests/modeling/usa_names/test_names.py::test_filter_constant" --runxfail -q
```

### Symptom

```
ValueError: Invalid reference string found in query: SELECT ...
trilogy/dialect/base.py:2370: ValueError
```

A concept reference reaches the renderer that the final source map can't resolve, so
`base.py` rejects the rendered SELECT. This is the same *class* as the old q02 bug
(an unresolvable reference renders into SQL) but a different trigger — a `filter` over
a **constant** in the usa_names model — and it does not go through `union_dim_pushdown`.
Start by dumping the offending SELECT (the ValueError message includes it), find which
concept's reference is unbound, and trace its source map back through
`_assemble_final_node` / `resolve_concept_map`. Sibling `test_aggregate_filter_anonymous`
in the same file is only a shape `AssertionError`, so the constant-filter path is the
differentiator.

### Acceptance

- `test_filter_constant` renders valid SQL with correct rows; no `ValueError`.
- Full v4 sweep stays at 0 failed.

## Note for the audit

After A and B land, the `_INLINE`/`_MODELING`/`_TPCDS_SIZE` buckets are genuinely
"rows-correct, shape/size only" — which is what `v4_audit.md` currently *claims* but
isn't yet true while these crashes are hidden in those buckets.

## Resolution (A — 2026-06-25)

Two changes, both narrowly scoped to existence handling (no modeling regressions —
the isolated combined-sweep failure set is byte-identical with and without the fix):

1. **`strategy_builder.py::_existence_parents_for`** — when the supplying candidate's
   subtree already contains the host node, `_deep_copy_node` clones the whole candidate
   subtree so the shared host becomes an independent duplicate. This breaks the
   `host -> candidate_copy -> ... -> host` row-stream cycle (the RecursionError) while
   keeping the existence source renderable (verbose but acyclic). This alone clears the
   q2.1 / rowset recursion.
2. **`v4_node_generators/root.py::gen_root`** — resolve multi-arg existence sources at
   build time (`_existence_arg_count(...) >= 1`, was `== 1`). The single-arg-only guard
   left q10's wrapper SelectNode with empty `existence_concepts`/parents, deferring to
   `_attach_existence_sources`; the deferred path never wired the source onto the union
   member scans that render the subselect → `INVALID_REFERENCE_BUG`. Resolving up front
   gives the wrapper real existence parents (the producer CTEs), so the subselect binds.

Result: all three build valid SQL with rows matching the reference; q10/q2.1 revert to
`_TPCDS_SIZE` (over the length ceiling) and rowset to `_INLINE` (precedence shape diff).
Regression lock: `tests/core/processing/test_v4_existence_cycle.py`.

**B (`test_filter_constant`) is still open** — investigated and confirmed a genuinely
separate root cause: the WHERE-over-per-name-aggregates is *disconnected* from the
constant output (`test_filter_constant_with_constant` raises
`DisconnectedConceptsException` for exactly this; the no-unnest variant renders the
unbound aggregate refs as `INVALID_REFERENCE_BUG`). Fixing it means treating a
disconnected aggregate filter as a global scalar/cross-join gate — planner surgery
unrelated to the existence side-channel, deferred.
