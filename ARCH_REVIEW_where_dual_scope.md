# Architecture review handoff: WHERE dual-scope + window condition pushdown

**Date:** 2026-07-12. **Branch:** `july-fix`. **Status:** all changes verified
green on the full suite (5480 passed / `-m "not adventureworks_execution"`,
one pre-existing flaky live-LLM test, one pre-existing deselect
`test_prompt_does_not_leak_schema[q76]`).

Two related work streams, reviewed together because they share one semantic
contract. Stream 1 is committed; stream 2 is in the working tree.

- **Commits:** `d4c018275` (`bugfix_window_where_sourcing`) is a REGRESSING
  intermediate — do not review it standalone. `555a9cadc`
  (`better_pushdown_handling`) contains the corrections; review streams 1 as
  the combined diff of both commits.
- **Working tree (stream 2):** `trilogy/core/where_scope_normalization.py`
  (new), `trilogy/core/having_normalization.py` (where-scope block removed —
  net content is HAVING-only again), `trilogy/core/models/build.py`,
  `tests/test_where_select_dual_scope.py` (new),
  `tests/test_derived_concepts.py` + `tests/modeling/usa_names/test_names.py`
  (deliberate re-pins), `tests/test_window_where_pushdown_matrix.py` (new, from
  stream 1's commit). Perf logs/pngs are regenerated artifacts, ignore.

## The semantic contract (both streams enforce this)

For any computation that ranges over the visible row set — aggregate, window,
`group(...) to` — appearing in a statement:

1. **As a select output:** computed over the WHERE-filtered rows ("push where
   before calculation in select").
2. **As a WHERE reference:** computed over the population at its own grain,
   ignoring its peers in the clause (it is the gate, so it cannot be gated by
   itself or its siblings).
3. **Spelling-invariance:** none of the following may change the answer —
   whether the filtered/filtering concept also appears in the select list;
   inline vs `as`-aliased vs environment-named (`auto x <- ...`) spelling;
   bare (`sum(z) by x`) vs expression-wrapped (`(sum(z) by x) + 1`) vs
   reference-chained (`auto v <- sx + 1`).

The inline spelling always satisfied 1+2 because parse/build minted a distinct
virtual concept for the WHERE reference. Every other spelling shared one
address between the two roles, and one address carries one value — so the
population value silently leaked into the projection (a rank came back as the
global rank; `having rank = 1` returned zero rows; an aliased gated aggregate
showed 12 where its inline twin showed 2).

## Stream 1 — window/aggregate WHERE pushdown (committed)

**Bug:** `where vehicle_label = 'A-v1' select vehicle_label, rank ... as r`
computed the rank over the unfiltered universe iff the filtered *derived
row-scalar* was also a select output. Root cause was NOT priority ordering
(BASIC deliberately sorts before AGGREGATE/WINDOW in `get_priority_concept`):
the invariant "conditions ride along into whatever a build swallows" was
severed at exactly one point — the circular-routing guard in
`get_loop_iteration_targets` (discovery_utility.py) nulls conditions when the
priority concept is itself a condition row-argument, and that condition-free
build then co-sourced sibling windows/aggregates as optionals, marking them
found, unfiltered.

**Fix (all in `get_loop_iteration_targets`):** when the guard strips
conditions, DEFER pushdown targets out of that build's optionals so they get
their own loop iteration with conditions routed into their sourcing; keep the
priority's lineage parents + property keys visible on its node (stack
connectivity ignores hidden outputs — without this the deferred target has no
join surface back). Two gates, each earned by a real regression:

- `condition_input_row_scalar_priority`: defer only when every WHERE row-arg
  is row-scalar (`concept_is_row_scalar`, condition_utility.py — lineage
  closure all ROOT/CONSTANT/BASIC). q05's WHERE (rowset-derived entity label +
  OR-of-dates across two fact tables) must stay at completion level; deferring
  co-sourced the other channel's date beside each fact and inflated sums ~10x.
- Only WINDOW/AGGREGATE targets, never FILTER: a FilterItem is an
  author-scoped row intent; q05's per-channel filtered measures pin the plan
  where the statement WHERE composes at the merge.

**Rejected:** exempting row-scalars from the
`must_evaluate_condition_on_this_level_not_push_down` forcing in
`initialize_loop_context` (concept_strategies_v3.py). It fixed the repro but
broke partial-datasource routing (landmark `city='USSFO'`) and or-filter
aggregates with DisconnectedConceptsException. That forcing path is fine; only
the optional-swallowing was broken. (This rejected version is what commit
`d4c018275` contains.)

## Stream 2 — dual-scope address split (working tree)

Makes contract point 3 hold by desugaring every spelling to the inline
two-address form at build time.

### `trilogy/core/where_scope_normalization.py` (new module)

`normalize_select_where_scope(base, environment)` — runs in
`Factory._build_select_lineage` immediately after `normalize_select_having`,
same contract: pure, deterministic (lineages rebuild per rowset body /
multiselect arm), never mutates the authored statement or environment; minted
twins ride the returned copy's `local_concepts`.

For each WHERE row-argument that is also a select output:
- `_resolve_where_scope_target` follows pure-`ALIAS` chains to the real
  concept; bails on single-row scalars (see gates).
- `_collect_cross_row_parts` tree-walks the lineage (through scalar functions,
  parentheticals, case branches, macro `FunctionCallWrapper`s, and references
  to derived concepts) collecting cross-row wrapper nodes
  (AggregateWrapper / NumberingWindowItem / NavigationWindowItem /
  `Function(GROUP)`). FilterItem is a boundary (row-invariant per-row values).
  No parts → no rewrite.
- Three skip gates (each pinned by a test that failed without it):
  1. **Single-row scalars** (`sum(x) by *`): the scalar-output filter path
     (`_is_scalar_only` exemption decline) keys off the shared address;
     a twin detaches the gate (test_where_filter_on_scalar_output_value_is_applied).
  2. **Group-atomic WHEREs, aggregate targets only** (`_where_is_group_atomic`
     + `_covered_by_grain`): if every WHERE row-arg is functionally determined
     by the aggregate's `by` grain (transitive keys-walk; aggregates store
     their `by` addresses as `keys`, properties chain to their key), row
     admission keeps/drops whole groups, the two scopes provably coincide, and
     the twin would only double-scan the fact table (q30-alt scan-count pin,
     q81 SQL-size pin). Windows never skip — any admission reshuffles them.
  3. **Constant/unnest universes** (`_constant_universe` — no ROOT derivation
     in the closure): row identity for constant universes is regenerated
     downstream rather than carried through condition nodes, so a twin cannot
     gate it (pre-existing hole, shared with the inline spelling — see
     residuals; test_rowset, test_window_alt pin the old behavior).
- `_where_scope_lineage` builds the twin's lineage: references to other
  cross-row-bearing concepts are **inlined as their (recursively rewritten)
  lineage nodes** rather than minted as local sub-twin concepts. Two reasons:
  the result is self-contained (its cross-row parts become anonymous wrappers
  that the salt splits — below), and `Concept.get_select_grain_and_keys`
  resolves lineage refs through the ENVIRONMENT only (author.py:1315), so a
  local-only sub-twin concept breaks grain recomputation (this was hit and is
  why the sub-twin-concept approach was abandoned).
- `arbitrary_to_concept` mints the twin (deterministic hash name; identical
  expressions dedup), the WHERE conditional is rewritten via
  `with_reference_replacement`.

### `trilogy/core/models/build.py`

- `Factory.virtual_scope_salt` (new ctor param): in `instantiate_concept`, a
  virtual minted for a cross-row arg (`_is_cross_row_instantiation`:
  AggregateWrapper / window items / `Function(GROUP)`, through
  FunctionCallWrapper) gets the salt suffixed to its generated name. The WHERE
  factory in `_build_select_lineage` passes `virtual_scope_salt="wscope"`.
  Effect: an anonymous `(sum(z) by x)` nested in a WHERE expression
  instantiates at `..._wscope`, never sharing an address (value) with the
  identical expression nested in a select output. Scalar functions/filter
  items stay unsalted — row-invariant, sharing is correct and desirable.
  The `name` override is passed to `arbitrary_to_concept` ONLY when salted, so
  macro-wrapped instantiation names are unchanged otherwise.
- Twin build routing in `_build_select_lineage`: `where_scope_twins` =
  local concepts referenced by the WHERE (row + existence args) and absent
  from the environment. These are EXCLUDED from the main factory's
  local-concept build loop and built ONLY by the where factory, then flow into
  `materialized` via the pre-existing copy-back loop. Invariant protected:
  exactly one canonical BuildConcept per twin address (a main-factory build
  would embed unsalted nested virtuals — a second variant of the same address).

## Deliberate behavior re-pins (semantics changes, confirmed intended)

- `test_derived_concepts.py::test_where_aggregate_input_not_filtered_by_where`
  aliased form: `(1, 12)` → `(1, 2)` (now matches inline).
- `tests/modeling/usa_names/test_names.py` (2 tests): SQL-shape regexes accept
  optional `_wscope` suffix on WHERE-side virtual aggregates. Plan shape
  otherwise identical (WHERE-only aggregates, renamed).

## Known residuals (documented, out of scope)

1. **Const/unnest universes**: named spellings keep old population-gate
   behavior (gate applied, no re-scope) via skip gate 3; the INLINE spelling
   over a constant universe silently drops the filter entirely (pre-existing
   on base: `where (row_number nums) = 1` over `unnest([1,2])` returns both
   rows). Root fix is carrying row identity for constant universes through
   condition nodes in discovery.
2. **v4 discovery** (`CONFIG.use_v4_discovery`, off by default, separate code
   path): on the original repro shape it filters the rank correctly but drops
   the label predicate from the projection (returns `A-v2` too).
3. Multi-grain aggregate gates (`where sx_by_x > 5 and sy_by_y > 3`) fail the
   group-atomicity check and mint twins — correct but potentially
   double-computing; a finer FD analysis could dedup.

## Test coverage

- `tests/test_window_where_pushdown_matrix.py` (stream 1): 72-cell
  duckdb-oracle matrix — 9 window types × 4 filters × 2 select shapes, data
  tuned so asc, desc, AND within-partition orderings all shift under filter —
  plus 2 named regressions. The oracle applies the filter first and computes
  the window over filtered rows only.
- `tests/test_where_select_dual_scope.py` (stream 2): 15 tests — aggregate and
  window × alias / env-named / inline-control / where-only-control /
  expression-wrapped (alias + inline) / ref-chain / rowset-body /
  HAVING-binds-to-select-scope.
- Full suite, ruff, mypy (strict on `trilogy/`), black: clean.

## Suggested review focus

1. **Salt keying** (`instantiate_concept`): the cache keys
   (`local_concepts` / `local_non_build_concepts`) use the salted name — check
   for any other consumer that regenerates the name via
   `generate_concept_name` and expects to find the cached entry.
2. **`_covered_by_grain` soundness**: the keys-walk assumes `Concept.keys` is
   faithful for every derivation it can encounter (aggregates = by addresses,
   properties = key set, BASIC = derived from args). A concept with wrong/empty
   keys fails CLOSED (mints the twin — correctness-safe, perf-risky).
3. **Twin routing set** (`where_scope_twins` = where-referenced ∧ env-absent):
   is there any legitimate pre-existing case of an env-absent local concept
   referenced by a WHERE that is NOT a twin? (HAVING-promoted hidden outputs
   are env-absent but never WHERE-referenced.) If one exists, it would now
   build in the where factory instead of the main factory.
4. **Purity/determinism** of `normalize_select_where_scope` under repeated
   rebuilds (rowset bodies build per consumer) — twin names are hashes of
   rewritten lineages; verify no ordering dependence in `_where_scope_lineage`
   replacements (walk order follows `concept_arguments`).
5. **Shared `build_cache`** across the two factories: twins are distinct
   addresses so no collision, and skip-path shared concepts are built first by
   the main factory (cache hit for the where factory). Confirm no path builds
   a where-referenced NAMED concept in the where factory BEFORE the main
   factory's local loop.
6. **Stream 1 deferral**: `condition_input_row_scalar_priority` is computed
   from `conditions.row_arguments` BEFORE the twin rewrite existed; now that
   twins land in row-arguments (aggregate/window derivations, not row-scalar),
   the deferral won't fire for dual-scope gates — intended (the twin IS the
   population gate; nothing needs conditions pushed inside it), but worth a
   second pair of eyes on the interaction.
