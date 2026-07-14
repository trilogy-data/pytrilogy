# Framework bugs — enriched full-99 run, 2026-07-13/14

Source run: `evals/tpcds_agent/results/enriched_full_20260713` (enriched leg, sf=1,
84/99 pass, 33.8M tokens). All 15 failures were diagnosed one-agent-per-query from
first principles against HEAD (branch `agent-scope-feedback`); prior verdict docs were
treated as hypotheses only and re-verified.

**Result: 6 FRAMEWORK, 9 AGENT.** Per-query detail in `evals/tpcds_agent/diag_qNN_20260713.md`.

Two facts that apply to every bug below:

* **The canonical `.preql` passes in all six cases.** Each escapes its bug by accident
  (single-key grain, a HAVING that happens not to build a semijoin, only one window…).
  The test suite cannot see any of this. Every fix below needs a NEW test at the shape
  that fails, not a re-run of the existing query tests.
* **Five of six are SILENT** — wrong rows or wrong tables, no error. The eval's token
  bar was the only detector.

---

## Priority 1 — the nullable-join-key family (q11, q51, q59, q86)

Four failures, one conceptual root: **the planner's handling of nullable join keys is
not sound.** It drops rows in three different ways and duplicates them in a fourth.
Fix as one piece of work; patching them individually will just move the bug.

| q | Direction | Mechanism |
|---|---|---|
| q11 | drops rows | NULL-unsafe row-`IN` in a HAVING semijoin |
| q59 | drops rows | same, reached via nullable *measures* entering the grain tuple |
| q51 | drops rows | nullability **erased** across MergeNode → plain `=` on a null-extended column |
| q86 | **duplicates** rows | null-safe `=` applied to rollup keys that are not unique |

> **REGRESSION EVIDENCE for the in-flight membership rewrite (2026-07-14, from the q14 owner):**
> canonical `test_fourteen` now fails on the shared tree (+534 rows: 675768855.12/156101 vs
> oracle-exact 673409655.64/155567). The only q14 SQL diff vs HEAD is the tuple-`IN` →
> null-safe-`EXISTS` rewrite, and the old form's subquery carried `is not null` guards the new
> `IS NOT DISTINCT FROM` form drops — so NULL-attribute item rows now MATCH NULL tuples in the
> membership set. Executing HEAD's generated SQL gives the oracle-exact total; the tree's gives
> the wrong one. Null-SAFE must not mean null-MATCHING for membership: a tuple with a NULL
> component should stay excluded (SQL `IN` semantics), while non-NULL tuples become matchable.

### q11 / q59 — HAVING semijoin deletes NULL-keyed rows (SILENT)

`having_normalization.py:796 build_grain_key_membership()` builds a semijoin over ALL
grain keys — including nullable properties — and `dialect/base.py:1636
_render_composite_membership()` renders it as `(a,b,c) IN (select …)`. DuckDB row-`IN`
is NULL-unsafe (`(1,null) in (select 1,null)` → NULL, not TRUE), so any group with a
NULL in any key can never match and is silently deleted.

* q11 repro: `select rev.cid, rev.fname having rev.tot > 100000` over a rowset →
  44,907 rows vs oracle 46,507; the 1,600 NULL-first_name groups vanish.
* Fires iff: semijoin AND ≥2 grain keys AND ≥1 nullable key.
* q59 is the same bug through a different door: `having_normalization.py:776
  stable_grain_keys()` returns `sorted(base.grain.components)`, and for a select over a
  rowset with no aggregate of its own **every row-level output is a grain component —
  including nullable derived measures**. 8-line repro in `diag_q59`: 306 rows vs 318.
* q59 also shows the semijoin is often gratuitous: the HAVING target is same-grain and
  the engine ALREADY applies it correctly as a plain `WHERE` in the CTE, then redundantly
  re-derives the projection and semijoins it back against that same CTE.
* Fix direction: null-safe membership rendering (`IS NOT DISTINCT FROM` / EXISTS-form),
  and/or exclude nullable non-key outputs from the grain tuple, and/or skip the semijoin
  entirely when the predicate is same-grain.

### q51 — MergeNode erases nullability (SILENT)

`merge_node.py:688-728 MergeNode._resolve()` stamps join-induced nullability onto the
resulting `QueryDatasource` but **never writes it back to `self.nullable_concepts`**.
Downstream nodes read nullability from the parent StrategyNodes (`base_node.py:153`,
`:211`), so it is lost, and `join_resolution.py:453-487 get_modifiers`/`side_nullable`
never emit `IS NOT DISTINCT FROM`. Generated SQL joins on a null-extended payload column
with a plain `=`; the null-extended side is deleted.

* Repro: two windows over two different nullable payload columns of a `union join` →
  488,653 rows; one window or no window → correct 610,684.
* Monkeypatching the write-back fixes it (agent's q51 goes from ~50 wrong rows to 3).

### q86 — rollup + 2 windows with differing partitions DUPLICATES rows (SILENT)

The planner materialises one rollup CTE per partition spec, then INNER-joins them on the
rollup **dimension keys only** (`parent`, `category`, `class`) with `IS NOT DISTINCT
FROM` — **no grouping discriminator** (`lvl`/`g_cat`/`g_class`) in the key. Under ROLLUP
those keys are not unique: grand-total, NULL-category subtotal, and genuine NULL-category
detail rows all key as `(NULL, NULL, NULL)`, so null-safe equality fans them 3x3.

* 133 → 139 rows (rowset variant 133 → 153).
* Trigger matrix: rollup alone OK; rollup + ONE window OK (either partition); rollup +
  two windows with the SAME partition OK; two windows without rollup OK. Minimal failure
  = rollup + ≥2 windows with DIFFERING partition specs.
* `group_node.py:165-190` sets the nullability that forces the null-safe predicate — its
  comment claims null-safe joins prevent "doubling"; here they cause it. Keys chosen in
  `join_resolution.py:655 get_node_joins`, consumed by `merge_node.py:273`. Rollup
  GroupNode still declares grain = by-keys (`group_node.py:87-125`), which is the lie.
* Fix direction: the grouping discriminator must be part of the join key for rollup
  outputs (or the rollup GroupNode must not claim by-keys grain).

---

## Priority 2 — q23: global aggregate renders with NO aggregate function (SILENT)

> **FIXED 2026-07-14** (branch `agent-scope-feedback`). Two-layer fix: (1)
> `merge_node.py _inject_scoped_join_key_exposure` now skips abstract-grain
> parents (`_abstract_output_grain`) — a `by *` aggregate has no join key and is
> broadcast via keyless FULL join; (2) the `EXPERIMENT` unconditional collapse in
> `dialect/base.py` is replaced by `_aggregate_collapse_safe`: a global (`by *`)
> aggregate rendered into a keyed non-grouping CTE is now a loud
> `INVALID_REFERENCE_BUG_AGG_GRAIN_MISMATCH` (strict mode raises), never a silent
> identity. Keyed aggregates keep the collapse (#572 FD case and the designed
> union-join partition extension both depend on it). Matrix:
> `tests/join_matrix/test_global_aggregate_broadcast_matrix.py` (trigger matrix
> cells A–G + loud-not-silent backstop). The q23 prompt store-channel defect is
> also fixed in `query_prompts.json`.

The worst one. `agg(x) by *` (ROOT/global) in a SELECT carrying an authored `union join`
emits SQL containing **zero `max(` / `count(`** and no GROUP BY — the "global max" is
FULL-JOINed back on the join key, so every row's global max is its own value. Runs clean,
returns plausible garbage. 1.68M tokens; the agent noticed the anomaly and could not see
why, because the plan metadata (`derived_value_scopes → group_by: ["*"]`) is CORRECT —
only codegen is wrong, so there was no diagnostic surface at all.

Chain:
1. `merge_node.py:317 _inject_scoped_join_key_exposure()` — **the q59 key-drop fix** —
   force-injects a scoped-join key into ANY merge parent whose grandparent exposes it,
   **including an abstract-grain GroupNode**.
2. That re-grains the node, flipping `CTE.group_to_grain` to False
   (`core/models/execute.py:92`, `:248`).
3. `dialect/base.py:1358-1362` then collapses `max(x)` → bare `x` via
   `FUNCTION_GRAIN_MATCH_MAP`.
4. **`dialect/base.py` carries an in-code `EXPERIMENT: guard removed, unconditional
   collapse`** — that removed guard is precisely what turns this from a loud error into a
   silent wrong answer. **Decide whether to revert that experiment first; it is the
   difference between a bug and a catastrophe.**

* Trigger matrix: fails only with `union join` + `by *` over a rowset output (max and
  count alike, select or having). Passes with no join, with `subset join`, or with
  plain-model operands.
* A/B on the agent's own file: `union join` → `subset join` takes it from 100 wrong rows
  to 52 rows containing 3 of the 4 reference rows exactly.
* Prior q23 artifacts (duckdb bind-parameter placeholders) are REFUTED. This is new.
* q23 ALSO needs a prompt fix — see "question defects" below.

---

## Priority 3 — q14: union partiality erased → clean query reads the WRONG TABLES (SILENT)

> **STATUS: FIXED 2026-07-14.** `union_unhealed_partial_addresses` in `core/models/build.py`
> (shared by `BuildUnionDatasource.partial_concepts`, `_structural_partial_concepts`, and
> `create_union_datasource_candidate`). Matrix tests (8 shapes × both lexical name orders) in
> `tests/engine/test_enum_unions.py`. Verified on real all_sales sf=1: `count(s.order_id)` =
> 240,000. **Fix direction #4 below is WRONG — attempted and reverted**: the mixed group is the
> legitimate per-channel provider of `return_channel_dim_id` (web return-site lives on web_sales);
> rejecting it re-paired q05's return-dim join onto `channel` alone → deterministic 3 GiB OOM in
> canonical `test_five`. Partial propagation alone already keeps the mixed union from outranking
> the pure sales union (its keys stay `~`-partial), while leaving it available as q05's FK bridge.

### The invariant (this is the spec the code violates)

> A **partial datasource**'s columns are partial by default. A **complete union across
> those partials promotes them to full** — that is what the union is for. **EXCEPT**
> columns carrying an **explicit `~`** inside a partial datasource: those are
> subset-covering for a reason the union does not heal, and **must stay partial through
> the union.**

`BuildUnionDatasource.partial_concepts` returning `[]` does not merely lose information —
it **promotes exactly the one class of column that must never be promoted.** The correct
value is the union of the children's **explicit** column-level `~` partials. (Returning
*all* children's columns would be the opposite error: it would deny the promotion the
union exists to grant.)

### The violation

```python
# trilogy/core/processing/node_generators/select_helpers/source_scoring.py:30-41
def _structural_partial_concepts(ds) -> list[BuildConcept]:
    """...
    BuildUnionDatasource doesn't carry per-child partial info; treat it as having
    no structural partials of its own — its children's structural partials are
    handled when those children are scored.      # <-- FALSE when the UNION is the candidate
    """
    if isinstance(ds, BuildUnionDatasource):
        return []          # union of ~-partial children scores as COMPLETE
    return ds.column_level_partial_concepts or []
```

```python
# trilogy/core/models/build.py:2318-2320
@property
def partial_concepts(self) -> List[BuildConcept]:
    return []              # makes the wrong pick SILENT in the aggregate path
```

### Consequence

`get_union_sources` (`datasource_injection.py:140`) injects two rival union groups for the
enum key `channel`: the three `*_sales` tables and the three `*_returns` tables. The
returns children bind `~order_id` / `~item.sk`. With those `~` erased, the returns union
scores `partial_count = 0` — **exactly tying** the sales union in `_ds_mat_score`
(`source_scoring.py:189`) — and the tuple tiebreak falls through to the **node name
string**: `"ds~web_returns_unified-…"` < `"ds~web_sales_unified-…"`. The returns union
wins.

```
import raw.all_sales as s;
select s.channel, s.order_id limit 3;
-- UnresolvableQueryException: no complete sources found for {'s.order_id'}
--   ... yet a complete source (the sales union) demonstrably exists.

select count(s.order_id) as c;
-- runs CLEAN, returns 202,316.  Generated SQL reads ONLY the three *_returns tables.
-- truth = 240,000.
```

| request | result |
|---|---|
| `s.channel, s.order_id` | FAIL unresolvable |
| `s.channel, s.order_id, s.quantity` | PASS (correctly unions the 3 *sales* tables) |
| `count(s.order_id)` | **PASS but WRONG** — returns tables only |
| returns datasources renamed `zz_*` (sort after sales) | **all PASS**, `count(order_id)` = 240,000 |

That last row is the proof: **the outcome is decided by lexical datasource-name order.**

### Fix direction

1. `_structural_partial_concepts` must propagate children's explicit `~` partials to the
   union (per the invariant above).
2. `BuildUnionDatasource.partial_concepts` = union of children's explicit column-level
   partials, not `[]`.
3. Both together make the sales union strictly better than the returns union on
   `partial_count`, removing the name-order tiebreak entirely.
4. Separately: `_best_enum_union` mints a nonsense MIXED group
   `web_sales + catalog_returns + store_returns` — that union would stack web-sales rows
   onto catalog/store-return rows. Reject mixed-family combos.

---

## Priority 4 — loud but real

> **STATUS: all three FIXED 2026-07-14** (branch `agent-scope-feedback`). Verified
> end-to-end: the agent's q11 msg-19 query (the exact 25 GiB OOM) now executes in
> **0.6s under a 4 GB cap and byte-matches the 90-row reference** (jointly with the
> priority-1 membership work on this branch).

* **q11, 4-way `union join a=b=c=d`** — the closure hypothesis was WRONG: a bare 4-way
  union join plans clean (3 FULL JOINs, no payload keys). The explosion was (a) the
  O(n²) semijoin chain below, and (b) `CTE.__add__` (`core/models/execute.py`) merging
  two copies of one logical CTE by `unique_id` — joins to the SAME right CTE whose
  pair sets differ by one redundant column both survive, rendering a row-multiplying
  duplicate `FULL JOIN "abhorrent"`. Fixed: `coalesce_duplicate_joins` merges
  same-(type, left, right) joins into one carrying the union of the pairs.
  Test: `tests/test_coalesce_duplicate_cte_joins.py`. Payload columns in join keys
  remain (rowset outputs are grain members, and the pairs now render null-safe —
  harmless in all observed cases; prune via FD closure only if it resurfaces).
* **q11, HAVING leaves chain O(n²) semijoins** — mechanism: `get_query_node` folds all
  HAVING memberships onto the final node; `PredicatePushdown` then pushes each
  membership atom into every eligible parent INCLUDING the sibling membership feeder
  CTEs (the j<k triangular order falls out of `_promotion_would_cycle`). Semantically
  redundant (the final AND already intersects), quadratically expensive. Fixed: never
  push an existence-bearing atom into a CTE serving as an existence feeder of the same
  consumer (`predicate_pushdown.py`). A/B on the synthetic 4-way mirror: 15 feeder
  cross-refs / 22.9 KB → 0 / 14.8 KB. Test:
  `tests/optimization/test_existence_feeder_pushdown.py`.
* **q51, unactionable error** — the bare `raise SyntaxError` in
  `node_generators/window_node.py` (window parents resolved but missing concepts, e.g.
  windows over the coalesced keys of a union-join rowset) now returns `None` like its
  sibling branch, so discovery finishes and surfaces a standard
  `DisconnectedConceptsException` naming sourced vs. unresolved concepts ("Resolution
  error", not "Syntax error: None"). Test:
  `tests/test_window_missing_parent_diagnostic.py`. The underlying planner gap (window
  parent drops `wd.item_sk`) still exists — the QUERY still fails, but loudly and
  actionably; it is the same union-join family as priority 1.

---

## Our diagnostics lied — three separate surfaces (this is why the silent bugs survived)

The agents did not fail for lack of trying; in three cases they checked for exactly the
right thing and our tooling told them nothing was wrong.

* **q59**: agent trace msg 34 — *"Column stats show no nulls in the ratio columns...
  That's expected at this scale factor."* It was checking for the very NULLs the semijoin
  was deleting.
* **q50**: `derived_value_scopes` reported only `group_by: [10 store attrs]` and never
  disclosed the planner-injected input-population collapse — the wrong answer looked
  clean. (This is the new scope-diagnostics feature hiding the exact thing it exists to
  reveal.)
* **q01**: `column_stats.distinct` is DuckDB `approx_unique`
  (`trilogy/dialect/duckdb.py:386,401`) with no "approximate" label — reported 12,027
  distinct over 11,602 rows, so the agent distrusted the one stat that would have exposed
  its dedup.

---

## Not framework, but ours to fix (the AGENT bucket, 9 queries)

The row diffs are agent-authored, but **five of nine were walked into by our own
affordances**:

* **q87** — agent wrote `count(*)`; Syntax [223] (`parsing/v2/errors.py:87`) rejected it
  and told it to *"count a key field: `count(<key>)` (counts are already distinct)"*. It
  obeyed, picked a nullable column, and SQL `COUNT(col)` silently dropped 852 NULL rows.
  The hint has no NULL warning and the "(already distinct)" parenthetical is false — the
  emitted SQL is a plain `count(col)`.
* **q05** — copied `order by _level asc` from the rollup example at
  `trilogy/ai/syntax_examples.py:700-702`, which frames level-sorting as THE rollup sort;
  the prompt said "sort ONLY by channel label and entity identifier". One sort key = the
  whole failure.
* **q54** — used `subset join qualifying.cust_sk = store.customer.sk` as a FILTER.
  `subset join a = b` declares a ⊆ b and preserves b — a domain declaration that restricts
  NOTHING. The subset leaf was then pruned; the generated SQL contains no
  catalog_sales/web_sales/item at all. A lint for "subset join that restricts nothing"
  would have caught it.
* **q01 / q89** — output-column SHAPE, in opposite directions: q01 needed a hidden (`--`)
  column it didn't add; q89 added one it should have hidden. Same idiom, two failures.
* **q80** — the prompt's word "outlet" lures at `all_sales.outlet_id`, which for CATALOG
  is `CS_CALL_CENTER_SK`. Renaming that key removes the trap.

Genuine agent sloppiness with no framework/guidance contribution: **q56** (filtered the
SCD item *variant* instead of the item *code*), **q66** (omitted `* quantity` on all four
measures), **q80**'s NULL-propagation slip (`sum(a) - sum(coalesce(b,0))` instead of
`sum(a - coalesce(b,0))`).

## Question defects

* **q23** — prompt never says the frequent items are *store-channel*; the oracle and
  canonical `.preql` both restrict to store. Adding that restriction ON TOP of the engine
  fix gives a byte-exact 4-row match. **q23 needs both fixes.**

---

## The >500k token-sink prior needs qualifying

It fired correctly on q23 (1.68M), q86 (1.44M), q14 (1.07M), q11 (995k), q51 (617k).
It was WRONG on q05 (990k), q80 (943k), q66 (854k), q54 (856k) — all pure context/turn-count
burn with no engine bug: monotonically growing prompt tokens, no retry loop, max context
~51k. q66 ran `explore` twice on the same model (plain, then `--include-hidden`, ~43k chars
of near-duplicate).

Sinks remain a good DETECTOR (they found five real bugs, four of them silent) but are no
longer a reliable PREDICTOR — 4 of 10 were false positives this run. Dedup'ing repeated
`explore` output is the cheapest token win available.

## Stale records retired (verified non-reproducing on HEAD)

* q11 "scoped-join repeated-LEFT-anchor operand order → DisconnectedConcepts" — no
  DisconnectedConcepts anywhere in the trace.
* q87 "engine bug" — canonical returns 47,298, matches the oracle exactly.
* q66 "quantity quirk = guidance defect" — prompt names quantity 3x,
  `catalog_sales.preql:52` documents `ext_sales_price = sales_price × quantity`. Already
  fixed.
* q54 "int32 arithmetic overflow" (revenue is DECIMAL) and "reference store fan-out"
  (`query54.sql:32` already has `ss_store_sk = s_store_sk`).
