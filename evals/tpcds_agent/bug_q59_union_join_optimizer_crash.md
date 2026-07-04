# q59 token sink — internal crash in the OUTER→INNER join optimizer

**Target:** q59 in `evals/tpcds_agent/results/20260703-151512/` — 4,376,400 tokens.
Passed (100 correct rows) but only after massive thrash. Trilogy v0.3.288.

## Symptom (what the agent saw)

q59 = weekly store sales, this-year (2001) vs next-year (2002), matched on
`(store, week=week-52)`, report per-day ratios. The natural Trilogy shape is
two `where … select …` rowsets joined with the new `union join`
(SUBSET/UNION joins that replace LEFT/FULL). Every attempt to `union join` two
rowsets returned an opaque **internal** error (`"Unexpected error in …"`, exit 1,
no location), repeated across messages 84–100 of
`agent_log.q59.conversation.txt`:

```
Unexpected error: _pair_side_fully_matches() takes 7 positional arguments but 8 were given
Unexpected error: 'UpgradeOuterFromKeySetEquivalence' object has no attribute 'subset_binding_sources'
```

The first fires on a **composite-key** union join
(`… and this_year.week = next_year.week - 52`); the second on a **single-key**
union join and on a plain multi-day `auto … by ss.store.text_id`. Because the
error is an internal Python exception with no query location, the agent could
not tell which construct was at fault. It abandoned rowset+join entirely, tried
window `lead`, tried self-joins, churned through store.id-vs-text_id fan-out
(expected semantics, not a bug), and finally stumbled into a workaround: encode
the composite key as a string (`concat(text_id,'-',week_seq)`) and do a
**single-key** `union join this_year.k = next_year.k` — which happened to dodge
the crashing path. That workaround is the final `workspace/query59.preql`.

## Minimal repro (self-contained BODY)

```
import raw.store_sales as ss;
auto total <- sum(ss.sales_price) by ss.store.text_id, ss.date.week_seq, ss.date.year;
with this_year as
where ss.date.year = 2001
select ss.store.text_id as code, ss.date.week_seq as week, total;
with next_year as
where ss.date.year = 2002
select ss.store.text_id as code, ss.date.week_seq as week, total;
select this_year.code, next_year.code
union join this_year.code = next_year.code
    and this_year.week = next_year.week - 52
limit 5;
```

At the eval-time build this raised the `_pair_side_fully_matches`/
`subset_binding_sources` internal error. On the **current working tree** it
runs clean and returns correct rows.

## Trigger matrix (current working tree — all repros now PASS; crash is fixed)

| # | Construct | Eval-time result | Current-tree result |
|---|-----------|------------------|---------------------|
| A | `union join a.k = b.k` on concat single key | (agent's workaround) OK | 312 rows |
| B | `union join a.code=b.code and a.week=b.week-52` (composite) | **CRASH** `_pair_side_fully_matches 7≠8` | 312 rows — **identical to A** |
| D | `union join a.code = b.code` (single, non-unique) | CRASH `subset_binding_sources` | 16 854 rows (correct fan-out; code alone isn't unique) |
| — | multi-day `auto … by ss.store.text_id`, no join | CRASH `subset_binding_sources` | OK |
| C | `subset join this_year = next_year` (bare rowset, no `.key`) | n/a | `UndefinedConceptException` (parser wants a key concept — see below) |

Key result: **B == A == 312 rows**. The composite `and` union join now resolves
as a true composite AND (no cross-product fan-out) — exactly what the agent
needed and feared it couldn't get. The only thing standing between the agent and
the direct answer was the internal crash.

## Root cause (file:line) + regression status

`trilogy/core/optimizations/value_set_join_upgrade.py`, class
`UpgradeOuterFromKeySetEquivalence` (the FULL/LEFT/RIGHT→INNER narrowing pass
that runs on every OUTER/UNION join). Two mid-refactor breakages:

1. **Arg-count desync** — `_pair_side_fully_matches(...)` (def ~L465) was called
   (`_narrow_directionally`, ~L713/L728) with a `self.subset_binding_sources`
   8th argument the definition no longer accepted → `TypeError: takes 7 …
   but 8 were given`.
2. **Missing attribute** — `__init__` stopped setting `self.subset_binding_sources`
   while a call site still read it → `AttributeError`.

Both are regressions from the **join refactor** (`dcc62ed78 union_checkpoint`
→ `956e7303b hacky_joins` → `35f8157b6 checkpoint`, the SUBSET/UNION-join
replacement of LEFT/FULL). `git show` confirms the `subset_binding_sources`
parameter/attribute pair was introduced there. HEAD (`35f8157b6`) has def+call
both 8-arg and sets the attribute; the **eval ran on a dirtier intermediate**
of that window (v0.3.288 was last bumped at `36deef863`, pre-refactor, and never
re-bumped), which is where the def/call fell out of sync.

**Already being fixed in the uncommitted working tree.** `git diff HEAD` on this
file (24 ins / 27 del) removes `subset_binding_sources` entirely and makes
`_pair_side_fully_matches` a clean 7-arg (`domain_graph, subset_join_map,
scoped_canonical`). With those edits the current engine no longer crashes on any
repro above, and `workspace/query59.preql` runs to 100 correct rows.

## Classification

- **Real framework bug (regression), primary token-sink driver:** the two
  internal crashes above. Being fixed in the working tree — verify the fix lands
  and add a join-matrix cell for `union join` (single + composite key) between
  two `where … select …` rowsets so this can't silently regress again.
- **Residual guidance gap (not a hard bug now):** even with the crash gone, the
  opaque `"Unexpected error"` (no query location) gave the agent nothing to act
  on and sent it down window/self-join dead ends; and the docs' `and` vs `= c`
  composite-key semantics led it to *believe* composite `union join` fans out
  when it does not (B proves it doesn't). A worked composite-key `union join`
  example + a location-bearing error would have saved most of the 4.3M tokens.
- **Agent errors (clear messages, not framework):** SQL-ism `… on a=b`
  (clean parse error), `FROM` keyword (`Syntax [101]`), misplaced `by code, week`
  (clean parse error), store.name-vs-text_id fan-out (expected grain semantics,
  correctly handled with `max(name) by text_id`). Case C's bare
  `subset join rs = rs` (no `.key`) is a malformed key, not a supported whole-
  rowset form — design doc only shows `subset join a.k = b.k`.

---

## Re-run 20260704-035023 — NEW silent wrong-result regression (NOT the crash)

**q59 REGRESSED to FAIL, 1,918,016 tokens.** Report: `59 | fail | ref 100 | cand
100 | result set differs from reference`. Verdict below is against the CURRENT
working tree (battery commits `58037e728`→`c0d82fb1e` + the uncommitted
build.py/join_resolution.py/rowset_node.py/merge_node.py diffs), engine built on
`results/20260704-035023/workspace/` (DB copied to scratchpad to dodge a stray
`Python313 PID 38544` file lock — the run's only `"Unexpected error"` in
`agent_log.q59.conversation.txt` is that same transient `_duckdb.IOException`,
NOT an engine bug).

### (a) Original optimizer crash: FIXED, did NOT regress
No `_pair_side_fully_matches` / `subset_binding_sources` error reproduces on any
repro. `value_set_join_upgrade.py`: `_pair_side_fully_matches` def (L563) and both
call sites (L856/L872) are in sync (7 positional + `graph_proof_only` kwarg);
`subset_binding_sources` is fully removed. The fix landed committed in
`c0d82fb1e full_work` (value_set_join_upgrade.py, 45 lines). Crash is gone.

### (b) NEW framework bug — union join between two rowsets now COLLAPSES the sides
Same commit `c0d82fb1e` also rewrote `rowset_node.py` (+96) and now **every**
`union join` between two independent `where … select …` rowsets returns WRONG
rows — the two sides collapse onto one canonical concept instead of joining.
Ground truth from raw SQL on the run's DB in the last column:

| Case | body | current engine | correct |
|------|------|---------------:|--------:|
| B composite derived | `code=code AND week=week-52` (2001 vs 2002) | **431** | 312 |
| composite plain-eq | `code=code AND week=week` (2001 vs 2002) | **371** | 0 (weeks disjoint) |
| D single key | `code=code` | **7** | 16 854 |
| single week | `week=week` | **742** | 0 |

D is the smoking gun: the generated SQL builds ONLY the `next_year` CTE and emits
`next_year_code AS this_year_code, next_year_code AS next_year_code` — the entire
`this_year` source is dropped (7 = 6 store codes + 1 null). B degrades to a
chained double `FULL JOIN`: the first FULL JOIN (`juicy`) joins on the derived
`week-52` key ALONE (drops the `code` co-key), a second re-adds code → 431 not 312.
Contrast the bug-report matrix above (B=312, D=16 854) captured on the PRIOR
working tree — those numbers regressed within the battery.

**Optimizer is NOT the cause.** Toggling `upgrade_outer_key_set_equivalence` and
`narrow_equal_domain_joins` OFF leaves B=431 / D=7 unchanged → the wrong result is
produced in join RESOLUTION / rowset planning, upstream of every optimization pass.

### Root cause (file:line)
`trilogy/core/processing/node_generators/rowset_node.py::_expose_coalesced_key_contents`
(def **L284**, called **L438**) + the coalescing-scoped-join key-group collapse it
documents: a `full`/`subset`/`union` join "collapses a join-key group onto ONE
canonical body concept, leaving each authored side only as a *pseudonym*." That is
correct when one side is a subset of a shared base, but for TWO independent rowset
SOURCES it means only the canonical side's CTE is materialized and the other side
is exposed as a hidden pseudonym rather than joined — the join is lost (single key)
or degrades to a co-key-dropping FULL-JOIN chain (composite). This rework REPLACED
the 2026-07-02 fix `inject_scoped_join_key_exposure` in `MergeNode._resolve` (which
made every merged side expose its OWN member of each authored key group so both
stayed joinable); that function is now gone (`grep` finds no references).

### What the agent hit this run (⇒ token sink)
No crash. The agent wrote the natural `union join this_year.store_id =
next_year.store_id and this_year.week_seq = next_year.week_seq - 52`, saw the
collapse's fan-out/duplicate rows (same store+week 3×; log ~L1999–L3040), spent
~30 messages theorising about null keys / non-unique keys, abandoned rowset+join,
and fell back to a window `lead(…, 52)` shape (final `workspace/query59.preql`)
that produced 100 rows differing from reference → fail. The silent wrong-result
from the union-join collapse is the driver.

### Classification
**REGRESSED — NEW framework bug (silent wrong-result), introduced by the battery
(`c0d82fb1e full_work`), NOT the original crash re-appearing and NOT agent
variance.** The union-join-between-two-rowsets feature that this doc certified
(B==312) is broken across single, composite-plain, and composite-derived keys.
Add a join-matrix cell (`tests/join_matrix/`) for `union join` between two
`where … select …` rowsets asserting single-key fan-out AND composite-key exact
count, so this can't regress silently again. (Canonical
`tests/modeling/tpc_ds_duckdb/query59.preql` uses window `lead`, not union join;
`test_fifty_nine` only asserts SQL length < 12000, so it did NOT catch this — and
strict row-scoring of the canonical also `fail`s 100-vs-100, worth a separate look
but out of scope here.)

---

## FIXED 2026-07-04 — union-join-between-rowsets collapse resolved

The doc's root-cause attribution to `_expose_coalesced_key_contents` was WRONG
(disabling it changes nothing). The real defect was three independent holes,
all in how discovery/join-resolution honor authored coalescing key groups:

1. **Group-mate pseudonym leak (the collapse).** The union join makes
   `this_year.code` / `next_year.code` MUTUAL pseudonyms;
   `validate_concept` (discovery_validation.py) marked every pseudonym of a
   found concept as found, so sourcing ONE rowset satisfied the other side's
   key and discovery completed with a single node — the entire other side
   dropped (case D, 7 vs 16 854). Fix: a distinct member of a coalescing
   (`full`/`union`) key group is never satisfied through a group-mate
   pseudonym unless the node's subtree materializes the mate itself; the mate
   still lands in `found_map` so the authored relation keeps the stack
   connected (`BuildEnvironment.distinct_scoped_join_group_mates`, restricted
   to INCOMPARABLE-derived groups — SUBSET/LEFT substitution is exempt).
2. **Derived key never materialized.** For `this_year.wk = next_year.wk - 52`
   between two independently-sourced rowsets, nothing computed the derived
   expression, so the completion merge joined on `code` alone (fan-out).
   Fixes: `gen_rowset_node` materializes producible coalescing derived keys
   onto the rowset node (`_materializable_derived_join_keys`), and
   `MergeNode._resolve` re-grew the deleted exposure injection
   (`_inject_scoped_join_key_exposure`) so every merged side surfaces its own
   member of each authored key group.
3. **Authored pairs pruned as redundant.** `reduce_concept_pairs`
   (join_resolution.py) FD/grain-pruned the plain `k = k` co-key because the
   right side's grain was covered — sound within one entity, unsound across
   independently-authored ∦ sides. Authored coalescing members
   (`DomainGraph.coalescing_relation_members`, INCOMPARABLE only) are exempt
   from all three prunes.

Regression cells: `tests/join_matrix/test_independent_rowset_matrix.py`
(single-key keys-only — the smoking gun; single-key + measures; composite
plain; composite derived; python-oracle expected rows) and the fuzzer
`independent_rowset_join` family (12/12 pass; the `key_only` oracles were
corrected to project the coalesced group value, matching the ruled semantics
in `tests/test_scoped_join_permutations.py`).

Related-but-separate finding (pre-existing, NOT this regression): the fuzzer
`chasm` cases exposed that `sales`/`returns` bound `group_id` WITHOUT `~`,
i.e. lyingly declared complete — rule-B directional narrowing then correctly
(per ruled lying-declaration semantics) collapsed a union join to INNER.
Fixed by marking those fact FK bindings `~` partial (models.py) and updating
the chasm oracles to the groups-domain-preserving results.
