# Handoff — v4 search cost

Status after s51: **A1 (truncation reporting), A4 (memoization) and the test
gaps are DONE.** The dead-code question is answered. Branch-and-bound is still
open, but the diagnosis that motivated it was wrong and the corrected one is
below — read §"Branch-and-bound" before picking it up.

Owner: unassigned. Prereqs: none. Expected size: one session.

## Where the numbers stand

**Measure call counts, not seconds.** Wall-clock on this machine is spiky —
repeat runs of the same corpus vary 45–75 s, and an in-process A/B swung
further. Call counts are deterministic and are what the tables below use.

q05 is the widest request in either corpus; `cProfile` before and after:

| q05 | before | after |
|---|---:|---:|
| total function calls | 369,825,763 | **84,818,513** |
| `_functional_into` | 28,864,284 | **9,150,483** |
| `binds_fully` | 48,854,158 | **0** (set membership) |
| `dict.get` | 96,238,378 | **30,446,259** |
| `_label_chain_state` | 855,818 | 855,818 |
| `_compute_pending_obligations` | 24,388 | 24,388 |

The last two rows are the control: enumeration states and label walks are
**unchanged**, so the search explored exactly the same space and only stopped
recomputing inside it. Corpus-wide, `search_sources` calls went **409 → 274**
(the per-request memo). `v4_sql_snapshot.py check` stayed **109/109 identical**
at every step, and the full suite passes 6221.

Reproduce:

```
.venv/Scripts/python.exe local_scripts/v4_search_census.py both     # call counts
.venv/Scripts/python.exe local_scripts/v4_q05_profile.py query05    # cProfile
.venv/Scripts/python.exe local_scripts/v4_sql_snapshot.py check     # 109/109
```

## What landed

**A1 — truncation is reported.** `SearchResult.truncated` is now derived from a
typed `SearchLimit` (`COVERS` / `STATES`) naming the budget that was spent.
`SearchResult.exhausted` separates the two failure modes the fallbacks used to
treat identically: truncated-with-a-plan (valid, may not be cost-minimal) vs
truncated-with-nothing (which is *not* a decline — it makes no claim that no
solution exists). `_report_truncation` in `source_planning.py` logs a warning at
the `_network_source` call site naming the limit, the candidate count and the
terminals.

**A4 — two memos, plus four the handoff did not anticipate.** The per-request
memo is `V4History.search_cache`, keyed on `SourceNetwork.signature()` — a
structural key over terminals, candidate bindings, address grains, join
requirements and axis families. It holds no build information, and a fresh
`V4History` is minted per statement and per nested sub-build, so it is scoped to
one build request by construction. `_pending_obligations` is memoized on the
network's own `_obligation_cache`.

Those two were roughly half the win. The rest came from profiling rather than
from the handoff's list: **90% of q05 was inside `_enumerate_covers`**, and
inside that, 28.8 M calls to `_functional_into` and 48.8 M to `binds_fully` over
a network of 46 candidates. Four quantities there are cover-INDEPENDENT and are
now memo tables on `SourceNetwork` alongside `join_keys`:

- `functional_into(origin, target)` — was recomputed per state
- `row_complete(node)`
- `full_binders(address)` — the candidates binding it fully, so the hot tests
  become one set membership
- `chain_completers(address)` — candidates that can END a labeling chain for it
  (bind it fully, or reach a full binder). This one also replaces the
  `labelable` obligation's precheck, which was re-deriving it per state.

`_label_chain_state`'s frontier now tests the cover-independent
`chain_completers` membership FIRST, which spares the reachability walk for
every candidate that could never complete the chain.

One trap worth knowing: `_enumerate_covers` pushes onto a LIFO stack, so the
push ORDER decides which covers survive truncation. An early draft iterated
`full_binders(address)` directly and made that order depend on frozenset
iteration — i.e. on `PYTHONHASHSEED`. It still walks the sorted `binders()`
tuple and tests membership. Keep it that way.

**Dead code — both answers were "the docstring is wrong", not "delete it".**
`SolutionCost.dominates` is NOT dead: `local_scripts/s32_network_shadow.py`
classifies a network pick against the legacy plan with it. The false part was
the class docstring claiming the search compares as a partial order when
`search_sources` takes a lexicographic `min`; both are now stated.
`ConditionFit` keeps all five labels, with a docstring saying plainly that only
`disqualifying` and `partial_is_full` are read and that the other four are
inert. APPLIES is the natural input to a push-down cost axis, but whether the
WHERE lands on the scan or post-merge is a SHAPE question — it belongs to
`handoff_v4_shape_debt.md`, not to source selection. The
`test_v4_network_search.py` assertion that APPLIES is produced therefore stays.

**Test gaps — all four closed** (`test_v4_network_search.py`, 25 → 37 tests):
`TestConnectivityObligation` (one-hop bridge, multi-hop bridge built through the
fixpoint, and the unbridgeable split that must decline rather than fabricate a
join), `TestSearchBudget` (both limits, `exhausted` vs decline, and the
no-limit case), `TestRowPartialChains` (a row-partial middle terminates a chain
but does not extend it — with the row-complete twin as the control), and
`TestUnofferedProbePinning` (strips the probe's edges from a captured real graph
and asserts `_pin_unoffered_probes` re-supplies the binding as `injected`).
`TestSearchMemos` covers signature stability and separation, and asserts the
obligation memo does not change the answer.

## Branch-and-bound — the premise in §0.2 is wrong

> **Superseded, with a fix already spiked.** The diagnosis below stands, and the
> follow-through is now its own handoff with measured evidence and a
> plan-neutral variant: **`local_scripts/handoff_v4_arm_union_branching.md`**.
> One correction to the analysis below: the arms enter via the **`labelable`**
> obligation (21,693 branches), not `cover` (65). Go read that handoff instead
> of restarting from here.

**Do not start by writing a cost lower bound.** Measured with
`local_scripts/v4_cover_yield.py`:

```
query05  covers=4096  limit=cover_limit  disconnected=0  distinct_reduced=1
query66  covers=1417  limit=-            disconnected=0  distinct_reduced=32
query23  covers=2714  limit=-            disconnected=0  distinct_reduced=40
```

q05's 4,096 covers reduce to **exactly one** source set. There is no cost spread
to prune against — a lower bound would prune nothing, because every branch ends
at the same answer.

The cover-size histogram says what is really happening:
`{7:1, 8:12, 9:66, 10:220, 11:495, 12:792, 13:924, 14:792, 15:495, 16:220,
17:66, 18:12, 19:1}` — that is C(12,k) for k=0..12. Dumped with
`local_scripts/v4_q05_covers.py`:

- the **base cover is 7 sources** — five partition-family UNION candidates
  (`web_sales_unified-catalog_sales_unified-store_sales_unified` and friends)
  plus two date dimensions — and at that cover the binding profile is 2 on all
  15 terminals with **zero pending obligations**. It is already the answer.
- the **12 optional sources are the individual partition ARMS**
  (`ds~s.web_sales_unified`, `ds~s.store_returns_unified`, …). Each arm is a
  legal partial satisfier of some `cover` obligation, so the search branches
  onto it; the union that subsumes it gets pulled in anyway; and `_reduce` then
  discards the arm. Every subset of the 12 becomes a distinct emitted cover.

So the target is **arm-vs-union branching**, and the question to answer first is
whether an arm should be offered as a `cover` satisfier at all when a union
candidate over the same family binds the terminal fully. Note the tension: the
design's own rule is that a `cover` obligation admits partial binders ("partial
suffices here — the upgrade to a full binder is a soft branch"), and
`test_whole_population_request_picks_the_union_over_one_arm` depends on arms and
unions coexisting as candidates.

Two shapes worth evaluating, in this order:

1. **Do not branch onto a partial satisfier that a full satisfier in the same
   partition family subsumes.** Cheapest, and aimed exactly at the measured
   waste. Needs care that it stays a statement about the family, not about
   partiality in general — a partial binder with no subsuming union (a returns
   table) must still be a satisfier.
2. **Prune states that strictly contain an already-emitted cover.** Simpler to
   state, but NOT obviously sound: `_blend_joins` is minimised over spanning
   trees, so an extra source can genuinely lower it by supplying a functional
   path, and the soft-branch upgrade of a partial terminal is exactly a
   superset push. If you try it, the 109/109 snapshot gate is the arbiter.

Whatever you do, `v4_sql_snapshot.py check` must stay 109/109 and q05 must stop
hitting `COVER_LIMIT` — the warning is now in the log, so you can see it go.

## Still open, lower priority

- `direct_miss 17` in the census is unexplained. The neighbouring worry is
  settled: the census now splits `search_exhausted` from `search_declined` and
  reports **0 exhausted, 8 declined**, so no request is currently falling
  through to `_direct_source` because the search ran out of budget. q05 is the
  only truncation and it keeps its plan. Re-check this after any change to
  `COVER_LIMIT` or to the enumeration.
- `build_source_network` itself is not memoized. It was never the bottleneck
  (search was 114.9 s of 125 s) but it is now a larger share of a much smaller
  total; measure before assuming it is worth doing. Note its result is what
  `_network_source` reads AFTER the search (bindings, equivalence), so a memo
  there is not interchangeable with the search memo.
