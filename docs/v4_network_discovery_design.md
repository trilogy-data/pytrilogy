# v4-native network discovery — single-pass source search (design)

Status: **LANDED — THE ONLY PLANNER**. The legacy recursive planner and its
`CONFIG.use_v4_discovery` switch have been removed from the tree, so there is
no longer anything to compare against or fall back to. What remains is SHAPE
and COST, not correctness — see §0.10, which is the only section you need for
current state. §0–§0.9 below are the migration's history, kept because every
rule in the search was earned by a specific defect recorded there; read them
for *why* a rule exists, not for what the code does today.

Comparison figures against the legacy planner throughout this document are
dated measurements from the migration, not reproducible checks.

## 0.10 Current state (2026-07-30, s50) — read this first

**Correctness.** The known-failing registry reached 0 entries (s49, closed
2026-07-29) and has since been deleted along with the legacy planner it
tracked gaps against. Sessions s39–s49 burned the registry 30 → 0; the per-session
mechanisms are indexed in session memory and in git history, and the four
final ones (aggregate-axis grain expansion, the global computed-origin
RELATION rail, RECURSIVE groups never hosting row atoms, pseudonym-origin
materialized args) are recorded in the s49 record.

**But an empty registry is not the same as no gaps, and s50's size audit found
one the batteries cannot see: TPC-H q02 silently DROPPED a WHERE atom.**
(FIXED s50, along with a SECOND defect its regression query uncovered; the
registry is back to ZERO. Both diagnoses are below.)
`where … and supplier.nation.region.name = 'EUROPE'` has *zero* effect on v4's
generated SQL — the SQL is byte-identical with the atom present and removed
(under v3 it is not). Placement tracing shows why: the atom is hosted on
`grp:[@condition]filter:…`, the internal filter group of
`min(supply_cost ? supplier.nation.region.name = 'EUROPE')`, so it restricts
the aggregate's INPUT and never the output population. FINAL does not
re-assert it. The battery passes because the exposure needs a non-European
supplier whose cost ties the European minimum, which TPC-H's generator does
not produce at sf=0.1 **or sf=1** (measured: 0 such rows). Inject one row and
v4 returns an INDONESIA supplier that v3 and `PRAGMA tpch(2)` do not:

```
INSERT INTO partsupp VALUES (18139, 855, 999, 418.71, 'synthetic tie row');
-- v3: 44 rows   v4: 45 rows
```

A census over both corpora finds **9 atoms** hosted on a `[@condition]filter`
group (tpcds q04/q11 ×2/q30/q30-alt/q74/q81, tpch q02). The other eight are
row-verified by the TPC-DS battery, so hosting an atom in a d1 scope is not
wrong per se.

**The fix — `_nested_scope_swallows_atom` (`condition_placement.py`).** A d1
scope is reached through the WHERE, so its groups sit lineage-UPSTREAM of the
statement's rows and `_upstream_most` elects them over a ROOT that could host
the atom just as well. That is harmless exactly when the scope's value
re-enters the outer plan keyed BY the atom's own concept, and a silent drop
when it does not. Two conditions, both required, and the atom is then forced
onto a non-nested host:

1. a candidate FILTER scope's own condition already implies the atom (placing
   it there restricts nothing even locally); AND
2. no nested candidate exposes the atom's row inputs in its GRAIN, so the
   scope's filtering cannot propagate outward through the join that reads its
   value.

q02 fails (2): `min(supply_cost ? region = 'EUROPE')` is grouped by part id and
joined back on part id, so region never reaches the outer rows. q30/q30-alt/q81
satisfy (2): their `avg(… ? year = Y and state is not null)` is grouped BY
state and joined back on state, so the placement is correct — and forcing them
out costs a second `web_returns` scan (measured: the earlier, blunter rule that
dropped all nested hosts broke q04, q30-alt and q74).

Two earlier attempts are recorded because each was disproved by measurement,
not by review: "prefer any non-nested host" cost three queries their rows, and
"drop only the implying filter group" left the atom on the d1 AGGREGATE reading
it — equally nested, equally invisible to the output.

Gates: goldens **109/109 identical** (zero TPC-DS shape change), TPC-DS battery
107 passed, TPC-H 28 passed + 1 xfailed, join_matrix + engine + core/processing
+ join_resolution + scoped_join 1482 passed with the xfail/xpass sets unchanged,
mypy/ruff/black clean. Both branches of the rule are pinned by
`tests/core/processing/test_v4_condition_placement.py`; the guard was A/B'd
(with the rule disabled, the fix-branch test fails and the control passes).
Cost: q02 grows 3,412 → 3,811 chars, because it now applies a predicate it was
dropping.

**A second, distinct defect — also fixed.** The regression query written to
guard the first one at sf=0.1 (`tests/modeling/tpc_h/query02-region.preql` plus
its own reference SQL, run through `run_query`'s new `sql_file=`) still returned
**100 rows against the reference's 47**, and probing showed the aggregate's own
filter was irrelevant: ANY aggregate in the WHERE reproduced it.

The chain: an aggregate WHERE-arg is a search terminal no datasource binds, so
`search_sources` reports it unreachable and `_network_source` declines the whole
conditioned ROOT request. That decline is INTENTIONAL and documented — a
row-shape-barrier arg "must be sourced through its own node and joined — left to
`gen_root`'s `_resolve_root_condition_sources` fallback, which the bridge
triggers by failing to source the arg here". The bug was in that fallback: it
sources the un-produced WHERE args in a separate `search_concepts` seeded only
with the ARGS' own grain keys, so `region.name` was materialized at
(region, part) grain and rejoined to the output scan on part alone — asking
"does this part have SOME European supplier" instead of "is THIS supplier
European". `_resolve_root_condition_sources` now also seeds that search with the
NODE's own row identity (its grain, or the KEY concepts it outputs when it has
none yet — a freshly sourced ROOT scan has no grain, which is exactly the case
that broke), falling back to the unseeded search when that identity is
unbindable so a hard case cannot cost the filter entirely.

Gates: goldens **109/109 identical**, TPC-DS battery 107 passed, TPC-H **29/29**
(the registry entry xpassed and was removed — **registry back to 0**),
join_matrix + engine + core/processing + join_resolution + scoped_join 1454
passed with xfail/xpass sets unchanged, mypy/ruff/black clean. Cost: q02
3,811 → 3,947 chars; TPC-H ratio 1.038 → 1.043.

**Two lessons worth keeping.** First, the initial read of this second defect
was wrong: it looked like a merge-join-key bug ("two scans sharing
`supplier.id` merged on `id` alone"), and that framing would have sent the fix
into the join builder. Isolating it against a query with NO filter scope and a
plain `min(...)` — where the symptom persisted identically — moved the cause to
the condition-source fallback. Reproduce the ingredient, don't read the SQL and
guess. Second, a regression test written to guard one bug found a second,
unrelated one in the same shape; the sf=0.1 variant earns its place in the
suite independently of the bug it was written for.

**Systematic predicate audit (s50).** Both q02 defects were silent wrong rows
that no gate could see, so the class was swept rather than assumed unique.
`local_scripts/v4_predicate_audit.py` deletes each AND-atom from a statement's
WHERE, regenerates, and diffs the SQL; a byte-identical result means the atom
did nothing. It runs both planners, and the differential is the triage signal
(v3 effective + v4 no-op = suspect). It works on the parsed statement, not the
query text, so it is exact rather than format-dependent, and it is validated
both ways (it flags a duplicated atom and ignores two independent ones).

Over both corpora (131 queries): **4 suspects, 4 redundant under both planners,
1 v4-stricter, 0 errors** — and q02 no longer appears, which is the fix
confirming itself. Triage:

- **tpch q18** `order.id is not null` — NOT a bug. The plan's
  `INNER JOIN … ON order_id = order_id` cannot match a NULL, so the atom is
  enforced by construction. Latent fragility only: if that join ever became
  LEFT, nothing would enforce it.
- **tpcds q11** `channel in (...)` and `year in (...)` — NOT bugs. Each
  aggregate already filters to one channel and one year, and
  `store_first_year > 0 and web_first_year > 0` restricts the customer set to
  exactly those rows, so the atoms are implied.
- **tpcds q11 `sales.billing_customer.sk is not null` — CONFIRMED WRONG ROWS.**
  The aggregate groups BY that key and the dimension is reached through a LEFT
  OUTER JOIN, so the NULL group survives as an all-NULL output row. q11 passes
  only by luck of the data: its NULL group clears both `> 0` filters
  (store 11,683,560 / web 5,595) but fails the ratio test (0.012 vs 0.975). Add
  one web-2002 row for the NULL customer and v4 returns an extra
  `(None, None, None, None)` that v3 and the reference do not.

Distilled to an undoctored repro on the shipped dataset — an aggregate grouped
by a nullable key, `where key is not null`, selecting a dim attribute: **v3
98,992 rows, v4 98,993**. Landed as
`test_not_null_on_aggregate_grain_key_is_enforced` and tracked in the registry
(**0 -> 1**).

**Where it is NOT.** Placement is correct (the atom lands on `grp:root:root:∅`
and propagates to `root_d1`), the atoms survive `_root_atoms_satisfiable_from`,
and the nodes returned by `build_node` carry the condition — verified by
instrumentation at each step. It is also not an optimizer: toggling
`strip_redundant_not_null` and `predicate_pushdown` changes nothing. The loss
is in **plan assembly, downstream of `build_node`**: walking the final strategy
tree shows NO node carrying the atom, so the conditioned node is built and then
DISCARDED in favour of the bare union node. Full handoff with the repro, the
ruled-out list and the gates: `local_scripts/handoff_v4_predicate_assembly_drop.md`.

**Flags.** There are none left. The network search is not optional, it IS
sourcing; `use_v4_network_search` and then `use_v4_discovery` were both
removed as the migration closed.

**Size, measured over both benchmark corpora** (2026-07-30,
`local_scripts/v4_size_report.py`, full report in
`local_scripts/v4_size/REPORT.md`):

| suite | v3 chars | v4 chars | ratio | smaller | identical | larger |
|---|---:|---:|---:|---:|---:|---:|
| TPC-DS (109) | 524,631 | 450,223 | **0.858** | 43 | 36 | **30** |
| TPC-H (22) | 30,789 | 31,974 | **1.038** | 8 | 8 | **7** |

(TPC-H post-fix: q02 grew by 399 chars applying the predicate it had been
dropping. The pre-fix figure was 31,575 / 1.026.)

v4 wins decisively in aggregate on TPC-DS and **loses on TPC-H**. TPC-H was
never in the shape-audit loop — every earlier size number in this document is
TPC-DS-only — and it is where the residual defects concentrate. Against
hand-written reference SQL both planners sit at ~2.3×, which is the real
headroom.

The migration's structural promise is confirmed and the debt is specific
(`local_scripts/v4_sql_symptoms.py`, TPC-DS): repeated table scans 113 → **66**
and dedup GROUP BYs 83 → **59** (the S3 twin-scan family dissolving, exactly as
§0.1 argued), against unfolded passthrough CTEs 24 → **36** and split
aggregates 33 → **39**. The two debts are one coupling: a same-parent dedup
bucket beside an aggregate gives the scan two consumers, which blocks
`collapse_single_parent` from folding either back in.

**Generation cost: resolved down to one residual (s51).** TPC-DS generation was
37 s under v3 and 125 s under v4, with a census
(`local_scripts/v4_search_census.py`) attributing 114.9 s of it to
`search_sources` itself.

Wall-clock on the dev machine is too spiky to quote (repeat runs of the same
corpus vary 45–75 s), so the metric is **call counts, which are deterministic**.
For q05, the widest request in either corpus, `cProfile` before and after:

| q05 | before | after |
|---|---:|---:|
| total function calls | 369,825,763 | **84,818,513** |
| `_functional_into` | 28,864,284 | **9,150,483** |
| `binds_fully` | 48,854,158 | **0** (set membership) |
| `dict.get` | 96,238,378 | **30,446,259** |
| `_label_chain_state` | 855,818 | 855,818 |
| `_compute_pending_obligations` | 24,388 | 24,388 |

The last two rows are the control: the number of enumeration states and label
walks is **unchanged**, so the search explored exactly the same space and simply
stopped recomputing inside it. Corpus-wide, `search_sources` calls went
**409 → 274**. `v4_sql_snapshot.py check` stayed 109/109 identical throughout —
every change was a memo or a reporting fix, none touched what the search
decides. What was done, and what it taught:

- **Truncation is no longer silent.** `SearchResult.truncated` is now derived
  from a typed `SearchLimit` naming which budget was spent, `_network_source`
  logs a warning at the call site, and `SearchResult.exhausted` distinguishes
  the two cases the fallbacks used to treat identically: a truncated search
  that still has a plan (valid, maybe not cost-minimal) versus one that emitted
  no cover at all. The latter is NOT a decline — it makes no claim that no
  solution exists — and `plan_source` falling through to `_direct_source` on it
  is a guess, which the log now says out loud.
- **Identical searches were recomputed inside one query.** A memo on
  `SourceNetwork.signature()` — a structural key of terminals, candidate
  bindings, address grains, join requirements and axis families, holding no
  build information — lives on `V4History.search_cache`, so it is scoped to one
  build request. 409 searches became 274.
- **The dominant cost was not the algorithm but recomputation inside it.**
  Profiling q05 put 90% of the time in `_enumerate_covers`, and inside it 28.8 M
  calls to `_functional_into` and 48.8 M to `binds_fully` over a 46-candidate
  network. Four quantities are cover-INDEPENDENT and are now memoized on the
  network: `functional_into`, `row_complete`, `full_binders` (which candidates
  bind an address fully) and `chain_completers` (which can end a labeling chain
  for it). q05's search went 35.3 s → 5.7 s on its own.
- **Branch-and-bound is still open, but §0.2's premise is wrong.** q05's 4,096
  covers were measured (`local_scripts/v4_cover_yield.py`) to reduce to
  **exactly one** source set, so there is no cost spread for a lower bound to
  prune against. The cover-size histogram is C(12,k) for k=0..12: a 7-source
  base cover (5 partition-family UNION candidates + 2 date dimensions) that
  already binds every terminal fully with zero pending obligations, times the
  powerset of 12 individual partition ARMS. Each arm is a legal partial
  satisfier of a `cover` obligation, gets picked on some branch, and is then
  discarded by `_reduce`. The waste is arm-vs-union branching, not a missing
  bound — see `local_scripts/handoff_v4_search_cost.md`.

**Corrections to the history below.** Two statements in §0.9 are superseded:
`_connect` is no longer a greedy bridge fabricator — it is a pure connectivity
CHECK, and bridging is a `connected` obligation discharged by the same
machinery as every other invariant (see `_pending_obligations`), so every
alternative bridge enters dominance. And `local_scripts/s34_handoff.md` is
stale as of s38 (it still reports a 30-entry registry); the shape audit it
pairs with was deleted in the working-set purge and lives only in git history
at `51cadbc48^:local_scripts/v4_shape_audit.md`.

**Open, in priority order.** (1) The passthrough/split-aggregate coupling — highest measured size value, and
TPC-H makes it unavoidable; handoff at
`local_scripts/handoff_v4_shape_debt.md`. (2) Search cost: truncation
reporting, then the search memo, then branch-and-bound; handoff at
`local_scripts/handoff_v4_search_cost.md`. (3) Predicate pushdown back onto
scans (TPC-H q20's `CANADA` is deferred past a join).
(4) The recorded S4/S5 and provider-choice families.
(5) Nullability is still not modeled (§0.1). (6) Condition labels beyond
`SENSITIVE`/`IMPLIED_EXACT` are computed and never read (§3 promised them as
dominance inputs; `APPLIES`/`UNAFFECTED`/`DEFERRED`/`NEUTRAL` have no
consumer). (7) Stage D is still an adapter: the search prices `completions` and
`partial_terminals`, but `_network_source` passes only `sources` and
`connectors` to the legacy bridge emitter, which re-derives the rest — so the
§3 "emit a solution, not a graph" contract is unfulfilled and the two can
disagree.

### Module layout (s57) — §3's "one module" is now six

The search outgrew the single `network_search.py` §3 planned for it (1,964
lines). It is now a layered stack under `v4_helper/`, ordered so nothing above
the bottom layer can reach a build model:

| module | holds |
| --- | --- |
| `network_model` | the vocabulary — `SourceCandidate`, `SourceNetwork`, `Obligation`, `SolutionCost`, and the answers derivable from the labels alone |
| `network_build` | stage A: labeling the network. **The only module that reads build models** |
| `network_coalescing` | stage A: presence-probe pinning and union-join axis families |
| `network_topology` | what a chosen set of sources looks like — components, blends, declared-relation pairing |
| `network_obligations` | what a partial cover still owes |
| `network_search` | stages B/C: enumerate, reduce, cost, choose |

That layering is the point, not the line counts: "this module is pure — it
selects sources and reports why, but builds no StrategyNodes" used to be a
claim in a docstring and is now an import boundary. `network_topology` is
shared by obligations and cost deliberately — the search DEMANDS a structure
that the cost CHARGES for its absence, and a predicate stated twice lets the
search discharge an obligation the cost still charges for (the merged-key
predicate had five spellings before s55).

Three things died in the same pass rather than being carried across: 
`SolutionCost.dominates` (its only reader, the s33 shadow harness, went with
the ladder), `_reduce`'s `baseline` obligation set (measured: 3,492 `_reduce`
calls across both corpora, **zero** with a non-empty baseline — every cover
`_enumerate_covers` emits has an empty pending set by construction, and the
`_connect` its docstring credited has not existed since s55), and
`_functional_reach`, which had no production caller and now lives in the test
that pins `chain_completers` against it. Gate: corpus byte-identical 132/132.

## 0. Progress (s32)

**Landed (inert — no production code path imports it):**

- `trilogy/core/processing/v4_helper/network_search.py` — stages A–C:
  `build_source_network` (labeled network), `search_sources` (cover enumeration
  → connectivity → dominance prune), `SourceSolution` / `SearchResult`.
- `tests/core/processing/test_v4_network_search.py` — 11 tests over real parsed
  models (labels, union-vs-arm, twin-scan dominance, unreachable terminals,
  determinism).
- `local_scripts/s32_network_shadow.py` — shadow harness: monkeypatches
  `plan_source`, runs both planners per request, compares the chosen physical
  scan sets (union candidates expanded to their children), classifies and
  reports. `local_scripts/s32_binders.py` — "who binds this address, how, and at
  what grain" probe. `local_scripts/s32_solution_detail.py` — per-request
  breakdown of every non-dominated alternative with its assignment, join keys
  and fan-out attribution, alongside the legacy choice scored the same way. The
  detail probe is what turned "27 incomparable trades" into two concrete
  defects; the summary table cannot show you a source that provides nothing.

**Measured, full TPC-DS corpus (109 queries, 362 ROOT source requests):**

| run | what changed | match | diverge |
|---|---|---:|---:|
| 1 | first cut, no union candidates | 291 | 67 |
| 3 | + union candidates, union-normalized comparison | 308 | 50 |
| 5 | + grain fan-out cost axis | 295 | 63 |
| 9 | + cover minimality, un-laundered fan-out, decomposable terminals | **315** | 45 |

Zero exceptions across all runs; 2 requests where both planners decline
(a derived concept another group supplies). As of run 9 there are **no
requests where the network fails to find a solution**.

**What the divergences taught us (each fix was driven by the data, not by
reading legacy code):**

1. **Union candidates are stage-A material** (fixed). Without them the search
   answered a whole-population request from one partition arm — the s10 enum
   bug reproduced from first principles. A partition family is a candidate
   source that binds the discriminator FULLY; each arm binds it partially.
2. **Coverage is not binary** (fixed). The cover enumeration originally treated
   a partial binding as "covered" and never explored the full binder, so the
   completion join v3 makes by hand was never on the table. Partial covers stay
   candidates, but every full binder is still branched on and dominance decides.
3. **A cost axis must be local to its source** (fixed, then fixed properly).
   The first fan-out axis compared each source's grain against the whole
   solution's carried addresses — so adding a redundant source widened the
   yardstick and *lowered* the cost. Narrowing it to "what the source
   contributes **plus the keys it is joined on**" did not close the hole: the
   join keys are also solution-dependent, so a cover could still launder its own
   fan-out by adding a source joined on the fanning source's grain key (q94,
   q10, q54 all did exactly this). Fan-out is now judged against what the source
   contributes and nothing else. A source contributing nothing survives only as
   a connectivity bridge, so there its join keys ARE its contribution.
4. **A non-minimal cover is invalid, not merely costlier** (fixed). The cover
   enumeration branches on every binder, so it produced supersets — and the
   fan-out axis then actively *preferred* them, because the extra source
   laundered the fan-out. Concretely, q10's winning cover carried
   `catalog_sales_unified` as a redundant sibling of the union that already
   contains it, providing nothing, joined on 27 columns; q94/q95/q16 bolted a
   `*_returns` scan onto a pure sales request. Every added source is a join, and
   a join with a source that binds nothing new can only restrict the population
   (an inner join onto a narrower row set) or fan it out — a wrong-rows change.
   `_reduce` therefore drops any source the rest of the cover makes redundant,
   keeping the pre-filtered `IMPLIED_EXACT` member when two are mutually
   redundant. This is what collapsed `incomparable` from 27 to 4: most of those
   "trades the cost model cannot separate" were not trades at all.
5. **A derived terminal over requested terminals is not a sourcing
   requirement** (fixed) — the §5 `filter_downstream` carry-over. `is_returned
   <- _returned_ticket is not null` is an expression, not a column, so no
   candidate binds it and the search declared it unreachable (q29, q84: the only
   two no-solution cases). It is computed inline once the sources are joined.
   The parents must be **requested**, not merely bindable: dropping a derived
   terminal whose parent nothing else asks for would lose the requirement rather
   than relocate it. Only BASIC lineage qualifies — an aggregate, window, filter
   or rowset output is its own opaque unit and anchors a join.

**Triage of the 45 divergences** (`local_scripts/s32_shadow_verify.log`). The
shadow scores the LEGACY choice under the same cost model, so each divergence
classifies itself — re-run any time with
`.venv/Scripts/python.exe local_scripts/s32_network_shadow.py`:

| verdict | n (run 5) | n (run 9) | meaning |
|---|---:|---:|---|
| `network-dominates` | 33 | 38 | the network's pick is better on some axis and worse on none — candidate wins, need row-level confirmation |
| `incomparable(needs axis)` | 27 | **4** | a trade the dominance test cannot separate |
| `legacy-uncovered` | 3 | 3 | legacy plans NOTHING for the request; the network does — see §0.1, this is a hazard, not a terminal-set artifact |
| `network-declines` | 2 | **0** | legacy plans, the network finds no solution |
| `LEGACY-DOMINATES(search bug)` | **0** | **0** | no case where the legacy plan is strictly better |

**Zero `LEGACY-DOMINATES` is still the headline**: under a shared cost model the
legacy planner never strictly beats the network.

The 4 surviving `incomparable` cases were read individually and are all the same
shape — **the network is better on the axes that lead, and pays on one that
trails**, so only the strict dominance *test* calls them incomparable; the
lexicographic pick already prefers the network:

- q29 legacy `(3,3,1,2,0,0)` vs network `(0,0,1,3,0,0)` — three partial
  terminals and three completion joins traded for one more source.
- q97-one / q97-two legacy `(2,2,2,4,0,0)` vs network `(0,0,2,6,0,0)` — same
  trade: legacy leaves `customer.sk` / `item.sk` partial and completes them
  later, the network binds them outright.
- q64 legacy `(0,0,0,15,10,1)` vs network `(0,0,1,6,1,0)` — nine fewer sources
  and nine fewer connectors for one fan-out.

So there is **no missing axis** — the earlier "27 trades" were an artifact of
non-minimal covers and launderable fan-out, and dissolved when those became
invalid rather than costly.

## 0.1 Divergence anatomy and the row-level verdict (s33)

Parsing `s32_shadow_verify.log` by what changed, rather than by verdict, collapses
the 45 divergences into three families:

| family | n | shape |
|---|---:|---|
| network drops sources, adds none | **35** (34 `network-dominates` + 1 `incomparable`) | a redundant dimension scan, over 16 queries (incl. `query97.preql` ×2) |
| both add and drop | 5 | q64 ×4 (legacy's 12-source blob → 1 dimension), q29 ×1 |
| network adds sources, drops none | 5 | `query97-one`/`-two` ×4, q29 ×1 — **legacy planned nothing** |

The dropped instances are overwhelmingly one thing: `sales.item.items` ×25,
`sales.billing_customer.customers` ×12, `cs.item.items` ×7. The `s32_solution_detail`
breakdown of q78 shows the exact shape — legacy's assignment for the extra source is
`provides: ['sales.item.sk']`, `joined_on: ['sales.item.sk']`. It contributes exactly
the FK the fact table already binds: the S3 twin scan, verbatim.

**Verdict: every drop is row-preserving, so all 34 are wins.** The authority for
this is the MODEL, not the warehouse — the planner provisions from declarations
(`?` nullable, `~` partial, `grain (...)`, `subset join`), so a fact that happens to
have no NULLs in sf=1 proves nothing the planner may rely on. Grounded that way:

1. **Subset-join semantics settle the family outright.** `subset join a = b` asserts
   a ⊆ b, so the subset side's values are *declared* present in the superset's
   domain. Reading the key from the subset side alone is therefore complete, and
   pulling in the superset's own table buys nothing — enriching a non-key property
   is the only reason to join it. Only a `union join` would require both sides.
   Legacy's extra scan is precisely the join subset semantics say is unnecessary:
   across the whole family the dropped source's assignment is `provides: [the key]`,
   `joined_on: [the same key]`.
2. **The column modifiers say the same thing per-binding.** `SS_ITEM_SK: item.sk`
   carries neither `?` nor `~`, and `items` declares `grain (sk)` — so *by
   declaration* the join can neither restrict (no NULLs) nor fan out (unique key): a
   provable no-op, covering the 37 `item.items` instances.
   `SS_CUSTOMER_SK: ?customer.sk` IS declared nullable, so an INNER join to
   `customers` restricts *by model*, which makes the drop at-least-as-correct — the
   requested population is the fact's rows, and a sale the model declares may have no
   customer is still a sale.
3. **Corroboration, and what it is actually for.** sf=1 agrees with the model on all
   15 FK pairs the corpus joins: zero orphans, unique dimension keys, `item_sk` never
   NULL, `customer_sk`/`sold_date_sk`/`cdemo_sk`/`store_sk`/`addr_sk` NULL in
   quantity (129,392 of 2,880,404 `store_sales` rows have no customer). Rendered
   joins line up too — q11/q04 emit `LEFT OUTER JOIN customer`; q78 emits `INNER JOIN
   customer` under `where sales.billing_customer.sk is not null`; q04/q14's INNER
   `date_dim` sits under a year filter. This is a **model-validation** check, not a
   planner check: it is how you catch a mis-declaration like the recorded q78
   `is_returned` bound non-nullable → INNER collapse. Where data and model disagree,
   the model is the bug.

The corpus does not exercise an INNER join on a `?` FK with no NULL-excluding
predicate; the drop is still correct there by (2). Worth a regression test, not a
rule change.

### The real defect behind `legacy-uncovered`: `_connect` fabricates bridges

Earlier triage read these as a terminal-set artifact, and a first pass read q97's axis
as "the union of both channels' pairs". Both are wrong. q97-one declares `subset join
store_sales.customer.sk = customer.sk` (and the catalog equivalent), so `customer.sk`
is the SUPERSET; the requested terminals `{customer.sk, item.sk}` are two independent
superset axes and the correct answer is their cross product, no fact table involved.

What the search actually does (probed directly): the cover `{customer.customers,
item.items}` is two components with **no join keys between them**, so `_connect`
fabricates `catalog_sales` as a bridge and `_reduce`'s connectivity guard then
protects it — the chosen solution is `catalog_sales + customers + items` with
`catalog_sales` contributing `provides=[]`. That restricts customer × item to the
pairs that transacted in the catalog channel. Wrong rows.

This is **defect #4's own argument turned on the carve-out that exempts bridges**: a
source binding nothing new can only restrict the population or fan it out, and "it
holds the cover together" does not make that harmless. The principled fix: **a
disconnected cover is not invalid — it is a cross product**, which is strictly safer
than a population-changing bridge, so the bridge carve-out should go. Whether this
request then returns a cross-product solution or keeps legacy's deliberate `None` (the
authored-join path owning the axis) is the open question, and the cutover answers it.
q29's added-only case needs the same read.

Corollary for stage D: do **not** guard it by refusing to plan wherever legacy returns
`None`. That would mask exactly this bug.

### Not modeled: nullability

`network_search.py` reasons about partiality and has **zero** references to
nullability, though `Modifier.NULLABLE` sits right there on the column assignment.
Per `docs/subset_union_join_design.md` these are different things — `~` is a subset of
*values*, `?` speaks to *rows* — and the cost axes cover only the first. It does not
affect the drop verdict above, but bridge admissibility and join-type narrowing both
turn on it.

## 0.2 Cutover, first burndown (s33)

**Stage D landed as an adapter**, behind `CONFIG.use_v4_network_search` (nested under
`use_v4_discovery`; `TRILOGY_V4_NETWORK_SEARCH=1` opts the battery in, restored per-test
in `tests/conftest.py` so it cannot leak). `_network_bridge_plan` in
`source_planning.py` turns a `SourceSolution` into a `BridgePlan`, and the existing
`_datasource_nodes_for_bridge` -> `_merge_component_sources` ->
`_complete_partial_requested` chain emits it unchanged, so every carry-over they
implement stays in force. Flag-off is untouched by construction.

| gate | result |
|---|---|
| TPC-DS generation, 109 queries (`s33_network_burndown.py`) | **109/109** |
| TPC-DS execution vs references, flag ON | **105 passed, 1 failed, 1 xfailed** |
| same battery, flag OFF (v4 ladder baseline) | 106 passed, 1 xfailed |
| `test_v4_network_search.py` | 15 passed |
| `mypy trilogy` / ruff / black | clean |

The cutover costs exactly ONE regression: `test_five` (query05).

**Two adapter defects the burndown found, both fixed:**

1. Concept nodes must be kept by **address**, not by `concept_to_node(...)` — a graph
   concept node carries an `@grain` suffix that need not match the default-grain
   spelling, so keying on the minted node deleted the graph's own node and severed the
   datasource edge. Symptom: `Missing source reference to sales.billing_customer.sk`.
2. `reinject_common_join_keys_v2` is a carry-over that runs INSIDE
   `determine_induced_minimal_nodes`, which this path bypasses. Ported explicitly.

Also landed: memoization of `SourceNetwork.join_keys` (the cover search asks it O(n^2)
times per cover over thousands of covers).

### q05 — diagnosed, and three fix classes RULED OUT

Not a cross-family union bug. `web_sales_unified` declares
`WS_WEB_SITE_SK: ?return_channel_dim_id` — for the web channel a return's site IS the
sale's site, because TPC-DS `web_returns` carries no site key — so
`{web_sales, catalog_returns, store_returns}` is a deliberate, model-sanctioned family.
What happens instead:

- the plain returns family does NOT bind `return_channel_dim_id` (`binds() == False`);
- the network takes plain-returns for the measures plus the `*_dim_return` family for
  the FK *and* its text id, assigning the FK to the dimension keyed BY it;
- no fact-side source materializes the FK, the two share only `s.channel`, and the merge
  join runs on a 3-valued discriminator -> ~4000x fan-out.

Legacy's `cheerful` CTE is a **10-branch UNION ALL** stacking the three `*_dim_return`
arms AND the three fact arms carrying a return site (`CR_CATALOG_PAGE_SK`,
`SR_STORE_SK`, `WS_WEB_SITE_SK`) — one union node assembled from partial sources of
different keysets.

**Ruled out 1 — `unkeyed_joins` as a cost axis** ("a source joined on less than its own
grain multiplies rows"). True statement, fired on exactly the right source, still failed:
key-completeness is solution-dependent hence launderable (defect #3's lesson). The search
satisfied the axis by ADDING a `catalog_page` scan to manufacture join keys, leaving the
bad join untouched.

**Ruled out 2 — the same rule as a CONNECTIVITY predicate** (an edge exists only if the
shared keys cover one side's grain). Looked immune to laundering because connectivity is
monotone. It is wrong on the semantics: it forbids the canonical fact-to-fact conformed
dimension blend. `BRIDGE_MODEL` in `test_v4_network_search` joins `catalog_sales` (grain
`order_number, item_id`) to `store_sales` (grain `ticket_number, item_id`) on shared
`{customer_id, item_id}`, covering NEITHER grain — a core Trilogy operation, not a
defect. Two unit tests caught it at once. A fact-to-fact join covers a grain only when
the facts share their FULL grain; an earlier note in this doc got that wrong.

**Ruled out 3 — building a new union-construction mechanism.** Unnecessary:
`get_union_sources` already emits the hybrid group and it IS in `network.candidates`, and
node assembly already stacks several selected union groups into one CTE (that is what
`cheerful` is). The candidate exists and the emitter can already consume it.

**Residue: a narrow SELECTION problem.** The network must pick the hybrid alongside the
plain returns family. Today (a) the 6-source cover without it is cheaper on `sources`,
and (b) `_reduce` drops it regardless, since its minimality test is profile-based ("can
anyone provide this value?") and the hybrid provides nothing the dimension does not.

What separates q05's bad join from the legitimate blend is NOT key coverage. It is that
`return_channel_dim_id` is a **requested terminal** that is also its provider's own grain
key, where another candidate could have co-located it with the fact side and was not
chosen. That points at `_assign` / terminal provenance, not join topology. UNTESTED —
validate any such rule against `BRIDGE_MODEL` and the twin-scan tests FIRST, which is the
step that killed ruled-out 2.

**Open — the immediate work list:**

- **Stage D as an adapter, then CUT OVER and burn down the diffs.** Shadow triage has
  reached its limit: it cannot tell a win from a wrong-rows change, only that the cost
  model prefers one. With 0 `LEGACY-DOMINATES` and the largest family confirmed, the
  cheapest remaining verification is to make the plan execute and let the batteries
  disagree with it.
  - Stage D does **not** need the §3 `SourceSolution` emission rewrite to start.
    `BridgePlan` is only `(concepts, graph, full_cover_fallback)`: synthesize one whose
    graph holds exactly the solution's chosen datasources plus its terminal and
    connector concept nodes, and feed the existing `_datasource_nodes_for_bridge` →
    `_merge_component_sources` → `_complete_partial_requested` chain unchanged.
    Network = *selector*, legacy = *emitter*. Insertion point: the attempt loop at
    `source_planning.py:1437-1514`; the pinned planners above it keep running first.
  - Then flip it on in-branch and burn down: `v4_sql_snapshot.py check`, the TPC-DS
    battery, join_matrix, gcat + enum_unions. Every failure is either a §5 carry-over
    that did not port or a divergence triaged wrong — both are cheaper to read as a
    failing query than as a cost tuple.
  - Fix the `_connect` bridge rule (above) BEFORE flipping, since q97's shape is
    already known to be wrong; do not add guards that hide declines.
- **Union family selection.** `get_union_sources` produces cross-family groups
  (a sales arm + two returns arms, all `complete where channel = X` on distinct
  values) — `_best_enum_union` keeps them deliberately for q05. They no longer
  win any request now that minimality prunes them, but the search has no rule
  saying why they shouldn't: arms must be interchangeable in POPULATION, not
  just disjoint on the discriminator. Make it a rule before the cutover rather
  than relying on minimality to mask it.
- ~~**Dimension joins the network drops**~~ — **CLOSED (s33)**. Confirmed a genuine
  win by the three checks in §0.1: all 34 are row-preserving, so the whole family is
  the S3 twin scan dissolving as intended. What remains is a regression test for the
  INNER-on-nullable-FK shape the corpus lacks.
- **Enumeration bound**: q05 (15 terminals, 46 candidate sources) is the only
  request that truncates. Memoizing enumeration states made the *exhaustive*
  search 91s → 15s, and the exhaustive answer is identical to the capped one, so
  `COVER_LIMIT` is not currently changing any result — but that is measured, not
  guaranteed. Branch-and-bound on a cost lower bound is still the right end
  state.
- Not yet modeled: derived connectors (`_derived_connector_nodes`), stage D
  emission into `_datasource_nodes_for_bridge`, and the rest of the §5
  carry-over inventory.

## 0.3 Connectivity becomes a graph-correctness requirement (s34)

The s33 residue was framed as "a narrow SELECTION problem" for q05. It is not narrow,
and it is not about q05: **connectivity was modelled as "the chosen sources form one
component", which is far too weak a requirement.** Any shared key satisfies it, so a
dimension can attach to the fact side through a 3-valued discriminator while the key
that actually identifies it sits unclaimed on a candidate nobody selected. Same defect,
different clothes, in the authored-join suites: a merged key sourced ONCE satisfies
coverage while one side of the declared equality has no way to produce it.

The fix is to build the spanning structure out of the keys that IDENTIFY rows, and to
require every declared relation to be materialized on both of its sides. Two rules, one
enumeration repair, and one emitter boundary:

### Rule 1 — the minimum-blend spanning tree (`_blend_joins`)

`joins_functionally(a, b)` asks whether the keys two sources share cover ONE side's
grain. If they do the join is a lookup: it can restrict, never multiply. If they cover
neither it is a **blend** — legitimate when two facts are related only through conformed
dimensions and nothing finer exists (`BRIDGE_MODEL`), a wrong-rows defect when something
finer does (q05). Which one it is, is a property of the whole cover, so the cost is the
number of blend edges in a **minimum-blend spanning tree**: functional edges are free,
and blends are only paid where no functional path exists.

Minimising over spanning trees rather than summing over pairs is what makes this
**un-launderable**, the property `unkeyed_joins` lacked (ruled-out 1). An extra source
adds a node the tree must span, so it can only lower the count by supplying a functional
PATH — co-locating the key, which is the actual fix. Bolting on `catalog_page` to
manufacture keys leaves the fact↔dimension edge exactly as it was and costs a source, so
it is dominated. And unlike ruled-out 2 this is a COST, not a connectivity predicate, so
it forbids nothing: the `BRIDGE_MODEL` fact↔fact blend still plans, it just prices at 1
where nothing can do better.

q05 falls out with no q05-specific rule. The cover with the hybrid
`web_sales + catalog_returns + store_returns` family scores 0 blends against the
6-source cover's 1, and the emitted SQL is now **byte-identical to the flag-off plan**:
`cheerful` joined on `s_return_channel_dim_id` instead of `s_channel` alone.

### Rule 2 — declared relations must be paired on both sides (`_unpaired_join_keys`)

`JoinRequirement` carries a declared relation's canonical build address plus EACH side's
own keys. A side the solution does not touch imposes nothing; a side it reads through a
carrier that cannot produce the merged key has dropped the authored equality, and the
sides then pair on whatever they happen to share (`sku` in the q17/q25 shape). This is
the search-side statement of what `inject_authored_join_key_terminals` asks the Steiner
walk for, and it is the leading cost axis.

The declared relations are read from `relevant_authored_join_pairs` — the SAME source of
truth as the ladder's terminal injection, so the two agree on which relations need
discovery help at all. (Measured separately: the ⊑/≡ *addresses* need no extra handling.
`s34_key_audit.py` finds zero declared edges over the corpus whose endpoints survive as
two distinct bound addresses — build-time canonicalization already unifies them. The gap
was never the key algebra, it was that nothing required the key to appear twice.)

### The enumeration repairs (`_pair_join_keys`, `_colocate_blended_grains`)

Neither rule can be satisfied by dominance alone, because cover enumeration is driven by
COVERAGE: it stops branching on an address the cover already binds. A merged key is bound
the moment one side's dimension is in, and a dimension's own grain key is bound by the
dimension itself — so the better cover is never generated and dominance never gets to
see it. Both are therefore asked for explicitly, the same way `_connect` asks for
connectivity, before `_reduce` runs:

- `_pair_join_keys` adds the far-side hop of a declared relation.
- `_colocate_blended_grains` adds, for a source none of whose joins covers its grain, a
  candidate that binds that grain AND joins functionally to a source OTHER than the
  blended one — otherwise the blend has moved rather than closed.

This is what made the rules *reachable* rather than order-dependent. q05 happened to
enumerate its repair by luck of the scarcest-first ordering; gcat's
`test_aggregate_optimization` did not, and only started passing once the repair was
explicit — the summary-table plan `fuel_aggregates + launch_info` is exactly a blend
co-location (`launch_info` supplies the `org.code`/`vehicle` keys the aggregate table
does not carry).

### `_reduce` is minimality over VALUES *and* STRUCTURE

Both rules would be undone immediately by `_reduce`, whose minimality test is
profile-based: q05's hybrid provides no value the dimension does not, and the far-side
dimension scan provides no value the near side does not. Reduction now also refuses any
drop that worsens `(_unpaired_join_keys, _blend_joins)` — redundant means redundant for
both.

### A one-scan solution belongs to `_direct_source`

Design §4 already said it; the adapter was not honouring it. Routing a single-source
solution through the bridge emitter drops the `GROUP BY` that collapses a scan read at
finer grain than the request (`subset join` rowset-onto-ROOT, duplicate rows). A union
candidate stays on the bridge path — `_direct_source` cannot render one.

`_network_source` now returns a typed `NetworkDecision` instead of a bare `None`, because
the two outcomes have completely different consequences for deleting the ladder: `None`
is a DECLINE that still needs a home, while `bridge=None` is a success whose renderer is
`_direct_source`. `plan_source` calls `_direct_source` directly for the latter rather
than entering the attempt loop — the loop would give `_bridge_plan` first refusal and let
a multi-source join beat a cover the search already judged sufficient (q23).

### Open defect: a union-join key group sourced from ONE arm

`test_full_join_two_keys_single_join` fails, and the single-scan deferral is how it
surfaces. Two `union join` clauses relate the same pair of sources; a later request asks
for `{r.k1, r.k2, r.m2, r.rrow}` and the search correctly observes that `rightt` binds
all four — so it defers to `_direct_source` and gets a second `right_tbl` scan and a
second `FULL JOIN`, where one shared CTE is expected.

The selection is wrong, not just the deferral: `r.k1` is the CANONICAL of a coalescing
(`union join`) key group, so its value is the COALESCE of both members and reading it
from one arm silently drops the left-only rows. This is §0's defect 1 — "a partition arm
binds the discriminator only partially" — in a different costume.

It cannot be expressed as a `JoinRequirement`: after canonicalization `l.k1` and `r.k1`
are the SAME build address, so "bind the canonical alongside this side's keys" is
vacuously true. The requirement is in AUTHOR space — the cover must include a carrier for
each MEMBER's own column (`member_binding_datasources` per member) — which is a second
requirement shape, not a tweak to the existing one. Note `_relevant_authored_join_pairs`
deliberately filters this pair out ("both members are already physical columns, injection
only perturbs the plan"), so the network cannot reuse that filtered list here: for a union
join the both-sides requirement holds whether or not the ladder needed help.

### Presence probes are pinned, not decomposable

Two separate holes, both from the same cause: canonical substitution rewrites a presence
probe's lineage argument to the key group's canonical, which **every** member's
datasource binds identically. So (a) `_decomposable` saw a BASIC expression over a
sourced address and inlined the probe, and (b) the reference graph offers the probe off
both sides and the search read it off the cheaper one. Either way "did THIS side match?"
gets answered with the other side's row — the exact collapse the probe exists to
prevent. The probe is now never decomposable, and it may only be bound by the carrier
`member_binding_datasources` resolves, which is the same datasource
`gen_presence_probe_node` pins.

### Gates

| gate | before (s33) | after |
|---|---|---|
| TPC-DS battery, flag ON | 105 passed, 1 failed | **106 passed, 1 xfailed** — equals the flag-OFF baseline |
| TPC-DS generation (109) | 109/109 | 109/109 (52s -> 71s: the repairs are not free) |
| join_matrix + gcat + enum_unions, flag ON | **12 failed**, 380 passed | **0 failed**, 392 passed |
| same suites, flag OFF | 392 passed | 392 passed |
| `test_v4_network_search.py` | 15 passed | 19 passed |
| `v4_sql_snapshot.py check` (now honours the env flag) | not run | 83 identical, **26 shape drifts** |
| ladder-bridge use over TPC-DS (`s34_path_census.py`) | not measured | **0 of 327 requests** |

**These gates are not the whole picture and must not be read as one.** A full-repo sweep
surfaces failures none of them cover — `tests/engine/demo`,
`tests/modeling/tpc_ds_duckdb/test_non_benchmark_queries.py`, `tests/test_scoped_join.py`,
`tests/modeling/join_resolution` — and several are ORDER-DEPENDENT, passing in a small
selection and failing in the full file. The live list of network-caused failures lives in
`local_scripts/s34_handoff.md`; keep it there rather than here, and always confirm a fix
against the whole file.

The 26 golden drifts are the reviewable cutover diff step 4 asks for. They are shape
changes only — the same 109 queries execute to the reference rows (the battery is the row
gate, and it matches flag-OFF exactly). The bulk are the §0.1 twin-scan drops, already
confirmed row-preserving; `q97-one`/`-two`/`q97` are the superset-axis change below.
Goldens are NOT refreshed: they still pin the ladder, so `check` keeps reporting the
cutover delta until the ladder is deleted.

The 12 join_matrix/gcat failures were **pre-existing under the flag** — A/B'd by
neutralising `_blend_joins`, which reproduced all 12 exactly. They were never a
regression from the blend rule; they were the untested suites the s33 handoff predicted
would break, and every one of them was a §5 carry-over the search had not implemented.

### Terminal-set correctness, measured

`local_scripts/s34_terminal_audit.py` compares `build_source_network`'s terminals against
the address set `_resolve_bridge_graph` seeds its Steiner walk with, per ROOT request,
over the whole corpus: **360/362 identical**; both divergences are the intentional
derived-terminal drop (`is_returned` over a requested parent, q29/q84). The search is not
losing requirements — everything above is a *selection* defect, which is why every fix in
this section is a rule about which cover wins rather than about what gets searched for.

### RULED OUT — "unshared terminals" as a cost axis (s34, reverted)

`test_shared_attribute_bridge_not_pruned` is a shared-dimension diamond: `week_seq`
reaches `sales` through `sold_dates` and `inventory` through `inv_dates`, and a cover
taking only ONE date dimension joins the facts on `item_id` alone and fans out. The
diagnosis is right and worth keeping:

- Connected components of the FUNCTIONAL edges do not detect it, because
  `sales -> items <- inventory` joins both facts to the shared item dimension and pins
  neither to the other. **A functional edge is directional**: shared keys covering the
  TARGET's grain give one target row per origin row; the reverse is a fan-out. So
  reachability must be a directed walk (`_lookup_closure`), not a component.
- With that, the defect states cleanly: a source contributes a terminal but cannot reach
  another one without a row-multiplying hop, *where some candidate could supply it*. The
  "could supply it" clause is what keeps `BRIDGE_MODEL` legal — a measure the other fact
  simply does not have is the request, not a defect.

**The implementation is nonetheless a net loss and was reverted.** Measured over
`tests/modeling/tpc_ds_duckdb + tests/engine/demo + test_shared_dimension_bridge`:

| | failures |
|---|---|
| axis ON | **6** — `test_three`, `test_website_demo`, `test_constant_extra`, q29 ×2, `test_rowset_shape` |
| axis OFF | **4** — q29 ×2, `test_rowset_shape`, `test_shared_attribute_bridge_not_pruned` |

It fixes one test and breaks three, and costs ~2× TPC-DS generation time (52s → 98s even
after indexing `binders` and memoizing the closure) because the repair search runs per
cover. The repair is adding sources it should not; the "could supply it" clause is too
weak a filter on its own. **Do not re-land it without a rule that bounds WHICH side of a
blend is allowed to grow** — and re-measure that table, since three of those regressions
are order-dependent and do not reproduce in a small selection.

*(s37: re-landed WITH that bound as a requirement shape, not a cost — see §0.7. The
diagnosis above was the keeper; this table's criteria were re-measured and pass.)*

### Still open

- **q97-one / q97-two** shape drift is now the *superset axis* (`customer`/`item` scanned
  and outer-joined), not the fabricated `catalog_sales` bridge — `_pair_join_keys`
  materializes the declared superset side. §0.1 argues this is the correct reading;
  neither query is in the battery, so it is unverified by rows.
- `_connect`'s bridge carve-out is untouched. It still fires, and §0.1's argument that a
  disconnected cover is a cross product (strictly safer than a population-changing
  bridge) still stands as the principled fix.
- Union family selection, enumeration branch-and-bound, nullability, and derived
  connectors: unchanged from §0.2.

Scope: replace the FULL → PARTIAL_UNSCOPED → PARTIAL_SCOPED attempt ladder in
v4 ROOT sourcing with ONE search over a labeled graph that returns a solution
(or a typed ambiguity). v4-only module; v3's `resolve_weak_components` path is
untouched.

## 0.4 The equivalence web reaches the search (s35)

The s34 removal left 26 new failures, all merge-or-derived-key shapes, with one
diagnosed cause: **the ladder's Steiner walk traversed the CONCEPT graph —
lineage and pseudonym edges included — and the network only looked at
datasource↔concept bindings.** s35 closes that gap by injecting the graph's own
equivalence information into the search, in four scoped pieces. The
`link_keys` idea (ruled out in s34: keys that connect but do not provide,
measured net-negative) was NOT revived; every piece below binds only what some
component can genuinely PRODUCE.

1. **`_equivalence_map` consumes `graph.pseudonyms`.** After `merge ka into
   kb`, both real addresses in `environment.concepts` carry the surviving
   side's lineage; each side's own variant exists only under its canonical
   (`_virt_*`) address — the demoted side's real lineage lives in
   `alias_origin_lookup`, and `generate_graph` materializes the equality as
   pseudonym edges between canonical NODES. Feeding those node pairs into the
   union-find is what lets two scans' derived-key bindings land in one class
   and become a join axis (the join_matrix derived cells, the demo merges).
   Env-level `concept.pseudonyms` alone can never see this: it is keyed by real
   addresses, and the canonical spellings are not in `environment.concepts`.

2. **Connector resolution reads the SOLUTION's bindings.** A connector class
   labeled by a canonical address resolves through
   `environment.canonical_concepts`, carrying — per chosen source — the concept
   behind the binding that source actually reads (its own side's variant), so
   each scan materializes its side and `renders_derived_key` attaches it.
   Never the whole class: gcat's `first_org` (a SECOND declared alias for
   `org.code`) showed that an unread member hands the join a column the
   authored FK already provides and rewires the join.

3. **`_pin_unoffered_probes`.** A presence probe the graph offers off NO
   candidate (mixed ROOT-member vs rowset-anchor relations; the spine-merge
   gcat shapes) is bound onto its carrier — the same datasource
   `gen_presence_probe_node` pins — provided the carrier binds the probe's
   argument in ANY spelling of its equivalence class. Marked `injected`,
   because `_direct_source`'s graph-scored select cannot see it: a single-scan
   solution leaning on an injected binding routes through the bridge emitter,
   whose `_datasource_renders_probe` gate is the probe's actual renderer.

4. **`connector~` candidates for non-BASIC merge origins.** A merge key with a
   RECURSIVE/AGGREGATE origin is emitted by no scan, so the sides it relates
   share no binding at all. Its subplan is real — `_derived_connector_nodes`
   materializes the origin carrying the key plus its own grain keys — so the
   search now sees exactly that contract as a datasource-less candidate the
   cover can select (`test_recursive_enrichment`, recursive bridge connector).
   This is the "derived connector as a CANDIDATE" idea from the s34 handoff,
   and unlike `link_keys` it survives measurement because the candidate
   PROVIDES its bindings (the emitter genuinely materializes them).

Two `plan_source`-level assemblies round it out:

- **`_cross_component_source`** — the `sum(samt) + sum(wamt)` scalar shape: two
  facts related only through a derived expression's lineage, no join key at
  all. Components are sourced separately and merged; the downstream aggregate
  machinery collapses each side before the single-row cross join, which is the
  ladder-identical plan. Gated on `_lineage_connected` over the reference
  graph — a request whose pieces NOTHING relates keeps failing loudly
  (q75/q64/q35 correct disconnects).
- **Single-scan bridge retry** — when the search says one scan but
  `_direct_source` cannot render it (the persist-refresh watermark shape, where
  a derived output needs the bridge's concept-node assembly), `plan_source`
  re-asks for the bridge rendering instead of failing. The ladder answered the
  same shape with a one-datasource bridge.

## 0.5 A concept's two spellings are one class (s36)

Gate 7 → 4 failed. Three fixes, each a one-cause diagnosis (details and the
diagnosis chains live in `local_scripts/s34_handoff.md`):

1. **q29 dangling feeder** — emitter-side, as predicted. The union-join
   assembly's `_inject_scoped_join_key_exposure` counted an EXISTENCE FEEDER
   grandparent as row availability and surfaced a coalescing member
   (`sr_data.cid`) onto an arm whose row parents cannot render it. Guarded by
   `_feeds_only_existence`, the same feeder test `_repoint_feeder_only_rows`
   uses.
2. **Mirrored-INNER duplicate alias** (unmasked by 1) — two independently-built
   merges over the same parents legitimately pick opposite bases, the
   same-name CTEs merge, and `unique_id` (orientation-sensitive) kept both
   joins. INNER `unique_id` is now orientation-normalized and
   `coalesce_duplicate_joins` merges mirrored INNER joins by partner set,
   intersecting per-pair modifiers (`=` beats `is not distinct from`: the
   two-join plan ANDs both conditions, so intersection is the faithful
   semantics). The trigger — order-dependent Nullable stamps on
   same-identifier QDS — is still open.
3. **q84 wrong rows** — the search's address space missed that a concept's
   `.address` and its own `canonical_address` are ONE concept. `ss.is_returned`
   (canonical `ss._virt_comp_*`, which the store_returns edge emits) was a
   terminal NOTHING bound; the search declined; `gen_root`'s fallback merged
   the condition parent on ticket keys alone, and a same-cdemo customer that
   fails the filters rode a qualifying ticket in. `_equivalence_map` now
   unions address↔canonical per concept — presence probes excluded, their
   `_virt_presence_*`/`_virt_func_*` split IS side identity. This also
   upgraded the derived-terminal guardrail: a BASIC over an unrequested parent
   is retained (not lost) AND satisfiable when a scan's graph edge emits it.

Two follow-ups landed in the same session:

4. **Construction-order nullability, the nondeterminism behind fix 2, fixed at
   the root.** `get_all_parent_nullable` seeded a node's nullability at
   CONSTRUCTION from the parent NODE's mutable attribute — which only carries
   join-analysis nullability after the parent's first `resolve()` (the
   resolve-time sync). Two copies of one logical node built before/after that
   resolve therefore resolved to different plans. Base `StrategyNode._resolve`
   now recomputes pass-through nullability from the freshly RESOLVED parent
   sources and re-applies the node's own `condition_proves_non_null`
   refinement; MergeNode/GroupNode already recomputed. Plans no longer depend
   on node construction order.
5. **`InlineDatasource` gate is demand-driven.** The required-inputs test
   counted source_map METADATA — inherited derived attachments the consumer
   never renders (`org.flag`/`vehicle.full_name`, hidden as unused at rule 18
   while inline runs at rule 3). When the plain subset test fails it now
   consults `render_cte_used_map`; only addresses the consumer actually
   renders from that parent can block. Fixes gcat
   `test_aggregate_optimization` solo and full-file, both flags.
6. **A filtered branch must not be null-extended** — the latent wrong-rows
   bug fix 4 unmasked (q30/q30-alt). Honest nullability turned the final
   assembly's join to the `state = 'GA'` branch LEFT (join typing preserves
   toward a nullable key with no null-safe partner), and the null-extension
   resurrected rows the request WHERE rejected; the prior INNER was
   construction-order luck. `_assemble_final_node` threads the contributing
   groups' applied condition atoms into the final merge's
   `preexisting_conditions`, and
   `MergeNode._tighten_joins_for_filtered_branches` refuses to null-extend a
   branch carrying a pre-applied request atom the merge does not re-render.
   Branch-local filters (rowset-internal WHEREs) are not request atoms, so
   their deliberate preservation stands — and the authored-coalescing
   registry veto applies on top (measured: without it the composite
   union-join rowset family dropped return-arm rows against a sales-side
   filter): a join whose keys touch `outer_relation_keys` /
   `coalescing_relation_members` is authored row intent and only the
   provably-row-identical narrowing pass may tighten it.

The `test_full_join_two_keys_single_join` residue is now fully traced (rows
pass; shape asserts fail): coalescing-group members are deliberately NOT
pseudonyms, so a mixed-side request ({l.m1, r.k1, r.k2}) has no connected
cover and falls back to a redundant second axis assembly, while the same-side
request reads one arm. The v3 target (per-arm scans + ONE assembly FULL JOIN)
suggests the native model: **a member binding is a PARTIAL binding of the
group class**, priced by the existing partial/completion axes. That is the
union-family-selection workstream — a design step, not a patch.

## 0.6 Union family selection (s37)

`test_full_join_two_keys_single_join` is CLOSED, and the s36 trace above was
wrong in one instructive way: the mixed-side request was never disconnected.
`_equivalence_map` (s36's address↔canonical union) already relates the members,
and both arms bind the class — the failing shape came from **grain-key
expansion**, and fixing that alone unmasked the real §0.3 defect. Two coupled
facts, each a lie the model told about a coalescing (`full`/`union` join) key
group:

1. **The axis canonical's declared grain is the surviving arm's row key** —
   `union join l.k1 = r.k1` leaves both concepts with grain `{r.rrow}`. The
   unified axis spans BOTH arms' domains, so that grain is not a requirement of
   the axis; expanding it (`_concepts_with_grain_keys`) dragged `r.rrow` into
   the LEFT arm's aggregate-parent request and forced `rightt` into its cover —
   the redundant second FULL-JOIN axis. The expansion now skips coalescing axis
   canonicals. With it gone, both aggregate parents read their own arm
   single-scan and the final assembly makes the ONE FULL JOIN: v4's SQL is
   byte-identical to flag-off.

2. **An arm's binding of the axis class is not FULL.** Removing the expansion
   made `where s_cust is null select c_cust` (join_matrix
   `test_union_where_side_is_null_bare_axis`) read the axis off the PROBE'S arm
   alone — empty result, the §0.3 "reading it from one arm silently drops the
   left-only rows" defect, previously masked because expansion happened to drag
   the surviving arm in. The native model from s36's handoff, implemented:

   - **`_axis_families`** — a second requirement shape, exactly as §0.3
     predicted. A requested axis class maps to per-MEMBER carrier candidates
     (`member_binding_datasources` per member, the same carriers
     `gen_presence_probe_node` pins). Recorded on the network, consumed
     everywhere fullness is judged.
   - **Arm bindings are downgraded to PARTIAL** (`_downgrade_axis_bindings`);
     full is a property of the COVER: `axis_complete` = every member has a
     carrier in it. `_binding_profile`, `partial_terminals`, and `completions`
     all consult it, which makes `_reduce` refuse to drop an arm (profile 2→1)
     with no new rule.
   - **`_complete_axis_families`** — the reachability repair, same argument as
     `_pair_join_keys`: enumeration stops branching the moment ONE arm binds
     the class, so the family cover is never generated on its own.
   - **The arm-pinned exemption.** An arm-scoped request — some OUTPUT terminal
     lives at an arm carrier's row grain, grain EQUALITY not subset — reads the
     axis at that arm BY DESIGN; a downstream assembly coalesces the arms (the
     aggregate-parent shape; also v3's read). Such requests get no family entry
     and behave exactly as before. Condition columns and their grain keys never
     pin: a filter restricts the axis population, it does not redefine the rows
     as one arm's — the side-pinned presence probe's own row key must not turn
     an axis anti-join into a single-arm read (that was the first
     implementation's bug, caught by the same join_matrix cell).
   - Groups with a member no candidate carries (rowset members) get no entry:
     the search cannot complete them, and the rowset machinery that owns those
     members assembles the axis downstream (q35/q59 unchanged).

Measured: full flag-ON gate **2 failed / 1626 passed** (was 3/1625 — the two
left are the shared-dimension diamond and the parked ambiguity test; a
`test_ninety_six` PermissionError in the run was a zquery-log collision with a
concurrently running sweep, passes solo); generation sweep 109/109; snapshot
31 drifts — the SAME 31 as s36, zero new TPC-DS shape change;
`test_scoped_join.py` fully green flag-ON. Unit guardrails:
`TestCoalescingAxisFamilies` captures the network from a real query build
(axis families are query-scoped declarations) and pins both the family
assembly and the arm-pinned exemption.

## 0.7 The diamond as a requirement, not a cost (s37)

`test_shared_attribute_bridge_not_pruned` is CLOSED — with the fan-out row
gone and correct rows — by re-expressing §0.3's reverted "unshared terminals"
idea through the §0.6 pattern: **requirement shape + unconditional repair +
reduce-protection via the structure tuple**, instead of a cost axis. The
directional-reachability diagnosis §0.3 said to keep is the core; everything
that killed the axis is gone:

- **`_functional_into(origin, target)`** — the DIRECTIONAL lookup test (shared
  keys cover the TARGET's grain: one target row per origin row). The
  undirected `joins_functionally` cannot see the diamond because
  `sales -> items <- inventory` puts both facts in one functional component
  while pinning neither to the other.
- **`_lookup_carriers(source, terminal)`** — candidates ONE functional hop
  from the source that bind the terminal fully, memoized at the CANDIDATE
  level. (The s34 axis re-searched per cover — half its 2× generation cost.)
- **`_broken_diamonds`** — count of (source, terminal) pairs where the
  source's own rows cannot be labeled with a requested terminal through any
  in-cover lookup while some candidate could supply one. This is the defect
  stated exactly: `week_seq` reaches sales through `sold_dates` and inventory
  through `inv_dates`; a cover keeping one date dimension leaves the other
  fact inheriting the week through the item-only meet — the fan-out.
- **`_repair_diamonds`** — adds the best carrier per broken pair, in the
  repair chain with the others (enumeration stops branching on a terminal one
  source already binds, so the two-dimension cover is never generated on its
  own). `_join_structure` gains `_broken_diamonds` as its third component,
  which is what stops `_reduce` calling the second date dimension redundant —
  its VALUE profile is genuinely redundant (`week_seq` is already bound), its
  structure is not. No new `SolutionCost` axis: repair is unconditional, so
  no unrepaired competitor survives to need dominating.

**The bound §0.3 demanded** ("which side of a blend is allowed to grow"): only
a SINGLE functional hop off the source's own keys qualifies — a pure lookup
that can label or restrict the source's rows but never multiply them — and the
terminal must be bound in the source's OWN key-class terms. That second clause
is what spares the q29 family: a different import alias of the same physical
date dimension is a different key class, so a returns fact is never forced to
materialize the SALES date role. A terminal NO candidate can supply is the
request itself, so `BRIDGE_MODEL`'s fact-to-fact blend still plans untouched
(pinned by `test_unsuppliable_terminal_stays_a_legal_blend`).

Measured against the §0.3 revert table's own criteria: the three tests the
axis broke (`test_three`, `test_website_demo`, `test_constant_extra`) pass in
the FULL engine suite (642/0); TPC-DS battery 214/0; sweep 109/109 at ~45s
(no 2× — the axis's other kill criterion); snapshot the SAME 31 drifts, zero
TPC-DS shape change. Unit guardrails: `TestDiamondLookups` (the diamond needs
both date dimensions; the unsuppliable blend stays legal).

## 0.8 Obligations: one pass, no repairs (s38)

The four repairs (`_pair_join_keys`, `_colocate_blended_grains`,
`_complete_axis_families`, `_repair_diamonds`) and the `_join_structure`
reduce-guard are GONE, consolidated into one primitive. They were four
independently-discovered instances of a single missing capability, each landed
as its own (requirement shape → greedy repair → reduce guard) triple when a
test exposed it — the whack-a-mole pattern made structural.

**The diagnosis.** Cover enumeration branched on COVERAGE: ∃ some binder per
address, stop the moment an address is bound. But every correctness invariant
quantifies ∀ per source / per relation / per arm. A cover that binds an
address in a SECOND place for structural reasons is therefore never generated,
so each invariant needed a post-hoc repair — and the repairs ran once, in a
fixed order, greedily (`options[0]` under three different sort keys), with no
fixpoint, invisible to dominance, and each needing its own hand-maintained
guard in `_reduce` or the next reduction would undo it.

**The fix.** The enumeration state generalizes from "remaining addresses" to
"pending obligations" (`Obligation`, `_pending_obligations`). One vocabulary:

- `cover(t)` — some source binds terminal t (partial suffices; upgrading to a
  full binder is a soft branch, priced by the partiality/completion axes).
- `axis(class, member)` — a requested coalescing axis needs a carrier per
  member arm (§0.6's family, now discharged by the same machinery).
- `paired(relation, side)` — a declared-relation side the cover carries must
  materialize the merged key against its own keys (§0.3 rule 2).
- `labelable(source, terminal)` — a source contributing terminals must be able
  to label its own rows with each requested terminal through one in-cover
  functional hop, whenever some candidate could supply the lookup (§0.7's
  diamond, stated generally).
- `colocated(source)` — a source none of whose in-cover joins covers its grain
  gets its grain key co-located when some candidate can (§0.3's blend repair).

The DFS pops a state, discharges the scarcest pending obligation by branching
on ALL its satisfiers, and emits a cover only when nothing is pending.
Properties that make this sound and total: obligations are **monotone**
(adding a source never re-opens one; it can only spawn new ones, and the
space is finite), every branch adds a source, and an obligation is **minted
only when a satisfier exists** — a requirement nothing could satisfy is the
request's own shape, which is what keeps the fact-to-fact `BRIDGE_MODEL`
blend legal with no special case. Dedup is on the chosen frozenset alone
(stronger than the old `(index, chosen)` key). `STATE_LIMIT` bounds visited
states; truncation is reported, never silent.

What this bought, beyond deletion (~150 lines and five functions):

- **Satisfier alternatives enter dominance.** The old repairs picked one
  carrier greedily; every way of discharging a requirement is now a distinct
  emitted cover judged on the cost axes. Ambiguity among carriers is visible.
- **Fixpoint for free.** A source added to discharge one obligation is itself
  re-checked (its own labelable/colocated obligations) on the next state —
  the second-order cases the repair chain silently ignored.
- **`_reduce` needs no per-invariant knowledge.** Its guard is now "the drop
  re-opens no obligation the cover had discharged" (plus profile,
  connectivity, and blend-count non-worsening — blends are the one pure-cost
  invariant with no obligation form). A future invariant added as an
  obligation is automatically protected under reduction.
- **Faster.** Sweep 109/109 at ~37s vs ~45s: discharge runs once per unique
  state instead of four repairs per enumerated cover.

`_connect` is deliberately NOT an obligation: it remains the last-resort
bridge fabricator (§0.1's open defect) and now the ONLY place a source can
enter a cover with no invariant justifying it. Its principled replacement
(disconnected cover = cross product, preferring functional paths) is the next
structural item and should be priced through the same `labels`/functional
machinery. `_reduce` computes its obligation baseline from the post-connect
cover for exactly this reason: what connect leaves open cannot be held
against a drop.

Measured at the cutover: unit guardrails 25/25 unchanged; join_matrix +
scoped_join + shared_dimension_bridge + join_resolution + core/processing
793 passed / 1 failed (the parked ambiguity test, identical to baseline);
sweep 109/109. Full battery/engine/gcat and snapshot results recorded in
`local_scripts/s34_handoff.md`.

## 0.9 The ladder purge: policy collapse + v4 by default (s38, same session)

What "the ladder" had already become by s38: the Steiner attempt loop in
`plan_source` was dismantled incrementally across s35–s37 (`_bridge_plan` /
`_resolve_bridge_graph` no longer existed; `_prune_redundant_partial_connectors`
already deleted; `determine_induced_minimal_nodes` + `penalize_partial` already
v3-only). The survivors were pure vestige:

- **`SourcePolicy`/`SourceAttempt`, deleted — and the partiality MODE with
  them.** v4 consumed exactly ONE bit of the policy (`.accepts_partial`); the
  UNSCOPED vs SCOPED `SearchCriteria` distinction was dead code, and the
  FALLBACK policy's third attempt was a literal duplicate call of the second.
  A first collapse to a threaded `accept_partial: bool` then revealed (user
  push) that the threaded value was CONSTANT on every path — a parameter that
  never varies is not a parameter. Final state: `search_concepts`,
  `strategy_builder`, and all five v4 generators carry no partiality argument
  at all (the `V4History` cache key lost it too); `SourceRequest` keeps one
  inverted, honestly-named constraint, `require_full: bool = False`, set by
  exactly one caller — `_complete_partial_requested`'s sub-request, where
  "find a COMPLETE source for this output" is the request itself. Inside the
  search partiality was already a per-binding label priced by the cost axes;
  the emitter boundary (`gen_select_node` / `create_select_node_candidate`,
  v3-shared machinery, untouched) now derives its flag from `require_full`
  instead of a threaded mode. Behavior edge accepted and gate-verified:
  connector sub-searches under a completion subtree are now permissive
  (strictness binds the requested output, not join infrastructure).
- **`use_v4_discovery` defaults TRUE.** v4 is the planner; the env override
  inverts (`TRILOGY_V4_DISCOVERY=0` forces v3 for comparison sweeps) and the
  known-failing registry xfails apply whenever v4 runs. v3 remains in-tree
  behind the flag until its own removal decision. *(Superseded: the flag, the
  env override, the registry and the legacy planner have all since been
  removed.)*
- **Registry burndown, measured not assumed: 43 → 31 keys.** All 43 entries
  re-verified in ISOLATION (the registry's own promotion standard): 15
  xpassed — quietly fixed by the s34–s38 work — and were pruned; 3 were stale
  nodeids for deleted tests. The FIRST full-suite v4 sweep outside the gate
  dirs (never previously run) surfaced 5 new pre-existing gaps, each A/B'd
  against the pre-refactor engine via an old-engine pytest plugin to prove
  they are not obligation-engine regressions. The parked ambiguity test is
  now a tracked registry entry (PARKED reason: model-level ambiguity may move
  to parse time — a user decision, not a planner fix).
- One flag-independent stale test fixed: `test_join_unique_id_*` asserted the
  pre-s36 `unique_id` format that the mirrored-INNER orientation
  normalization deliberately changed.

**Ordering determinism (same session, user report: CTE order flip-flops).**
Measured by a hash-seed A/B harness (render the 109-query corpus under
`PYTHONHASHSEED=1` vs `=2`, diff): exactly one query (q35) flipped — two
sibling existence CTEs swapped WITH-clause emission order, names stable,
plans identical. Log-bisection localized it to `optimize_ctes`: the CTE list
entering the optimizer was seed-identical; the list leaving it was not.
`reorder_ctes` delegated to the graph core's plain topological sort, whose
tie-breaks inherit edge-insertion order — which inherits set iteration
somewhere upstream. Fixed at the CHOKE POINT rather than chasing per-site
set iterations: `reorder_ctes` is now a stable Kahn's sort (position-keyed
heap) whose output is a pure function of the input list order and the
dependency EDGE SET, independent of hash seed. Post-fix: all 109 corpus
renders byte-identical across seeds. The seed A/B harness pattern
(`scratchpad/seed_render.py`) is the regression check for any future
flip-flop report — diff renders across two `PYTHONHASHSEED` values before
hunting code.

**`_connect` removal ATTEMPTED and REVERTED (same session) — the measurement
is the keeper.** Corpus census: 4,786 calls — 4,782 pass-throughs, 0 give-ups,
4 widenings, all q97-one/-two, all producing DOMINATED covers (the winning
q97 cover is a natural six-source two-fact solution; a row oracle proved
reference ≡ v3 ≡ v4 = (540709, 286686, 171) at sf=1). A check-only `_connect`
left the corpus byte-identical (snapshot 109/0) and fast gates green — but
broke gcat `test_array_agg` and `test_multi_join_assignments::test_select` in
the full halves: **fabrication is corpus-dead but suite-live.** Reverted; the
census, the q97 oracle, and the two failing shapes are the design bed for the
principled replacement (disconnected = cross product preferring functional
paths). Do NOT re-attempt as a blind delete; start from those two tests, and
census over the FULL suite, not the corpus (the corpus-only census was
exactly the one-passing-area trap).

Still open after the purge: the 30-entry registry (the distance to deleting
the flag and deciding v3's fate), nullability, enumeration branch-and-bound,
and the parked ambiguity decision (parse-time move).

## 1. What existed before the refactor (the ladder — DELETED s38)

`plan_source` (`v4_helper/source_planning.py:1421`) is the v4 entry point for
every ROOT group. Its structure:

```
plan_source(request)
├── _plan_coalescing_axis / _plan_complete_where_source / _plan_finer_filter_rollup   (pinned special cases)
└── for attempt in source_policy.attempts:            # FULL, PARTIAL_UNSCOPED, PARTIAL_SCOPED
    ├── _bridge_plan(request, attempt)
    │   ├── _single_source_covers_completely  → bail to _direct_source
    │   └── for filter_downstream in (True, False):
    │       └── for (search_conditions, allow_intersection) in condition_options:   # full / covered / none
    │           └── _resolve_bridge_graph(...)
    │               ├── copy graph, _inject_union_datasources, prune_sources_for_conditions(attempt.criteria)
    │               └── for _ in range(AMBIGUITY_CHECK_LIMIT=20):
    │                   ├── determine_induced_minimal_nodes(...)   # Steiner tree
    │                   ├── _prune_redundant_partial_connectors    # s31
    │                   └── knock out the connectors it found, re-run
    │               └── detect_ambiguity_and_raise(...) → found[0]
    ├── _datasource_nodes_for_bridge → _merge_component_sources → _complete_partial_requested
    └── _direct_source(request, attempt)
└── unconditioned retry: plan_source(conditions=None) wrapped in a filtering SelectNode
└── dead-last: the `full_cover_fallback` bridge
```

Worst case per ROOT group: 3 attempts × 2 `filter_downstream` × 3 condition
options × 20 knockout rounds = **360 Steiner searches**, plus recursive
`plan_source` calls from `_complete_partial_requested` and the unconditioned
retry. In practice most requests exit at the first attempt, but the ceiling is
real and it is paid on the hard queries.

### Why the ladder exists

`accept_partial` in v3 was **persistent escalation state** threaded through
recursive discovery: v3 descends concept-by-concept, and when a subtree fails it
retries the whole subtree in a more permissive mode. v4 assembles the entire
concept graph up front — so "this query needs a partial read" is a property of
the SOLUTION, not a mode you have to pick before searching. The ladder is a v3
artifact that v4 inherited wholesale.

## 2. The bug class it creates

**A phase commits to a source through an edge a later phase discards.** Three
recorded instances, two of them fixed tactically:

1. **s10 / enum `partial_key_union`.** The Steiner search picked a UNION
   datasource via a *partial* `order_id` edge; the final edge re-add
   (`determine_induced_minimal_nodes`, the `if not accept_partial: continue`
   at node_merge_node.py:303-306) DROPS partial edges — so the union ended up
   connected only through `chan`, and `order_id` got completed separately by
   `_complete_partial_requested`, nulling the co-resident `chan`. Fix: weight
   100 on union-partial edges (`penalize_partial`), gated to unions only
   because the same penalty on individual `~` bindings broke gcat's
   load-bearing launch-fact bridge.
2. **s31 / q96 + q88.** The Steiner search weights a partial `~` binding edge
   the same as a full one, so a partial-side fact table won a tie as a
   connector between two concepts the anchor already binds — an extra
   fact-table LEFT JOIN with coalesced count keys. Fix:
   `_prune_redundant_partial_connectors` runs *after* the search, undoing a
   choice the search should not have made (with a `non_partial_for` exemption,
   because a `complete where` partial is conditionally FULL).
3. **The search cannot see its own downstream cost.** Whether a chosen edge
   forces a completion join (`_complete_partial_requested`), a derived
   connector subplan (`_derived_connector_nodes`), or a rejection
   (`_bridge_parents_cover`) is discovered strictly after the search returns —
   and the response is to fall through to the next attempt, re-running the
   whole search under different global settings rather than picking the next
   candidate.

Both fixes are *post-hoc corrections of a search that optimized the wrong
objective*. The refactor's thesis: make partiality an edge label the single
search reasons about, and the class disappears structurally.

Secondary costs: ambiguity is detected by a **knockout-rerun loop** (remove the
connectors you just found, search again, compare the reduced sets) rather than
by enumerating non-dominated alternatives; and `detect_ambiguity_and_raise`
therefore reports "different paths" that are an artifact of knockout order.

## 3. Target design

One module, `v4_helper/network_search.py` (v4-owned; no shared-helper conflict
with v3). Four stages, each independently testable.

### Stage A — build the candidate network once

Inputs: the request's terminals (`_search_concepts_for_bridge` output, i.e.
requested + condition row args + condition lineage roots + authored-join
terminals + grain keys), the environment graph, and the conditions.

Output: an undirected multigraph over `c~` (concept) and `ds~` (datasource)
nodes where **every datasource edge carries labels instead of being pre-pruned**:

| label | meaning | today |
|---|---|---|
| `binding=FULL` | datasource binds the concept as a non-partial output | edge survives every attempt |
| `binding=PARTIAL` | a `~` binding | dropped from the final re-add when `not accept_partial` |
| `binding=PARTIAL_UNION` | union inheriting arm partiality | weight 100 (s10) |
| `condition=IMPLIED_EXACT` | a `complete where` partial the query's WHERE implies | survives pruning; gets the WHERE pushed onto its scan |
| `condition=APPLICABLE` | the ds binds every condition column non-partially | survives `prune_sources_for_conditions` |
| `condition=PROPERTY_REACHABLE` | condition column reachable via the ds's grain key | the aggregate carve-out in `_datasource_is_exact_match` |
| `condition=SENSITIVE` | ds holds an aggregate the WHERE would invalidate | pruned today |
| `derivation=CONNECTOR` | reachable only through a non-BASIC merge origin | `_derived_connector_nodes` supplies it |

The three `condition_options` retries (full / covered / none) and the two
`filter_downstream` passes collapse into **node and edge attributes**:
`filter_downstream` becomes a per-node "this derived concept is decomposable"
flag rather than a global purge, and the condition retries become a per-edge
`condition` label the dominance rules read.

`prune_sources_for_conditions` is NOT called on the search graph. Its verdicts
become labels. (It stays as-is for v3 and for the pinned special-case planners.)

### Stage B — enumerate candidate solutions

A candidate is a connected subgraph spanning all terminals. Enumerate the
non-dominated ones, not just the single cheapest tree:

- Seed with the weighted Steiner tree (the existing solver — this is not a
  rewrite of the graph algorithm).
- Generate alternatives by *edge-class relaxation* rather than node knockout:
  for each terminal with more than one binding datasource, the alternatives are
  its distinct binders. This is the honest source of ambiguity ("two datasources
  can serve this concept and neither dominates"), and it is directly enumerable
  instead of being inferred from repeated searches.
- Cap enumeration (keep `AMBIGUITY_CHECK_LIMIT`'s spirit) and `log()` when the
  cap truncates — a silent cap reads as "we considered everything".

### Stage C — dominance prune

Candidate A dominates candidate B when A is no worse on every axis and better on
at least one. Axes, in the order they matter:

1. **Coverage**: every terminal bound (a candidate that routes a terminal
   through derived lineage without a datasource that emits it is *invalid*, not
   merely worse — today's `_bridge_parents_cover` rejection).
2. **Partiality**: fewer terminals bound only partially. A partial binding that
   another candidate binds fully is dominated evidence — this is the s31 gate
   generalized from "prune a redundant connector node" to "prefer the candidate
   without it". `non_partial_for` datasources whose predicate the query implies
   count as FULL for this axis (the s31 exemption, expressed once).
3. **Completion joins implied**: a surviving partial binding on a *requested*
   output means `_complete_partial_requested` will add a join — so it is a
   real cost the search now prices, instead of a surprise afterwards.
4. **Datasource count**, then **connector concept count**, then a stable tie
   break on sorted datasource names (determinism — the s29 discriminator
   lesson).

If exactly one candidate survives → that is the solution. If several survive and
they differ in which concepts they introduce → raise
`AmbiguousRelationshipResolutionException` with the surviving alternatives
(strictly better than today's knockout-order-dependent message). If several
survive but agree on the concept set → any of them; take the tie-break order.

### Stage D — emit a solution, not a graph

```python
@dataclass(frozen=True)
class SourceSolution:
    datasource_assignments: tuple[DatasourceAssignment, ...]  # ds -> concepts it emits
    connector_concepts: tuple[BuildConcept, ...]              # what the search added
    partial_addresses: frozenset[str]                         # survives to the built node's partial_concepts
    completions: tuple[CompletionJoin, ...]                   # explicit, priced in stage C
    condition_placements: tuple[ConditionPlacement, ...]      # per-ds pushed WHERE vs post-merge
```

`_datasource_nodes_for_bridge` becomes a straight consumer of this: no
gap-fill re-pointing (assignments are explicit), no post-hoc condition
inference. `_complete_partial_requested` stops being a discovery step and
becomes a *renderer* of `solution.completions`.

## 4. What this subsumes

| today | after |
|---|---|
| `SourceAttempt` / `SourcePolicy` ladder in `plan_source` | one search; policy reduces to "may this request accept a partial answer at all" |
| `_bridge_plan`'s `filter_downstream` × `condition_options` retries | node/edge labels in stage A |
| `_resolve_bridge_graph`'s knockout loop + `detect_ambiguity_and_raise` | stage B enumeration + stage C dominance |
| `penalize_partial` weighting (s10) | partiality axis in stage C |
| `_prune_redundant_partial_connectors` (s31) | dominance rule 2 — the prune's gates become its test cases |
| `_complete_partial_requested` triggering | `solution.completions`, priced during the search |
| `_bridge_parents_cover` rejection | coverage validity in stage C |
| bridge-vs-`_direct_source` preference heuristics (added-connector / union / condition-matched-partial / `full_cover_fallback`) | a single-datasource solution IS a candidate; `_direct_source` becomes the renderer for a one-assignment solution |

## 5. What MUST carry over (each becomes an explicit rule + test)

These are accreted bug fixes. Every one of them is load-bearing; losing any is a
silent wrong-rows regression, not a build error.

- **Single-row / abstract-grain concepts never drive connectivity** (they join
  by cross product; driving the search with them invents a spurious join key
  and raises false ambiguity). Today: the `Granularity.SINGLE_ROW` filter in
  `_resolve_bridge_graph`.
- **`__preql_internal` addresses are not terminals.**
- **Derivation purge**: CONSTANT / AGGREGATE / FILTER nodes are not path
  material — EXCEPT a mandatory concept whose canonical is
  datasource-materialized (a summary table binding `count(x) by k` makes that
  aggregate directly selectable; without the exemption the dijkstra seed
  references a deleted node → `NodeNotFound`).
- **`filter_downstream`**: a derived concept whose parents are all being
  searched for is decomposable — but BASIC concepts directly bound to a
  datasource column, and ROWSET outputs, are exempt (a rowset is one opaque
  unit and anchors a join like ROOT).
- **BASIC non-`ATTR_ACCESS` edges are expensive** (weight 50 today): routing a
  join through a computed expression is worse than through a stored column.
- **Non-BASIC merge origins are supplied by `_derived_connector_nodes`**, never
  by a raw scan, and the datasource gap-fill must stand down for them (a
  re-pointed datasource lets the bridge scan the merged key directly and
  strands the connector). A BASIC merge origin computes inline and is fine.
- **The connector's own mandatory set carries uncovered bridge concepts whose
  grain components are a subset of the origin's grain** (s15: `orders.amt`
  riding the window CTE, otherwise INVALID_REFERENCE at render).
- **Union injection** (`_inject_union_datasources`) and union exact-match
  semantics: a child whose partition the conditions fully satisfy beats its
  union.
- **Partial markings survive onto built nodes** — they drive the partial→FULL
  join contract downstream. The solution must carry them explicitly.
- **`complete where` partials are conditionally full**: under a query implying
  `non_partial_for`, the partial is the PREFERRED (pre-filtered, smaller)
  source and its WHERE is pushed onto its scan so `partial_is_full` clears the
  flag. Its partiality is never dominance evidence.
- **A Steiner solution can traverse a node minted in another build scope** (a
  rowset body's key under different scoped joins); it proves connectivity but
  cannot be planned here — resolve what this scope knows and drop the rest
  (`_concepts_in_graph`).
- **`reinject_common_join_keys_v2` / synonym handling**: pseudonym mates must be
  re-added or a side never materializes its own member and the equality drops
  out of the merge join (q05 fan-out).
- **Determinism**: no `hash()`-derived ordering anywhere; sort by address /
  datasource name; stable tie-breaks (the s29 discriminator lesson).

## 6. Staging (COMPLETE — all five steps landed s32–s38)

Each step is independently landable and independently verifiable. Kept as the
record of how the cutover was gated; the gates themselves are the reusable
part.

1. **Instrument first.** Log, per `plan_source` call: which attempt won, how
   many Steiner searches ran, whether `_complete_partial_requested` fired,
   whether `_prune_redundant_partial_connectors` fired. Run over the 109-query
   TPC-DS corpus + join_matrix + gcat. This is the *evidence base* for the new
   dominance rules — the s-series lesson that a "dead" path is validated by
   instrumentation, not by reading code.
2. **Stage A alone**: build the labeled graph and assert (in a test harness,
   not in the plan path) that the labels reproduce today's
   `prune_sources_for_conditions` verdicts for every corpus query. No behavior
   change.
3. **Stage B+C behind a config flag**, shadow-mode: run both searches, compare
   the chosen datasource set, log divergences. Do NOT switch the plan over.
   Divergences are either a missing carry-over rule (§5) or a genuine win —
   classify every one before flipping the flag.
4. **Flip the flag** with the golden-SQL harness as the gate
   (`local_scripts/v4_sql_snapshot.py check`), plus TPC-DS battery, join_matrix,
   gcat + enum_unions, and a full v4 sweep. Every golden drift must be
   classified as a win before goldens are refreshed.
5. **Delete the ladder** and the two tactical patches (`penalize_partial`,
   `_prune_redundant_partial_connectors`) only after the flag has been default-on
   through a full sweep. Their tests stay and now cover the dominance rules.

Do NOT relocate `_prune_redundant_partial_connectors` into the shared helper as
separate work — it dissolves into step 5.

## 7. Risks

- **The carry-over list in §5 is the whole risk.** Each item is a fixed bug with
  a test; a rule that silently fails to port shows up as wrong rows, not an
  error. Step 3's shadow mode exists specifically to catch this before any
  behavior changes.
- **Ambiguity surface changes.** Today's message is knockout-order dependent;
  the new one enumerates surviving alternatives. Expect existing ambiguity
  tests to need updated (better) messages — that is a real diff to review, not
  noise to suppress.
- **Enumeration cost.** Stage B must be bounded and must log truncation. The
  win is that the *typical* case runs one search where today's worst case runs
  hundreds; the risk is a pathological schema where alternatives explode.
- **v3 must stay untouched.** `determine_induced_minimal_nodes`,
  `prune_sources_for_conditions`, and `resolve_weak_components` are shared with
  v3's recursive discovery. The new module may CALL the Steiner solver but must
  not change its signature or semantics; any shared change forces a v3 sweep.

## 8. Non-goals

- Rewriting the Steiner solver or the graph representation.
- Moving stage-1's `resolve_alternatives` scan-cost model (shape audit item 6 —
  related, but a separate change; this refactor does not depend on it).
- Changing how built nodes render, join, or mark partials downstream.
