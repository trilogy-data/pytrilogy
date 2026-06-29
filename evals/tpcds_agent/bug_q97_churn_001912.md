# q97 token-sink (run 20260629-001912, 709k tokens, PASSED)

> **ROOT-CAUSE UPDATE 2026-06-28 (claude).** Bisected past the handoff's "cross-model /
> composite" framing — neither is the trigger. The real trigger is the **presence-flag shape**:
> the FULL-join keys are referenced only inside CASE/`is null` flags, not projected. Reproduces
> on a SINGLE shared base too (`scratchpad/cmp2.py presence-only`).
>
> **Definitive cause:** a scoped FULL join treats its two key concepts as ONE *merged logical
> key*. Projecting BOTH sides — `select a_set.cust_id as ac, b_set.cust_id as bc` — renders
> **both** as the identical `coalesce("a"."cust_id","b"."cust_id")` (`scratchpad/cmp3.py`). So
> `a.k is null AND b.k is not null` is degenerate (the two are always equal), the per-side
> presence flags can never fire, and discovery — seeing the two keys as interchangeable via the
> FULL cross-pseudonym (`build.py:2484-2489`) — prunes one source entirely. The prune is upstream
> in discovery/source-selection, NOT in `join_resolution.py` (by then the source is already gone).
>
> **Verdict (reviewed w/ user 2026-06-28): NOT a framework codegen bug — working as designed.**
> A scoped `full join a = b` is an *identity assertion* (it declares a.k and b.k are the same
> concept), so per-side `is null` flags are tautologically empty — exactly as the merged-key model
> implies. The agent transliterated the reference **SQL**'s FULL OUTER JOIN, where `ssci.cust` and
> `csci.cust` stay distinct NULL-preserving columns; that idiom simply does not translate to
> trilogy's `=`-as-identity model. The framework's own canonical `query97.preql` confirms this — it
> avoids the join entirely and uses `import all_sales` + `max(case when channel='STORE' …) as
> store_present` / `catalog_present` grouped by the (customer,item) pair. The residual "prunes a
> source / returns 2 not 3" is a degenerate-query artifact, not worth a risky semantic change.
>
> **Fix = GUIDANCE, not codegen** (user is updating constants): agent-info should (1) teach the
> cross-model only-in-A / only-in-B / both idiom via `all_sales` + presence flags (or the agent's
> working `concat`-key + `count_distinct(k ? k in/not in other)` fallback), and (2) note that scoped
> `=` joins assert identity, so per-side `is null` after a `full join` is always false. No xfail /
> codegen test was kept — the prior "full FULL-OUTER-JOIN fix" framing was based on this report's
> mis-classification.


## TL;DR
q97 = count (customer,item) combos that are store-only / catalog-only / both, in
a 2000 window, comparing the two channels' distinct-pair sets. The agent reached
the textbook-correct shape on its FIRST real attempt — two distinct-pair rowsets
`full join`ed on the composite key, presence via `is null` (identical to the
canonical TPC-DS `query97.sql`: two CTEs `FULL OUTER JOIN ... ON cust=cust AND
item=item`). Trilogy ran it and returned **`0, 0, 27449`** — silently wrong
(correct: `540709, 286686, 171`). No error. The agent spent ~25 messages
(13→51) debugging the impossible numbers before abandoning joins entirely for a
`concat` composite-key + `in`/`not in` membership rewrite, which worked.

**Primary obstacle = a real codegen/resolution BUG (silent wrong result):** a
`full join` between two ROWSETS collapses the two join-key columns into one
identity and prunes one rowset source, emitting NO full join. This is the
substitution that's correct for INNER but invalid for FULL. The framework's own
canonical `query97.preql` sidesteps it (uses `all_sales` + presence flags, no
join), which is itself a tell that the rowset-FULL-join path is known-bad.

## Repro 1 — composite-key rowset FULL JOIN (the agent's msg-21 attempt)
File: `results/20260629-001912/workspace/repro_fulljoin.preql`
```
import raw.store_sales as ss;
import raw.catalog_sales as cs;
with store_set as where ss.date.year = 2000 and ss.customer.id is not null
  select ss.customer.id as cust_id, ss.item.id as item_id;
with catalog_set as where cs.sold_date.year = 2000 and cs.bill_customer.id is not null
  select cs.bill_customer.id as cust_id, cs.item.id as item_id;
select
  sum(case when store_set.cust_id is not null and catalog_set.cust_id is null then 1 else 0 end) as store_only,
  sum(case when store_set.cust_id is null and catalog_set.cust_id is not null then 1 else 0 end) as catalog_only,
  sum(case when store_set.cust_id is not null and catalog_set.cust_id is not null then 1 else 0 end) as both
full join store_set.cust_id = catalog_set.cust_id and store_set.item_id = catalog_set.item_id
limit 100;
```
Executed (workspace duckdb): `0, 0, 27449`  — WRONG (correct `540709,286686,171`).

Generated SQL (via `Executor.generate_sql`): only ONE CTE (`wakeful` =
catalog_set); store_set is gone; every `store_set.cust_id` AND
`catalog_set.cust_id` renders as the SAME column `wakeful.catalog_set_cust_id`,
so `store_only`/`catalog_only` are `X is not null and X is null` ≡ always-false →
0, and there is NO `FULL OUTER JOIN`.

## Repro 2 — minimal: single-key rowset FULL JOIN proves the key-collapse
File: `results/.../workspace/repro_singlekey.preql`
```
select store_set.cust_id as sc, catalog_set.cust_id as cc
full join store_set.cust_id = catalog_set.cust_id limit 10;
```
Generated SQL projects:
```
"cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "sc",
"cs_catalog_sales"."CS_BILL_CUSTOMER_SK" as "cc",   -- both sides = SAME column
```
store_set pruned, no FULL OUTER JOIN. Matches the agent's observed output where
`sc` always equalled `cc` (`[81418,81418],[90620,90620],…`) and both columns had
0 nulls — i.e. the "full join" behaved as an inner join on a single shared column.

## Classification
- (a) **Real framework correctness bug — SILENT wrong result, the worst class.**
  A scoped `full join` between two rowsets does not produce a FULL OUTER JOIN;
  the two distinct rowset output keys are substituted onto one canonical column
  and one source is dropped. Presence-flag/`is null` logic — the only reason to
  use a full join — is destroyed.
- (b) **Guidance defect compounding it.** agent-info actively steers here:
  `set-intersect-difference` shows presence flags only WITHIN ONE model; for two
  models the `scoped-join` example (block 2, "multi-key blend… JOIN ON THE FULL
  GRAIN", and the `full` semantics blurb) is the documented cross-model set-
  comparison path — exactly what the agent built, and exactly what is broken. No
  example shows the cross-model store-only/catalog-only/both shape that the
  canonical `query97.preql` actually uses (`all_sales` import + per-pair
  `max(case when channel=…)` flags). The agent never tried `all_sales`.

## Root cause / pointers (do NOT fix)
- Site: `trilogy/core/models/build.py` ~L2300–2410 — scoped-join key
  substitution. `scoped_full_join_keys` (L2330) and the rowset-FULL handling
  (`scoped_rowset_outer_sources`/`_targets` L2370–2379) are meant to keep both
  joined subgraphs sourceable and coalesce at the merge node, but for a
  rowset⇄rowset FULL the two output keys collapse to one canonical and one
  source is pruned (no coalesce, no full join emitted). The L2303 "KNOWN
  INCOMPLETENESS" comment and the strict xfail in
  `tests/test_scoped_join_permutations.py` flag this exact substitution-vs-FULL
  tension; cf. MEMORY `handoff_scoped_join_substitution_removal.md`
  ("Substitution KEPT for INNER/merge/FULL-registry").
- Likely fix direction: a rowset⇄rowset FULL must materialize BOTH rowset CTEs
  and emit a real `FULL OUTER JOIN` on the composite key with per-side null-
  preserving columns (the canonical SQL shape), never identity-substitute the
  two keys. At minimum, detect this collapse and raise rather than return a
  silent wrong answer.
- Secondary (guidance): promote the cross-model only-in-A / only-in-B / both
  presence-flag idiom (the `all_sales`+`max(case when channel=…)` shape from the
  canonical) to a discoverable agent-info example, and stop pointing set-
  comparison cross-model questions at the (broken) rowset full-join path.

## Verified
- Canonical `tests/modeling/tpc_ds_duckdb/query97.preql` builds (generate_sql OK)
  and `query97.sql` confirms the FULL OUTER JOIN reference shape.
- Agent's final shipped `query97.preql` (concat key + `count_distinct(k ? k in/not in other)`)
  runs clean → `540709, 286686, 171`; arithmetic reconciles to the per-set totals.
```
