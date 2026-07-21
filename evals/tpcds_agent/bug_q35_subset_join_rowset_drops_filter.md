# q35 — `subset join <rowset> = <model>` silently drops the rowset's WHERE filter (full-population wrong result)

Run: `evals/tpcds_agent/results/20260720-140600` (INGEST leg, Trilogy 0.3.295)
Status: **FAIL** "result set differs from reference" · 540,858 tokens (was PASS @135k on 0.3.293)
Class: **FRAMEWORK — silent wrong-result (grain / lost-restriction).** Regression coinciding
with the recent subset-join rowset-enrichment rework (q74). Compounded by a guidance gap.

## Symptom
q35 restricts customers to a population (store buyers in Q1–3 2002 who are also web/catalog
buyers) and aggregates demographic groups. The agent expressed the population restriction with
`subset join cust_set.cust_sk = customer.customer_sk`. The query runs cleanly with 100 rows and
the right columns — but the per-group `count(customer_sk)` comes back **~530** instead of **1**.
The generated SQL contains **zero** references to `store_sales` / `web_sales` / `catalog_sales`
/ `date_dim` / `2002`: the entire `cust_set` CTE (which carries the population filter) was pruned.
The query silently aggregates the FULL customer base (F=48,142 / M=48,420) instead of the
qualifying ~11k. No error, so the agent could not tell the result was wrong and burned 540k
tokens cycling through subset-join reformulations (and several Discovery errors) before submitting.

## Minimal repro (against the run's `workspace/`)
```
import raw.store_sales as store_sales;
import raw.customer as customer;
with cust_set as
where store_sales.date_dim.year = 2002 and store_sales.date_dim.qoy in (1,2,3)
select store_sales.customer.customer_sk as cust_sk;
where customer.customer_demographics.demo_sk is not null
select customer.customer_demographics.gender as gender, count(customer.customer_sk) as cnt
subset join cust_set.cust_sk = customer.customer_sk;
```
Gives `F=48142` (full population, `store_sales` absent from SQL). Correct answer = `F=11339`.

## Trigger matrix (toggle one ingredient)
| # | form | store_sales in SQL? | cnt(F) | verdict |
|---|------|---------------------|--------|---------|
| A | `subset join cust_set.cust_sk = customer.customer_sk`, `count(customer_sk)` | NO | 48142 | WRONG |
| B | same but `count(grain(cust_set.cust_sk))` (agent's actual form) | NO | 961579 | WRONG |
| C | rowset directly as subset side (no intermediate `cust_set`) | NO | 48142 | WRONG |
| E | subset join, **project** `cust_set.cust_sk` in output | NO | 96564 (all custs) | WRONG |
| I | flipped operands `subset join customer.customer_sk = cust_set.cust_sk` | NO | 48142 | WRONG |
| J | both sides rowsets | NO | 48142 | WRONG |
| **H** | `subset join store_sales.customer.customer_sk = customer.customer_sk` (two **datasource** keys) | **YES** | **11339** | **CORRECT** |
| **D** | `customer.customer_sk in cust_set.cust_sk` (membership idiom) | **YES** | **11339** | **CORRECT** |

Discriminator: **the subset side being a ROWSET** drops the filter. Operand order, `grain()`,
projecting the key, and one-vs-two rowsets make no difference. A subset join between two
datasource keys (H) restricts correctly; the `in` membership idiom (D) is the canonical answer
(the enriched reference `tests/modeling/tpc_ds_duckdb/query35.preql` uses `in`, not subset join).
Reconstructing the full q35 with `in` membership reproduces the reference row-for-row (cnt=1).

## Root cause (file:line)
`subset join a = b` lowers to a `LEFT_OUTER` scoped join with the **anchor as source** and the
**subset side as target**: instrumented tuple = `('customer.customer_sk','cust_set.cust_sk',
LEFT_OUTER)`, `scoped_merge_map = {'cust_set.cust_sk':'customer.customer_sk'}`,
`subset_sources()=['cust_set.cust_sk']` (subset side correctly identified).

The rowset-identity-preservation guard that would keep `cust_set.cust_sk` as its own column
only fires when the rowset is the **source** of a LEFT_OUTER:
- `trilogy/core/models/build.py:2619-2633` `_rowset_outer_pair` — for `LEFT_OUTER` returns
  `_is_rowset_keyed(s)` (SOURCE only). Here the rowset is the TARGET, so it returns False →
  `scoped_rowset_outer_sources/targets` are empty.
- `trilogy/core/models/build.py:2641-2646` `scoped_key_merge_map` therefore still contains
  `cust_set.cust_sk`, and `_build_concept` (`build.py:3071-3084`) substitutes its address to
  the ROOT canonical `customer.customer_sk`. Confirmed: `env.concepts['cust_set.cust_sk']
  .address == 'customer.customer_sk'`, derivation rewritten ROWSET→ROOT.

Because the rowset member is now indistinguishable from the anchor:
- `trilogy/core/models/build_environment.py:140-157` `_distinct_scoped_join_groups` drops the
  group (its `concept.address == member` test fails for the substituted member → <2 distinct).
- so the dedicated q35 safeguard `pseudonym_unsatisfiable_group_mates`
  (`build_environment.py:196-233`) — which is *written specifically to stop* "silently swapping
  in the mate's row population (q35 `subset join` between rowsets)" — never sees the group and
  returns `{}`. Its own guard at line 227 (`if any(... derivation != ROWSET): continue`) also
  excludes any group with a ROOT member, so even a distinct group here would be exempted.

Net: the subset restriction becomes a prunable LEFT enrichment; the join key is sourced straight
from `customer`, `cust_set`'s body (and its 2002/Q1–3 WHERE) is never sourced, and the result is
the full customer population. The q74 rework (`node_generators/rowset_node.py`,
`concept_strategies_v3.py`, currently uncommitted) retargeted subset resolution to "source the
other side and merge locally" — the exact path that, with a ROOT anchor, reads the anchor in full
and drops the restrictor.

## Resolution (2026-07-20)

**Regression attribution corrected**: the minimal repro fails IDENTICALLY at 0.3.294
(c6535202e) and at fe61fa56a (several releases back) — A/B'd via worktree against the same
run workspace. This is NOT a regression from the q74 subset-rowset-enrichment rework; the
hole is long-standing. The 0.3.293 PASS was the agent choosing a different formulation
(the run-to-run change is agent behavior, plausibly steered by the #603 agent-feedback
rework), not an engine behavior change.

**Fixed** — two-part, option (a):
1. `build.py` `_rowset_outer_pair`: a LEFT (subset) pair now also engages
   rowset-identity preservation when the rowset is the TARGET (subset side), unless the
   anchor is a derived-expression key (that pairing needs the substitution path for the
   derived-key machinery — pinned by `test_composite_matrix.py plain_derived-subset`).
   The rowset member keeps its own column; its body (and WHERE) is sourced when referenced.
2. `build_environment.py` `pseudonym_unsatisfiable_group_mates`: widened from
   all-rowset-member groups to any subset group pairing involving a ROWSET member, in both
   directions — a rowset member can't be credited off the anchor's pseudonym (original
   q35), and the ROOT anchor can't be credited off the rowset's pseudonym (mirror leak:
   projecting the member silently narrowed the anchor to the rowset's own source).
   All-ROOT groups (binding substitution) and derived-key groups stay exempt.

**Post-fix contract** (pinned in `tests/join_matrix/test_subset_join_rowset_onto_root.py`):
- `where cust_set.cust_sk is not null` + subset join now restricts correctly (workspace
  repro: F=11339 / M=11365 == reference). Trigger-matrix F/E/B become honest.
- The bare declaration (form A, no authored reference to the member) intentionally does
  NOT restrict — subset join declares domain knowledge, not row intent
  (docs/subset_union_join_design.md); the rowset stays prunable. The agent-guidance answer
  for population restriction remains the `in` membership idiom (D) or an explicit
  member null-test.
- v4 planner gap: projecting the member INNER-collapses the anchor instead of preserving
  the coalesced axis — registered in `tests/join_matrix/conftest.py` V4_FAILING.

## Fix locus (do NOT fix here)
Either (a) make `_rowset_outer_pair`/identity-preservation engage when the rowset is the TARGET
of a subset (`LEFT_OUTER`) join so the rowset key keeps its own column and its body is sourced,
and make the subset relation a restricting semi-join rather than a prunable enrichment; or (b)
fail loudly that `subset join` cannot restrict via a rowset and point at the `in` membership
idiom. Silently returning the full population is the dangerous failure — the token bar was its
only detector.
