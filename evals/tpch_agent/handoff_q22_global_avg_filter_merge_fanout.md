# Handoff: `by *` global-average WHERE filter over a merged model duplicates output rows

**Status:** OPEN regression (found 2026-06-12, TPC-H rebaseline). Was PASS in the
20260605 ingest baseline; the SAME generated query now fails on the current
engine — so it's a framework regression introduced somewhere in the current
join/rowset/merge stack (the enrichment-machinery consolidation is the prime
suspect; see `project_enrichment_machinery_consolidation`).

## Symptom

TPC-H q22 (ingest leg). The prior-passing generated query now returns **641 rows
instead of 7** — each correct aggregated group row is duplicated ~90x:

```
('13', 94, 714035.05)
('13', 94, 714035.05)   <- same row repeated ~90x per cntrycode
...
```

641 rows but only **7 distinct cntrycodes**; the values are correct, the rows are
multiplied. (Without the anti-join clause it's 1855 rows — the fan-out is
independent of the anti-join.)

## Repro (deterministic)

```python
import sys; sys.path.insert(0, 'evals'); from common import scoring
from pathlib import Path
pw = Path('evals/tpch_agent/results/20260605-130243_ingest/workspace')
eng = scoring.make_scoring_engine(pw / 'tpch.duckdb', pw, 'tpch')
print(scoring.score_query(eng, pw, 22, 'tpch'))          # -> fail, ref=7 cand=641
```

The generating query is `…/20260605-130243_ingest/workspace/query22.preql`.

## Minimal trigger (bisected by clause deletion on that preql)

The query: `merge customer into ~orders`, then group customers by `cntrycode`
(first 2 chars of phone), filtered to 7 country codes, `acctbal > 0`, **and**
`acctbal > avg_pos_balance` where `avg_pos_balance <- avg(acctbal ? …) by *` (a
single global scalar), plus `orders.orderkey is null` (anti-join).

| Variant | Result |
|---|---|
| FULL | **641 / 7 codes (BUG)** |
| remove `and acctbal > avg_pos_balance` (the `by *` global-avg filter) | **7 / 7 codes — CORRECT** |
| remove `and cntrycode in (...)` (the co-filter, keep avg filter) | 25 / 25 — clean |
| remove `orders.orderkey is null` (anti-join) | 1855 / 7 — still fans out |
| remove the `merge` | discovery error (merge needed to import orders) |

**Trigger = a `by *` global-average aggregate compared in WHERE
(`acctbal > avg_pos_balance`), together with a second row predicate
(`cntrycode in (...)`), over the merged model.** Either WHERE predicate alone is
clean; the two together (with the merge) duplicate every output group row.

A `by *` aggregate is a single scalar — comparing against it in WHERE should join
as a 1-row cross product and NOT change cardinality. The regression joins the
global-average CTE at the wrong grain, multiplying the post-aggregation rows.

## Where to look

- The `by *` / whole-grain global-aggregate path (see
  `project_global_aggregate_having_collapse`) interacting with the merge /
  enrichment node. The global-average CTE is presumably joined into the main
  query at a non-collapsed grain when a second WHERE predicate co-exists.
- `gen_enrichment_node` / merge enrichment machinery
  (`project_enrichment_machinery_consolidation`) — the merged `~orders` source
  may be fanning the customer grain before the global-average join.
- Compare generated SQL of FULL vs the `−avg-filter` variant: the extra/duplicate
  rows should trace to the join that brings in `avg_pos_balance`.

## Notes

- enriched leg q2 also flipped pass→fail this run but is **variance** (its prior
  preql still passes on the current engine) — NOT a regression. q22 (ingest) is
  the only real TPC-H regression; the join/rowset stack otherwise held
  (enriched 20→21/22, ingest 21→20/22, net flat).
- Likely the same fan-out family as the scoped-join operand-order bug
  (`bug_scoped_join_operand_order_noncommutative.md`); may share a root cause in
  the merge/enrichment grain handling. Worth checking together.
