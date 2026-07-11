# q35 — `subset join` between rowsets strands the subset member's key (render sentinel)

**STATUS: FIXED 2026-07-11.** Root cause was stack validation crediting an
unbound (rowset) subset-join member as "found" through its group-mate
pseudonym, so the member's rowset was never built. Fix: validation consumes a
widened mates map (`BuildEnvironment.pseudonym_unsatisfiable_group_mates`) —
an unbound, author-REFERENCED member of a subset group is never satisfiable
via a mate's pseudonym (authorship = new `statement_authored_addresses`
closure captured in `get_query_node`, computed pre-canonicalization so a
grain canonicalized onto an unreferenced anchor still collapses per the ruled
domain-metadata semantics). Guard tests:
`tests/join_matrix/test_subset_join_between_rowsets.py` (incl. the full
3-rowset or-of-members q35 shape). Note: projecting a member key yields the
coalesced group axis (union-join parity), not a per-side NULL-padded column —
per-side absence is the `is not null` presence-probe idiom.

**Classification:** FRAMEWORK BUG (loud). The engine's own codegen emits an
`INVALID_REFERENCE_BUG` render sentinel and raises `ValueError: Could not render
the query`. Per our rules a render-sentinel from generated SQL is always a
framework bug — the engine must compile the shape or reject it with a clean
authoring error, never emit self-inconsistent SQL.

**Cost in the run:** none. In `results/20260711-042547_enriched` the agent hit
this on its `subset join` draft, saw the loud error, and **recovered** by
rewriting with `in` membership. `report.json` scores q35 `status: pass`. So this
is a real bug but it did not cost a scored fail (it did burn iterations).

## Symptom

Run dir: `evals/tpcds_agent/results/20260711-042547_enriched`
(`agent_log.q35.jsonl` msg 21 / `.conversation.txt:3527`).

Running the agent's `subset join` draft of query35.preql produced:

```
Could not render the query: Missing source reference to ws.billing_customer.sk.
A planned reference has no backing source CTE -- typically an unsupported
cross-rowset or membership shape the planner could not wire. ...
Full SQL with sentinel(s): ... WHERE coalesce(INVALID_REFERENCE_BUG<Missing
source reference to ws.billing_customer.sk>) is not null or ...
```

The failing construct (three single-key rowsets combined via repeated
`subset join` on the same key, then an `or`-of-two null tests):

```trilogy
with store_cust as
where ss.date.year = 2002 and ss.date.quarter in (1,2,3)
select ss.customer.sk as cust_sk;

with web_cust as
where ws.date.year = 2002 and ws.date.quarter in (1,2,3)
select ws.billing_customer.sk as cust_sk;

with catalog_cust as
where cs.sold_date.year = 2002 and cs.sold_date.quarter in (1,2,3)
select cs.ship_customer.sk as cust_sk;

with target_cust as
select store_cust.cust_sk as c_sk
subset join web_cust.cust_sk = store_cust.cust_sk
subset join catalog_cust.cust_sk = store_cust.cust_sk
where web_cust.cust_sk is not null or catalog_cust.cust_sk is not null;
```

Imports are the auto-ingested `raw/*.preql` in that workspace:
`import raw.store_sales as ss; import raw.web_sales as ws; import raw.catalog_sales as cs;`

## Minimal repro

Two rowsets, ONE subset join, a single reference to the subset member's key is
enough — three rowsets / the `or` are not required:

```trilogy
import raw.store_sales as ss;
import raw.web_sales as ws;

with store_cust as
where ss.date.year = 2002 and ss.date.quarter in (1,2,3)
select ss.customer.sk as cust_sk;

with web_cust as
where ws.date.year = 2002 and ws.date.quarter in (1,2,3)
select ws.billing_customer.sk as cust_sk;

with target_cust as
select store_cust.cust_sk as c_sk
subset join web_cust.cust_sk = store_cust.cust_sk
where web_cust.cust_sk is not null;      -- references the SUBSET member's key

select target_cust.c_sk;
```

How to run (engine harness, no agent):

```python
import sys; sys.path.insert(0, "evals"); from common import scoring
from pathlib import Path
ws = Path("evals/tpcds_agent/results/20260711-042547_enriched/workspace")
eng = scoring.make_scoring_engine(ws/"tpcds.duckdb", ws, "tpcds")
eng.generate_sql(BODY)          # raises "Could not render ... Missing source reference to ws.billing_customer.sk"
```

Emitted SQL for the minimal case — only the preserving anchor `store_cust`
(`wakeful`) is scanned; `web_cust` is **never** emitted as a CTE nor joined, yet
its key is referenced in the WHERE:

```sql
WITH wakeful as (
  SELECT "ss_store_sales"."SS_CUSTOMER_SK" as "_store_cust_cust_sk"
  FROM "store_sales" ... WHERE D_YEAR=2002 and D_QOY in (1,2,3) GROUP BY 1)
SELECT "wakeful"."_store_cust_cust_sk" as "target_cust_c_sk"
FROM "wakeful"
WHERE coalesce(INVALID_REFERENCE_BUG<Missing source reference to ws.billing_customer.sk>) is not null
GROUP BY 1
```

## Trigger matrix

`subset join a = b` declares `a ⊆ b`; `b` (the right side, here `store_cust`) is
the preserving anchor and drives the scan. The discriminator is **whether the
SUBSET member `a`'s key is referenced at all** — in the WHERE *or* the SELECT.
Referencing only the anchor's key is fine.

| # rowsets | join(s) | what references the subset member's key | result |
|---|---|---|---|
| 3 | 2× subset | WHERE `webNN or catNN` (both far members) | **SENTINEL** |
| 2 | 1× subset | WHERE `webNN or storeNN` | **SENTINEL** |
| 2 | 1× subset | WHERE `webNN` (single, subset member) | **SENTINEL** ← minimal |
| 2 | 1× subset | SELECT projects `web_cust.cust_sk` (no WHERE) | **SENTINEL** |
| 2 | 1× subset | SELECT projects `web_cust.cust_sk` + WHERE `webNN` | **SENTINEL** |
| 3 | 2× subset | WHERE `webNN` only (single subset member) | OK* |
| 3 | 2× subset | WHERE `catNN` only (single subset member) | OK* |
| 3 | 2× subset | no WHERE | OK |
| 3 | 2× subset | WHERE `storeNN` (anchor only) | OK |
| 3 | 2× subset | WHERE `storeNN or webNN` | OK |
| 2 | 1× subset | no WHERE (projects only anchor key) | OK |
| 2 | 1× subset | WHERE `storeNN` (anchor only) | OK |
| — | `in` membership (agent's recovery) | `c.sk in store and (c.sk in web or c.sk in cat)` | OK |

Toggles that **don't** matter: number of rowsets (2 vs 3), the `or` vs a single
test, whether the reference is a WHERE null-test vs a plain projection, and
whether the far key is aliased the same as or differently from the anchor.

\*The 3-rowset single-far-member rows (`webNN` / `catNN` alone) render clean,
which is itself unstable/inconsistent with the 2-rowset single case — a
side-effect of which member the second subset join promotes into the scan. It is
not a real "fix"; it just shifts which member ends up stranded.

## Root cause

The renderer walks the `target_cust` node's WHERE/projection and looks each
concept up in the CTE's `source_map`. `web_cust.cust_sk` (`ws.billing_customer.sk`)
is absent, so it falls back to the sentinel string:

- `trilogy/dialect/base.py:1417-1419` — projection/condition path:
  `rval = INVALID_REFERENCE_STRING(f"Missing source reference to {c.address}")`
- `trilogy/dialect/base.py:1568` — membership/existence path, same fallback.
- `BASE_INVALID = "INVALID_REFERENCE_BUG"` (`base.py:272`); strict-mode scan of
  the rendered SQL turns the sentinel into the raised `ValueError`.

That is the emission site, not the cause. The **wiring gap** is upstream in the
subset-join-between-rowsets planner: for `subset join a = b` it keeps only the
preserving anchor `b` (`store_cust`) as the scan source and drops the subset
member `a` (`web_cust`) entirely — it is neither emitted as a CTE nor joined in.
Consequently the member's key never enters any CTE `source_map`, and every
reference to it (projection or condition) strands as `INVALID_REFERENCE_BUG`.
Note the member is dropped even though a `subset join` is supposed to
row-narrow the anchor to keys present in the member — so the narrowing join is
being optimized away along with the source.

The existing safety net does not catch this: `_has_unsourced_leaf`
(`trilogy/core/processing/v4_helper/strategy_builder.py:2236`) fails the plan
only when an **unsourced ROOT leaf OUTPUT** is found. Here the leaf (`store_cust`)
is fully sourced; the stranded concept is a **condition/projection reference to a
different member**, which that guard does not inspect — so the invalid plan slips
through to render instead of failing as a clean `UnresolvableQueryException`.

## Fix direction (not applied)

Either (a) actually wire the subset member `a`'s source into the plan (emit its
CTE and the narrowing join) so `a`'s key columns are available for both the
row-narrowing semantics and any projection/condition reference, or (b) if the
member is intentionally dropped, extend the `_has_unsourced_leaf`-style guard to
also detect condition/projection references with no backing `source_map` entry
and fail the plan with a clean authoring error rather than emitting a sentinel.
Option (a) is the correct one — the query is semantically valid (customers with a
store sale that also appear in web/catalog), and `in` membership compiles it.
