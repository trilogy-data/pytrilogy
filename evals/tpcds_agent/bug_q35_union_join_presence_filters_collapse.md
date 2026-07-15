# q35: `union join` collapses side-presence filters onto the coalesced key

**Status:** OPEN  
**Classification:** FRAMEWORK BUG (silent wrong result)  
**Run:** `evals/tpcds_agent/results/20260714_200120_enriched`  
**Impact:** q35 fails scoring with 100 candidate rows versus 100 reference rows;
only 24 rows overlap. The authored query runs without an error, warning, or
render sentinel.

## Summary

The q35 candidate expresses the required set operation directly:

```text
store AND (web OR catalog)
```

It builds one customer-key rowset per channel, connects each rowset to the
customer axis with `union join`, and uses side-specific `is not null` presence
tests. Trilogy generates SQL that does not preserve those predicates.

In a projection-only minimization, the side-specific tests collapse onto the
coalesced union key and admit customers found in **any** channel: 36,071 rows
instead of the correct 5,042. In the full q35 aggregate, projection/planning
changes the bad rewrite: Trilogy inner-joins the store rowset to the web key
before it left-joins catalog, so catalog-only qualifying customers can no
longer satisfy the authored `web OR catalog` branch.

Both generated forms contradict the documented `union join` contract: the
join declares a row-preserving union of domains, while explicit presence
predicates decide which sides are required. A query must not change set
semantics when extra properties or aggregates are projected.

## Original artifact

- Candidate: `results/20260714_200120_enriched/workspace/query35.preql`
- Conversation: `results/20260714_200120_enriched/agent_log.q35.conversation.txt`
- Reference: `tests/modeling/tpc_ds_duckdb/query35.{sql,preql}`

The candidate's relevant final block is:

```trilogy
select
    cust.address.state,
    cust.demographics.gender,
    # demographic grouping and aggregates omitted
union join store_cust.customer_sk = cust.sk
union join web_cust.customer_sk = cust.sk
union join catalog_cust.customer_sk = cust.sk
where cust.demographics.sk is not null
  and store_cust.customer_sk is not null
  and (web_cust.customer_sk is not null
       or catalog_cust.customer_sk is not null);
```

This is a valid spelling of the business predicate. The canonical answer uses
membership instead and scores correctly.

## Minimal reproduction

Use the q35 run workspace and its scale-factor-one DuckDB database:

```trilogy
import raw.store_sales as ss;
import raw.web_sales as ws;
import raw.catalog_sales as cs;
import raw.customer as cust;

with s as
where ss.date.year = 2002 and ss.date.quarter in (1, 2, 3)
select ss.customer.sk as sk;

with w as
where ws.date.year = 2002 and ws.date.quarter in (1, 2, 3)
select ws.billing_customer.sk as wk;

with c as
where cs.sold_date.year = 2002 and cs.sold_date.quarter in (1, 2, 3)
select cs.ship_customer.sk as ck;

select cust.sk
union join s.sk = cust.sk
union join w.wk = cust.sk
union join c.ck = cust.sk
where s.sk is not null
  and (w.wk is not null or c.ck is not null);
```

Execute through the eval scoring engine:

```python
import sys
from pathlib import Path

sys.path.insert(0, "evals")
from common import scoring

ws = Path("evals/tpcds_agent/results/20260714_200120_enriched/workspace")
eng = scoring.make_scoring_engine(ws / "tpcds.duckdb", ws, "tpcds")
sql = eng.generate_sql(BODY)[-1]
rows = list(eng.execute_raw_sql(sql).fetchall())
```

Observed on the current tree:

- explicit `union join` form: **36,071** customer keys;
- equivalent membership form below: **5,042** customer keys;
- union-join result: all 5,042 expected keys plus **31,029 extras**.

Correct control:

```trilogy
where cust.sk in s.sk
  and (cust.sk in w.wk or cust.sk in c.ck)
select cust.sk;
```

## Generated-SQL evidence

The minimized query initially builds the correct full coalescing axis:

```sql
FULL JOIN s ON w.wk IS NOT DISTINCT FROM s.sk
FULL JOIN c ON coalesce(w.wk, s.sk) IS NOT DISTINCT FROM c.ck
FULL JOIN customer
  ON coalesce(w.wk, s.sk, c.ck) = customer.c_customer_sk
```

Later CTEs re-source the same statement through individual sides. The authored
side probes no longer remain independent, and the WHERE condition is rendered
against coalesced keys. Consequently `s.sk is not null` can be satisfied by a
web/catalog group mate, and the OR is effectively true for any populated arm.

The full candidate takes a different invalid plan. Its `late` CTE contains:

```sql
FROM store_cust
INNER JOIN concerned
  ON store_cust.customer_sk = concerned.web_cust_customer_sk
```

`concerned` is the customer/web merge. Joining store to its **web member**
forces web membership before catalog is introduced. Catalog is only attached
later with a left join, so the authored `web OR catalog` condition cannot
recover store+catalog customers that lack web sales.

The change from 36,071 projection-only rows to the full candidate's different
wrong set is itself important: adding demographic projections and aggregates
changes the logical meaning of the same joins and predicates.

## Trigger matrix

| Shape | Result |
|---|---|
| Three rowsets, three `union join`s, side-specific `store AND (web OR catalog)` probes | **Wrong:** 36,071 vs 5,042 |
| Same logic with the order of web/catalog join declarations swapped | **Wrong:** 36,071 vs 5,042 |
| Full q35 demographic projection and aggregates | **Wrong:** generated store-to-web INNER join; 24/100 scored rows overlap |
| Same three rowsets using `in` membership | **Correct:** 5,042 keys |
| Canonical q35 PreQL using staged membership | **Correct** |

This rules out data, question wording, customer-role selection, and scorer
ordering. The discriminator is scoped union-key presence testing versus
membership; projection shape changes the particular bad plan but does not fix
it.

## Likely root cause

The failure is in preservation of a union-group member's identity after key
canonicalization. All key-group members share the coalesced canonical address,
but `member is not null` must remain pinned to the member's own source.

Relevant implementation areas:

- `trilogy/core/processing/v4_helper/source_planning.py:779` —
  `_datasource_renders_probe`; its comment describes exactly the failure mode:
  computing a member probe on the complement side makes it never-null and the
  test a silent no-op.
- `trilogy/core/processing/node_generators/presence_probe.py:228` —
  `gen_presence_probe_node`; materializes `_virt_presence_*` and contains a
  shortcut at lines 294-317 that omits the complement axis when conditions are
  believed to prove the probe non-null.
- `trilogy/core/processing/condition_utility.py:1048` — non-null proof walking,
  especially the intersection rule for OR conditions.
- `trilogy/core/processing/join_resolution.py:111` — join-type selection. The
  source states union-declared keys must remain FULL unless a narrowing pass
  proves row identity; the full q35 store-to-web INNER join violates that
  intended invariant for the authored predicate.

The precise upstream defect still needs tracing through the built plan, but the
observable corruption is clear: at least one rowset-member probe is substituted
with its union group's canonical/coalesced key, while another projection shape
allows the narrowing pass to treat the web member as authoritative for the
store join.

This is distinct from the earlier fixed q35
`INVALID_REFERENCE_BUG` documented in
`bug_q35_subset_join_rowset_render_sentinel.md`. That bug was loud and involved
`subset join`; this one is silent and involves `union join` presence semantics.

## Expected behavior

1. Each `rowset.member is [not] null` test must be computed from that member's
   own source before coalescing.
2. OR conditions must not prove either individual branch non-null.
3. A statement-scoped `union join` must remain row-preserving unless narrowing
   is proven safe for all authored member-presence predicates.
4. Adding unrelated projections or aggregates must not alter the qualifying
   key set.

## Fix and regression-test direction

Add a small three-source test with overlapping key sets:

```text
store   = {1, 2, 3}
web     = {1}
catalog = {2}
customer= {1, 2, 3, 4}
```

Assert that `store AND (web OR catalog)` returns `{1, 2}`, both with a key-only
projection and with dimension properties plus aggregates. Run the same test
with join declarations reordered. Inspect generated SQL to assert:

- no side-presence predicate is rendered solely against the coalesced group
  key;
- catalog is available before evaluating the OR;
- no store-to-web INNER join appears merely because store and web probes are
  referenced;
- the result matches the equivalent staged-membership query.

The existing rowset presence-probe and scoped-union regression suites are the
natural home for the minimized case.

## Workaround

Use staged membership for existence logic:

```trilogy
where customer.sk in store_buyers.store_cust_id
  and (customer.sk in web_buyers.web_cust_id
       or customer.sk in catalog_buyers.cat_cust_id)
```

This is the canonical q35 form and produces the reference result.
