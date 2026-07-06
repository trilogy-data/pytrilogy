# q54 token sink (713k→1.39M) — `subset join` onto a ROWSET key mis-renders as FULL, silently admits non-members

Date: 2026-07-06
Run: `evals/tpcds_agent/results/20260706-222300` (q54 PASSES, ref 1 row / cand 1 row)
Prior: `20260706-135542_enriched` (713k) → new 1.39M (+681k). No prior report.

## Verdict

**REAL FRAMEWORK BUG (silent wrong rows), not agent error, not a guidance defect.**
A query-scoped `subset join a = b` whose **superset anchor `b` is a ROWSET-derived key**
is rendered as a symmetric **FULL JOIN + coalesce** instead of the directional **LEFT
OUTER JOIN** it correctly emits when the anchor is a plain datasource key. The FULL join
invents rows from the partial (subset) side that do **not** match the anchor, so
**non-members of the declared superset leak into the result**. This is exactly the
subset-declaration contract (`a ⊆ b`, "a rows all match", superset-anchored,
row-preserving) being violated.

The "Resolution error … Could not resolve connections for query with output
['local.cat_cust…AGGREGATE', 'local.web_cust…AGGREGATE']" line in the log (jsonl rec 65)
is a **red herring**: it was the agent's ad-hoc probe `select count(cs…) as cat_cust,
count(ws…) as web_cust` over two disconnected facts (catalog + web) in one select — a
CORRECT disconnect the agent immediately understood ("Can't query two different facts in
one select"). It is not the sink driver.

## What actually drove the 1.4M tokens

The agent produced a correct final query using the `in` (semi-join) membership idiom
(1 customer, segment 880). It burned ~600k tokens because a parallel **`subset join`
attempt returned a DIFFERENT, larger customer set** and it could not tell which was
right. Sequence (from `agent_log.q54.jsonl`):

- `in` approach `ss.customer.id in all_qual.cid` → **1 customer** (26788). CORRECT.
- `subset join ss.customer.id = all_qual.cid` (query54_check.preql, rec 58) → **2
  customers** (26788 **and 63889**).
- Agent spent recs 63–107 cross-checking: catalog counts (1071), web counts (560),
  per-customer membership probes, whether 63889 qualifies, etc.
- Concluded 63889 is NOT a Women/maternity Dec-1998 buyer, so the subset join was wrong,
  and shipped the `in` version.

The extra WRONG row (63889) that the framework silently emitted is what destroyed the
agent's confidence in its own correct answer.

## Reproduction (current engine, trilogy 0.3.290)

q54 workspace, two variants (built + run):
- `subset join ss.customer.id = all_qual.cid` → rows `(26788, 43985.47)`, `(63889, 33257.17)` — **2 rows**.
- `ss.customer.id in all_qual.cid` → `(26788, 43985.47)` — **1 row**.
- Membership check: `all_qual.cid in (26788, 63889)` → only **26788**. So 63889 is a
  non-member and the `in` result is correct; the subset join leaked a non-member.

Generated SQL for the subset-join variant (last CTE):
```sql
FROM "sparkling"                    -- all_qual.cid  (the ROWSET superset anchor)
FULL JOIN "kaput" on "sparkling"."all_qual_cid" = "kaput"."ss_customer_id"   -- ss side
...
coalesce("kaput"."ss_customer_id","sparkling"."all_qual_cid") as "cust_id"
```
`FULL JOIN` preserves `kaput` (store-sale) rows even when the customer is not in
all_qual → 63889 survives. `having store_total is not null` does NOT drop it (63889 has a
non-null store total).

### Minimal isolation (synthetic 2-table model, scratchpad)

sales = {1,2,3,9}, cohort members = {1,2,3,7}:
- `subset join s.cust = members.mid` (members = a ROWSET) →
  `1, 2, 3, 7(null), 9(999)` — **FULL JOIN**: non-member sale **9 leaks**, and
  `coalesce("members_mid","s_cust")` in the SQL.
- Same query but `in members.mid` → `1, 2, 3`. CORRECT.
- `subset join s.cust = c.ccust` where `c` is a **plain datasource** (not a rowset) →
  `1, 2, 3, 7(null)` and SQL renders `cohort LEFT OUTER JOIN sales on c.cust = s.cust`.
  **CORRECT directional LEFT**: preserves the anchor (member 7 with null), excludes the
  non-member sale 9.

Only-difference-that-flips-the-behavior = **anchor is a ROWSET key vs a plain key.**

## Root cause (file:line)

1. `trilogy/parsing/v2/rules/select_statement_rules.py:270-276` —
   `subset join a = b` normalizes to `SelectJoin(join_type=LEFT_OUTER,
   source_address=b, target_address=a, authored=SUBSET)`. So for
   `subset join ss.customer.id = all_qual.cid`, **source = the superset anchor =
   all_qual.cid (a ROWSET key)**. Correct: it should be a superset-anchored LEFT.

2. `trilogy/core/models/build.py` `_rowset_outer_pair` (~lines 2400-2414):
   ```python
   def _rowset_outer_pair(s, t, jt):
       if jt is JoinType.LEFT_OUTER:
           return _is_rowset_keyed(s)          # <-- fires for subset's rowset ANCHOR
       if jt is JoinType.FULL:
           ...
   ```
   Because the LEFT_OUTER **source** (the subset's superset anchor) is rowset-keyed,
   the anchor is added to `scoped_rowset_outer_sources`/`scoped_outer_identity_sources`
   (build.py:2416-2454). That engages the **coalescing rowset-outer-identity** path
   (designed for genuinely coalescing FULL / both-derived-key cases, e.g. q97): both
   endpoints keep identity and the merge coalesces the key columns, which renders a
   symmetric **FULL JOIN**. The directional LEFT semantics of the SUBSET declaration are
   lost precisely (and only) when the superset anchor happens to be a rowset output.

The plain-datasource anchor skips this path (`_is_rowset_keyed` is False → stays
LEFT_OUTER → correct `LEFT OUTER JOIN`), which is why the same construct is correct there.

## Fix direction (NOT applied)

`_rowset_outer_pair` should not promote a **subset** join (authored=SUBSET / a
superset-anchored LEFT) to the coalescing rowset-outer path merely because the anchor is
a rowset key: a subset join is directional and must preserve only the authoritative
(anchor) side. Candidate: gate the LEFT_OUTER branch of `_rowset_outer_pair` on
`authored != JoinType.SUBSET`, or thread the authored SUBSET through so the rowset-outer
machinery renders a directional (anchor-preserving) LEFT rather than a coalescing FULL.
Guard would live in the join_matrix oracle: "subset join onto a rowset superset anchor
excludes non-anchor rows" (the synthetic above).

## Notes
- Reference `tests/modeling/tpc_ds_duckdb/query54.sql` is the FIXED one
  (`AND ss_store_sk = s_store_sk` present, line 32); the old store-fanout is not involved.
- Agent's final `in`-based query is correct and scores pass; no fix needed to make q54
  pass — the fix removes the silent wrong-rows trap that costs tokens.
