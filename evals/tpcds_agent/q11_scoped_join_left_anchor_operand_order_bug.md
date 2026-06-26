# Bug: chained scoped joins fail when the same key is the LEFT operand of ≥2 joins (operand-order sensitivity) (q11)

**Status:** FIXED 2026-06-26. Regression test `tests/test_scoped_join_star_anchor.py`.

## Root cause (the memory's union-find hypothesis was WRONG)

`_build_scoped_merge_index` is order-INSENSITIVE: a passing and a failing
arrangement produce the *identical* merge_map (e.g. `s01=s02,w01=s01` OK and
`s01=w01,s01=s02` FAIL both yield `{s01:s02, w01:s02}`). The bug was downstream:
three INNER membership sets in `Factory.__init__` (`scoped_rowset_inner_sources`,
`scoped_merge_sources`, `scoped_pseudonym_sources`) were computed from the raw
per-pair **source operand** only. INNER equality is symmetric, so union-find may
root EITHER endpoint as canonical and may collapse the other transitively — a
repeated-left-anchor star (`a=b, a=c`) roots `a,c` onto `b`, but `c` was only
ever a join *target*, so it never entered those sets. For a rowset spoke that
meant `_build_concept` (build.py:2786) substituted `c` onto the canonical,
dropping `c`'s own WHERE filter and making `c.rev` unsourceable →
DisconnectedConceptsException. Fix: wire every collapsed endpoint (those present
in `scoped_merge_map`), not just the authored source.

---

**Original report (OPEN — confirmed, 100% deterministic, cleanly isolated):**
**Surfaced by:** TPC-DS q11 enriched eval (run `20260626-031753`) — the agent's natural star-join
form was unresolvable; it churned trying merges and rewrites.
**Severity:** HIGH — a natural multi-rowset star join (common anchor on the left of every join)
fails with a misleading "not joinable" discovery error, even though the same join group resolves
fine when written any other way.

## Symptom

```
Resolution error: Discovery error: couldn't source all these concepts into one query; you may
need a join or merge to relate them across models. Sourced individually but not joinable from
model: {s01.fname, s01.lname, s01.login, s01.pcf, s01.rev, s02.rev, w01.rev, w02.rev}
```

## Trigger (isolated — identical projections, only operand order varies)

```trilogy
import raw.store_sales as store;
import raw.web_sales as web;
rowset s01 <- where store.date.year=2001 select store.customer.id as cust_id, store.customer.login as login, sum(store.ext_list_price) as rev;
rowset s02 <- where store.date.year=2002 select store.customer.id as cust_id, sum(store.ext_list_price) as rev;
rowset w01 <- where web.date.year=2001 select web.billing_customer.id as cust_id, sum(web.ext_list_price) as rev;
select s01.login, s01.rev as r1, s02.rev as r2, w01.rev as w1
inner join s01.cust_id = s02.cust_id
inner join s01.cust_id = w01.cust_id     -- s01 is the LEFT operand of BOTH joins
limit 5;
```

| Join form (same group `{s01, s02, w01}`) | Result |
|---|---|
| `s01=s02`, `s01=w01` (s01 left of both) | **FAILS** (DisconnectedConcepts) |
| `s02=s01`, `s01=w01` (s01 pivot middle) | OK |
| `s01=s02`, `w01=s01` (s01 right of second) | OK |
| `s01=s02=w01` (chained `=`) | OK |

So the failure is specifically: **the same key as the LEFT operand of ≥2 separate scoped joins.**
Putting that key on the right of *either* join, or chaining with `=`, resolves it. This is the
natural way to write a star join (anchor = spoke1, anchor = spoke2, …), so it bites hard.

Not the merge: fails identically with and without `merge store.customer.id into ~web.billing_customer.id`.
Not cross-model: a single cross-model rowset join (`s01=w01`) works; same-model 2-rowset works. The
trigger is purely the repeated-left-anchor operand arrangement across ≥2 joins.

## Likely fix area

The scoped-join equivalence-group resolution (`_build_scoped_merge_index` / `scoped_merge_map` in
`trilogy/core/query_processor.py`, and how the per-join `(source, target)` pairs union into groups).
`a=b` then `a=c` must union `{a, b, c}` exactly as `b=a`, `a=c`, or `a=b=c` do — the union/canonical
assignment is currently order-sensitive when one key is the repeated left (source) operand. Same
family as the prior scoped-join non-commutativity / operand-order fixes
(`project_bridge_dim_subset_prune`, `reference_scoped_join_blend_semantics`).

## Repro harness

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-031753/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
eng.generate_sql(open('repro.preql').read())   # DisconnectedConceptsException
```
