# SPIKE: side-qualified condition on a coalescing join key should bind to that side's RAW column, not the coalesced key

**Type:** engine design spike + scoping. NOT a self-contained bug fix — this hands off a
design question with the groundwork done. Read-only investigation; nothing edited.

## The question (from q59)

When an author conditions on a value that is **explicitly qualified to one side of a join**
(`where next_year.store_code is not null`, or `where next_year.x = 5`), can we make that
condition read that side's **own raw column** instead of the **coalesced key** the merge renders?

Today a side-qualified reference to a merged join key is a pseudonym that collapses to the
canonical (coalesced) concept, so `next_year.store_code` becomes `coalesce(this.k, next.k)` and:
- `next_year.store_code is not null` is TRUE whenever EITHER side is present → a no-op (can't
  express "keep only rows where the next_year side matched" = intersection).
- `next_year.store_code = 'X'` would filter the coalesced value, not next_year's side.

This is the single most common way agents try (and fail) to turn a row-preserving subset/union
join into an intersection — see also q84. The idiomatic workaround is `where <non-key attr of the
ANCHOR side> is not null`, which is non-obvious.

## What already exists (and is the right shape to generalize)

There IS a targeted carve-out: `Factory._coalescing_presence_probe` (`trilogy/core/models/
build.py:3344`). For a null test on a coalescing key member it mints a single-arg `COALESCE(ref)`
"presence probe" concept (`_virt_presence_*`, `PRESENCE_PROBE_PREFIX` in `constants.py`),
materialized on that member's own rowset BEFORE the coalescing merge, that rides through the join
un-fused and is NULL exactly where that side is absent. Projecting the member stays coalesced
(the group axis); only the null TEST is rerouted to the per-side probe. It's wired in
`_build_comparison` (build.py:3383) and the probe passes through the rowset machinery via
`_is_presence_probe` (`trilogy/core/processing/node_generators/rowset_node.py:212`).

This already embodies the exact principle the spike wants — **projection uses the coalesced
value; a side-qualified condition uses the per-side raw value** — but only for one narrow case.

## Trigger matrix (empirical — where the probe fires vs collapses to a no-op)

Repro harness (build engine once):
```
cd evals && ../.venv/Scripts/python.exe -c "
import sys; sys.path.insert(0,'.')
from pathlib import Path; from common import scoring
ws=Path('tpcds_agent/results/20260707-151529/workspace')
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
base='''import raw.store_sales as ss;
rowset a <- where ss.date.year=2001 select ss.store.id as sid, ss.store.text_id as scode, sum(ss.ext_sales_price) as amt,;
rowset b <- where ss.date.year=2002 select ss.store.id as sid, ss.store.text_id as scode, sum(ss.ext_sales_price) as amt,;'''
gen=lambda body: ('_virt_presence' in eng.generate_sql(base+body)[-1])
"
```

| join | condition target | presence probe fires? |
|---|---|---|
| `union` | RAW key `b.sid is not null` | **YES** (works today) |
| `subset` | RAW key `b.sid is not null` | no |
| `union` | PROPERTY `b.scode is not null` | no |
| `subset` | PROPERTY `b.scode is not null` | no |
| `subset` join ON `scode`, `b.scode is not null` | no |
| any | `b.k = 'value'` (equality, not null-test) | no (only IS/IS_NOT handled) |

q59 is `subset join this_year.store_code = next_year.store_code` (+ a derived `week_seq-52` key),
condition `where next_year.store_code is not null`, where `store_code = store.text_id`. It misses
on BOTH property-ness and subset-ness → no-op → 18 junk `next_year`-only rows leak into the
top-100 and displace correct rows. (Fix that made it byte-match ref: `where this_year.store_name
is not null` — a non-key attribute of the anchor/preserved side.)

## The three gaps (root cause, file:line)

1. **Only `full`/`union` members are eligible.** `_coalescing_presence_probe` gates on
   `domain_graph.coalescing_relation_members()` (`trilogy/core/domain_graph.py:292`), which
   returns only endpoints of `INCOMPARABLE`+`DECLARED` edges — i.e. `full`/`union` join keys. A
   **subset** join declares a directional `⊆` (SUBSET) relation, so its coalesced key members are
   absent from the set. Yet subset joins DO render row-preserving with a coalesced/mandatory key
   (confirmed: the null test is a no-op), so they have the same problem and want the same probe.
2. **Only the raw key member is eligible.** The gate is `address in coalescing_relation_members()`
   and `member.derivation == ROWSET`. A **property** reachable from the coalescing key
   (`store.text_id`, one hop off `store.id`) is not itself a member, so it's never probed — even
   though conditioning on it is exactly the same "did this side match?" question. Needs: if a
   side-qualified ref's grain/key is a coalescing member, route the presence probe through that
   member's side (and carry the property's per-side value).
3. **Only `IS [NOT] NULL`.** `_build_comparison` (build.py:3384) invokes the probe only for
   `IS`/`IS_NOT` vs `MagicConstants.NULL`. A side-qualified equality/inequality
   (`next_year.store_code = 'X'`, `> N`) still binds to the coalesced value. Generalizing means a
   side-qualified operand in ANY comparison should read the per-side raw column.

## Design principle to validate + the tension

Proposed unifying rule: **projection of a merged key → coalesced group axis (unchanged); a
condition operand explicitly qualified to ONE side → that side's raw (pre-coalesce) column.**
The presence probe is the null-test instance of this; the spike is whether to promote it to a
general rule for side-qualified condition operands.

Tensions / risks to resolve in the spike:
- **Merge semantics.** A join KEY is a declared domain merge; `next_year.store_code` being a
  pseudonym of the canonical key is load-bearing for FD/key implication in the domain graph
  (see `docs/domain_graph_design.md`, and `[[project_domain_graph_step1_landed]]`). Rerouting
  *conditions* to raw columns must NOT weaken the domain edges used for planning/narrowing —
  only the rendered operand changes, like the probe does today.
- **Which side is "raw"?** For `union` both sides coalesce symmetrically; for `subset` the render
  is directional. Confirm the per-side raw column is always recoverable pre-merge for both.
- **Anchor vs non-anchor.** q59's condition targets the NON-anchor (`next_year`) side; the probe
  must survive the row-preserving join and be NULL where that side is absent (the existing probe
  does this for full/union — verify for subset).
- **Equality semantics.** `next_year.store_code = 'X'` on a raw side column is unambiguous
  (filter next_year rows), but interacts with the join ON clause if the same key drives the join.
  Scope carefully: start with null-tests on subset + property (unblocks q59), defer general `=`.

## Recommended spike steps

1. Reproduce the trigger matrix above; confirm subset joins coalesce the key and that a per-side
   raw column exists pre-merge for the subset case.
2. Extend eligibility in `_coalescing_presence_probe`: (a) include subset/directional coalescing
   members (add a `domain_graph` accessor for subset-coalesced key members, parallel to
   `coalescing_relation_members`); (b) when the ref is a PROPERTY whose key is a coalescing
   member, probe via the key's side. Keep the "project stays coalesced" invariant.
3. Guard with tests mirroring the matrix (union/subset × key/property × null-test), plus q59 and
   q84 end-to-end (byte-match reference). Reuse `tests/join_matrix/` (canonical join oracle,
   `[[project_join_test_matrix]]`).
4. Only after 1–3, evaluate promoting to general side-qualified equality (gap 3) — likely a
   separate, larger change; document the decision.

## Payoff
Makes the natural `where <side>.<key> is not null` express intersection for subset/union joins
(agents reach for this constantly — q59, q84, q29-adjacent), removing a top recurring
agent-skill failure on the enriched leg without weakening the domain-merge model. Also lets the
guidance say "to intersect, filter `<side>.<joinkey> is not null`" (intuitive) instead of the
current non-obvious "filter a non-key attribute of the anchor side."

## Pointers
- `trilogy/core/models/build.py:3344` `_coalescing_presence_probe`; `:3383` `_build_comparison`.
- `trilogy/core/domain_graph.py:292` `coalescing_relation_members` (the eligibility set to widen).
- `trilogy/core/processing/node_generators/rowset_node.py:212` `_is_presence_probe` passthrough.
- `trilogy/constants.py:18` `PRESENCE_PROBE_PREFIX` doc.
- Prior art: `[[project_q97_coalescing_presence_and_derived_key_recursion_fixed]]` (where the
  probe was introduced), `docs/subset_union_join_design.md`, `docs/domain_graph_design.md`.
- Failing query: `evals/tpcds_agent/results/20260707-151529/workspace/query59.preql`; canonical
  `tests/modeling/tpc_ds_duckdb/query59.preql` (uses the anchor-non-key-attr workaround).
