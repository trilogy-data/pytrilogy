# Handoff — q54: `subset join` with a ROWSET superset anchor renders FULL+coalesce (non-members leak)

**Verification:** ⚠️ SUBAGENT-REPORTED — CONFIRM the repro before fixing. Detail report:
`bug_q54_resolve_connections_sink_20260706.md`.

## Confirmed-by-subagent bug
`subset join a = b`, where the superset anchor `b` is a ROWSET-derived key, renders as a symmetric
**FULL JOIN + coalesce** instead of the directional **LEFT OUTER JOIN** it correctly emits when `b` is
a plain datasource. Result: rows NOT in the declared superset leak into the output. SILENT.
- q54 repro: `subset join ss.customer.id = all_qual.cid` → 2 customers incl. non-member 63889;
  `ss.customer.id in all_qual.cid` → 1 (26788). Membership check confirms 63889 ∉ `all_qual`.
- Minimal synthetic isolation: `subset join s.cust = members.mid` (members = rowset) leaks non-member;
  identical `subset join s.cust = c.ccust` (plain datasource) renders `LEFT OUTER JOIN` and excludes it.
  Rowset-vs-plain anchor is the ONLY variable that flips behavior.

## Root cause (locus)
`trilogy/core/models/build.py` `_rowset_outer_pair` (~L2400-2414): the
`LEFT_OUTER → return _is_rowset_keyed(s)` branch fires on a subset join's rowset SUPERSET anchor
(source), routing it into the coalescing rowset-outer-identity path (~L2416-2454) → FULL.
Mapping at `trilogy/parsing/v2/rules/select_statement_rules.py:270-276` (subset → LEFT_OUTER,
source = superset anchor).

## Fix direction
Do NOT promote an `authored=SUBSET` LEFT to the coalescing FULL path just because the anchor is a
rowset key — keep it anchor-preserving directional LEFT OUTER. Guard the exact condition so genuine
FULL/derived-key rowset joins are unaffected.

## Guard test
Belongs in the join-matrix oracle: `tests/join_matrix/` — add a cell for `subset join` onto a rowset
superset anchor asserting a directional LEFT (no non-member leak), paired with the plain-datasource
cell that already passes. Adversarial: a legit FULL derived-key rowset join must stay FULL.

## Priority note
Same guidance-amplified family as q87: commit `4e69c5547` expanded `subset/union join` guidance, so
this correctness hole will bite more often.
