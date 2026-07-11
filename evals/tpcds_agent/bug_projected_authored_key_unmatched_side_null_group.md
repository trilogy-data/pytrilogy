# Projected authored key: unmatched side's rows lose their own id (NULL group)

Status: FIXED 2026-07-10 (pin:
`tests/join_matrix/test_property_hop_alignment_matrix.py::test_member_projected_two_side_rollup`,
gate unit `tests/discovery/test_authored_join_terminal_injection.py::test_member_projected_still_pins_hops`)

## Shape

Two facts, each with a local FK to its own scan of a shared dimension; facts
ALSO share a directly-bound key (`sku`). Authored relation on the dimension
business id, id PROJECTED:

```
select
    a_cust_id,
    sum(a_amount) as a_total,
    sum(b_amount) as b_total
union join a_cust_id = b_cust_id
;
```

Row universe pairs on (id, sku). A b-side row with no (id, sku)-matching a-side
row must still land in ITS OWN id's group — the merged output id is
`coalesce(a_side.id, b_side.id)` and the b side carries the value.

## Observed (before fix)

b rows unmatched on the composite pairing render with NULL id and aggregate
into a spurious `(None, None, …)` group; their own dimension's id is lost:

```
got:      [('C1',3,100), ('C2',4,400), ('C3',8,None), ('C4',None,None), (None,None,1000)]
expected: [('C1',3,100), ('C2',4,600), ('C3',8,None), ('C4',None,800)]
```

(b2=(C2, sku200, 200) and b4=(C4, sku200, 800) fall into the NULL group.)

## Root cause (two stacked defects)

1. **Hop-needed gate false negative (regression to Ambiguous error).** Projecting
   the merged id pulls both dimension scans into the request's datasource set,
   so `_relevant_authored_join_pairs` saw every member "directly bound" and
   skipped injection entirely — discovery then had two live paths
   (`a_cust_sk` vs `sku`) and raised
   `AmbiguousRelationshipResolutionException`. Fix: a member bound only on its
   dim scan does NOT settle the relation when a request datasource is an FK
   carrier lacking the member's column (`_member_needs_fk_hop` in
   `node_generators/common.py`). The q2 date-spine natural-join skip is
   unaffected (its members are keys with no hop).

2. **Redundant datasource re-pairs the preserving join (the NULL group).** With
   injection firing, the select path kept `a_facts` in the b-side aggregate's
   parent even though its only relevant binding (`a_cust_sk`) is a strict
   subset of `a_customers`' bindings. `reinject_common_join_keys` then added
   the UNREQUESTED `sku` edge between the facts, and the greedy join chain
   conjoined the b-fact's own-dimension FK edge with the cross-side `sku`
   probe in one FULL-join ON — a b row failing the probe also lost its own
   dim, so its id nulled. (The a-side parent only survived by alphabetical
   join-order luck: its fact attached before the probe.) Fix:
   `prune_dominated_datasources` in
   `node_generators/select_helpers/source_scoring.py` — a datasource whose
   relevant bindings are a strict subset of a single peer's (no partial
   advantage) contributes no requested content and only adds spurious pairing
   paths; dropped unless it is the connectivity bridge for the request.

## Notes

- Same family as the q97 per-side coalescing presence probes / q84 ROOT-member
  presence probes: the side-local id must survive for unmatched rows.
- Post-fix plan matches the healthy no-`sku` shape: each fact attaches to its
  own dimension only; dims FULL-join on the id; per-side rollups stitch on id.
