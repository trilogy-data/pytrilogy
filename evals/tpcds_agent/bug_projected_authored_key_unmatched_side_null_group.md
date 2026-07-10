# Projected authored key: unmatched side's rows lose their own id (NULL group)

Status: OPEN (pinned strict-xfail in
`tests/join_matrix/test_property_hop_alignment_matrix.py::test_member_projected_two_side_rollup`)

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

## Observed

b rows unmatched on the composite pairing render with NULL id and aggregate
into a spurious `(None, None, …)` group; their own dimension's id is lost:

```
got:      [('C1',3,100), ('C2',4,400), ('C3',8,None), ('C4',None,None), (None,None,1000)]
expected: [('C1',3,100), ('C2',4,600), ('C3',8,None), ('C4',None,800)]
```

(b2=(C2, sku200, 200) and b4=(C4, sku200, 800) fall into the NULL group.)

## Notes

- Same family as the q97 per-side coalescing presence probes / q84 ROOT-member
  presence probes: the side-local id must survive for unmatched rows.
- NOT a regression from the authored-join discovery injection work (2026-07-10);
  the id pairing itself is enforced — only the unmatched-row projection
  coalesce is wrong. Without the shared `sku` the cell passes.
- Discovery/ordering context: authored group keys now pivot the join tree
  first (`resolve_join_order_v2` authored_key_nodes) and are injected as
  mandatory discovery terminals (`inject_authored_join_key_terminals`).
