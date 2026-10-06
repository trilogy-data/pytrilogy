# Handoff: anchor-side WHERE over a `union join` to a rowset (2026-10-06)

Found while auditing `handoff_release_followups.md` before merging PR #702. The
regression on this branch is fixed. Everything below is OLDER than this branch
(origin/main `44d8513a8` behaves the same) unless noted.

## Repro fixture

`_COMPOSITE_UNION_JOIN_STDDEV_FIXTURE` (tests/engine/test_duckdb_rowset.py) plus
two rows: a 2001 sale with no return, and a return with no sale.

```python
FIX = _COMPOSITE_UNION_JOIN_STDDEV_FIXTURE.replace(
    "select 3 as i, 102 as t, 2 as s, 2 as d, 13 as q",
    "select 3 as i, 102 as t, 2 as s, 2 as d, 13 as q union all\n"
    "select 4 as i, 103 as t, 1 as s, 1 as d, 17 as q",
).replace(
    "select 1 as ri, 101 as rt, 2 as rd, 3 as rq",
    "select 1 as ri, 101 as rt, 2 as rd, 3 as rq union all\n"
    "select 5 as ri, 104 as rt, 1 as rd, 4 as rq",
)
J = "union join ticket = r_filtered.r_ticket"
```

Tickets: 100, 101 (sale + return, 2001), 102 (sale only, 2000), 103 (sale only,
2001), 104 (return only). `year` is a sale-side attribute: (item, ticket) ->
date_id -> year.

## Fixed on this branch

**C. FULL join re-admitted rows the WHERE removed** (regressed in `395bbcacf`).
`where year = 2001 select ticket, r_filtered.r_ticket, r_filtered.return_quantity`
returned return-only ticket 104. The sales group emits `ticket` relabelled as
`r_filtered.r_ticket`. Once placement read a group's emitted columns,
`_group_in_active_relation` counted that alias as an owned key, saw both sides of
the relation in one scan, and pushed the WHERE below the FULL join. A plain group
no longer owns a rowset handle (`_is_rowset_handle` in condition_placement.py).
Test: `test_union_join_anchor_where_drops_rowset_only_keys`.

## Open: wrong answers

**A. Rowset-only outputs raise.** `where year = 2001 select r_filtered.return_quantity $J`
-> `DisconnectedConceptsException: WHERE input(s) ['local.year'] cannot be related`.
Same for `select r_filtered.r_ticket`, and for `where state = 'CA' select r_filtered.ritem`.

**B. The aggregate form drops the WHERE silently.**
`where year = 2001 select count(r_filtered.return_quantity) as c $J` -> 3 (should
be 2). It takes a different placement path from A and never reaches the raise.

Semantics: without the WHERE, `select r_filtered.return_quantity $J` returns rowset
rows only (2, 3, 4), with no NULL rows for sale-only tickets. A WHERE only removes
rows, so the answer is a SEMIJOIN on the relation key: {2, 3}, count 2. The
hand-written form plans correctly today:

```
where r_filtered.r_ticket in (ticket ? year = 2001) select r_filtered.return_quantity $J;  -> 2, 3
where r_filtered.r_ticket in (ticket ? year = 2001) select count(r_filtered.return_quantity) as c $J;  -> 2
```

Proposed fix: rewrite such an atom before planning. Conditions: the statement
declares a join A = R; R is an output rowset's key; no output reads the anchor
side; and the atom's inputs reach R only through A. Rewrite to
`R in (A ? atom)`, using a tuple membership for a composite join. The rewrite has
to run before build, because B loses the WHERE before placement. A placement-time
FINAL route (an extension of `_keyed_by_output_rowset_base`) was tried and does
not work: the WHERE's only candidate host is the `dates` scan, which carries
`year` alone, so there is no key to pair on.

**Duplicate rows on an axis-only projection.**
`where year = 2001 select ticket, r_filtered.r_ticket $J` returns (100, 100)
twice. Both relation sides are projected, so `axis_only_projection`
(group_graph.py) sets `deduplicate_to_grain=False` and FINAL is built with
`whole_grain=True`. The hidden `year` column carries sale-line grain, so ticket
100's two lines both survive. The fan-out that rule protects is the sides' own
rows; a column carried only for the WHERE should not add rows. Without the WHERE
the result is correct.

**D. Count over an anchor-only key is NULL, not 0, under the WHERE.**
`where year = 2001 select ticket, count(r_filtered.return_quantity) as c $J`
gives (103, None). Without the WHERE it is (103, 0). main is worse: it returns
one row.

**Binder error: membership beside an anchor output.**
`where r_filtered.r_ticket in (ticket ? year = 2001) select r_filtered.r_ticket, ticket $J`
-> DuckDB `Values list "juicy" does not have a column named "ticket"`.

## Open: plan cost (no wrong rows)

**The union axis is kept under a WHERE that already makes it one-sided.**
`where year = 2001 select ticket, r_filtered.return_quantity $J` (and the
`state = 'CA'` and composite `and item = r_filtered.ritem` variants) plans
5 joins and ~2.6k chars of SQL. The same query without the union axis merge plans
3 joins and ~1.8k chars, with identical rows. `year` is sale-side, so every
return-only axis row has NULL year and the WHERE drops it: the FULL axis reduces
to the anchor's tickets, and a RIGHT join onto the anchor is enough.

The axis merge is the `preserve_parents` MergeNode on `r_filtered.r_ticket` (two
GroupNode parents, one per side). `_merges_coalesced_sides` correctly stops it
from being folded away. The fix belongs earlier: when a WHERE atom null-rejects
every row of one side (it reads only the other side's columns and is not an
`is null` test), build the relation as a directional pairing onto the surviving
side instead of a coalesced axis. The plan-time join typing pass already reasons
about null-rejection (`null_rejected(conditions)` in partial_bridging.py and the
5(a) typing rules); reuse that.

Validation: the probe harness compared current vs no-veto plans for 886 union join
queries; the SQL-only cases (18) are exactly this shape. A fix should shrink them
and leave the 32 row-changing cases (no WHERE) untouched.

## Open: `~` facts sharing a key

Found by the item 4 probe. When two `~` facts both bind the same key (sales and
returns both binding `date_id`), the rows of a query with no measures depend on
which fact the plan reads. `where week = 1 select order_id, customer` returns sale
orders under one plan and return orders under the other. The heal only changes
which plan is chosen. Decide whether the row universe is the union of both facts'
rows.
