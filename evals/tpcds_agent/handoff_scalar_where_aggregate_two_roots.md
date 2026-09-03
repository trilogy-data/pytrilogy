# Verdict: scalar WHERE-aggregate stays rejected; the two-root planner already shipped

**CLOSED 2026-08-20.** Do not implement the planner change this handoff originally
proposed. The feature it wanted is already reachable and already plans correctly; the
only thing the guard blocks is a *spelling* that would make one alias mean two numbers.
Action taken: the rejection message now names both readings and both working spellings.

## What the original handoff asked for

Support `select sum(cost) -> v where x > 1 and v > 1000;` (scalar / no grouping key) by
teaching the planner to build two aggregation roots (a WHERE-filtered output root and a
WHERE-*unfiltered* gate root) joined on the gate condition. It called this a "sizable
planner change" blocked on two gaps.

## Why that is no longer the right work

**Gap #1 (two roots, one filtered and one not) is SOLVED.** It was closed by later work,
not by this handoff. Any WHERE aggregate whose signature differs from the projection's
already sources its own unfiltered root. Verified against data (`x∈{1,2,3,4}`,
`cost={100,50,2000,10}`, whole table 2160, `x>1` 2060):

```
select sum(cost) -> v where x > 1 and sum(cost) by * > 2100;   -- 2060
select sum(cost) -> v where x > 1 and sum(cost) by * > 5000;   -- gate fails
auto total <- sum(cost) by *;
select sum(cost) -> v where x > 1 and total > 2100;            -- 2060
```

`> 2100` passing proves the gate reads the **unfiltered** 2160, not the filtered 2060:
exactly the semantics the handoff wanted. The generated SQL is the handoff's own target
node shape: an unfiltered gate CTE joined to a filtered output scan.

The `by *` is doing the differentiating work the handoff tried to synthesize
automatically: `_aggregate_full_signature` includes the `by` clause, so `sum(cost) by *`
is a different concept from the projection's `sum(cost)` and gets its own build path.
The user can just write it.

**So the blocked shape adds no expressiveness.** Every meaning it could carry has a
working spelling today:

| intent | spelling | gate universe |
|---|---|---|
| gate on the value being selected | `where x > 1 having v > 1000` | filtered |
| gate on the whole table | `where x > 1 and sum(cost) by * > 1000` | unfiltered |
| gate on rows passing an earlier stage | `where x > 1 then where sum(cost) > 1000` | staged |
| gate on an explicit subset | `select sum(cost ? x > 1) -> v where sum(cost) > 1000` | unfiltered |

**And its ambiguity is worst exactly here.** A WHERE aggregate is unfiltered while the
projection is filtered, so the two are always different numbers. A *grouped* select keys
them apart by its grain: `sum(cost) by g` in WHERE against `sum(cost)` in SELECT reads
as two arms of a join on `g`. A scalar select has no such key. In
`select sum(cost) -> v where x > 1 and v > 1000` the token `v` would have to name both,
with nothing structural to tell a reader which is which. Supporting it would ship the
one shape where the semantics are least legible, for zero new capability.

## Gap #2 (NULL row on gate-fail) is not a bug

The handoff wanted `[]` when the gate fails, and treated the `(None,)` row as a defect.
It is not. It is ordinary grainless-aggregate behavior:

```
select sum(cost) -> v where x > 99;   -- (None,), no aggregate gate involved
```

A grainless aggregate over zero surviving rows is one NULL row. `(None,)` on gate-fail is
consistent with that. `[]` appears only when the gated value *is* the projection
(`where grand_total > 5000 select grand_total`, `having v > 5000`), where there is no
row left to describe. Both are defensible; that they differ is more evidence the scalar
gate is a confusable shape, not a missing feature.

## What changed in the code

`scalar_where_aggregate_advice` in `trilogy/core/having_normalization.py`, used by all
three rejection sites (`parsing/v2/select_finalize.py` alias + inline guards, and the
`core/statements/author.py` legacy mirror). The old message said only "move to the HAVING
clause instead", which is actively misleading: HAVING gates the *filtered* value, the
opposite of WHERE-aggregate semantics, so a user who wanted the whole-table gate was sent
to the wrong answer. The message now names both readings and both spellings.

Pinned by `test_scalar_select_where_aggregate_still_rejected` (message offers both) and
`test_scalar_where_aggregate_error_suggestions_execute` (both suggestions run, and gate on
different universes) in `tests/test_derived_concepts.py`.

## If this is ever reopened

The bar is a use case that `by *` / a named `auto` / `then where` cannot express, not
merely that the rejected spelling is shorter. None was found.
