# `then where`: staged filters

## The problem

A flat `WHERE` deliberately does not let its conjuncts filter each other. Given

```sql
where f = 1 and sum(z) by x > 5 select x, sum(z) as v;
```

`sum(z) by x` is computed over *every* row, not over the `f = 1` rows — the two
conjuncts are siblings, and the aggregate gets a pristine scan. That is the
right default (it is what makes a WHERE aggregate a pre-aggregation gate rather
than a HAVING), but it is not always what you want. The inline-filter spelling
says the other thing:

```sql
where f = 1 and sum(z ? f = 1) by x > 5 select x, sum(z) as v;
```

which duplicates `f = 1` once per aggregate and gets unreadable fast.

## The syntax

`then where` chains filter *stages*:

```sql
where f = 1
then where sum(z) by x > 5
select x, sum(z) as v;
```

Two rules define it:

1. **The row gate is the AND of every stage.** A row must pass all of them.
2. **Stage N's aggregates and windows compute over only the rows passing stages
   1..N-1.** Stage 1 sees the full population.

So the query above is exactly equivalent to the inline-filter spelling, and
`tests/test_then_where_execution.py` asserts that pairing shape by shape.
Chains can be any length; each stage sees the ones before it:

```sql
where f = 1
then where z > 1
then where sum(z) by x > 5      -- computed over rows with f = 1 AND z > 1
select x, sum(z) as v;
```

A stage with no cross-row computation is just an ordinary conjunct — nothing is
delivered anywhere for it, and a flat `where` is simply a one-stage chain.

## What an earlier stage may contain

An earlier stage's condition reaches a later stage's aggregate by riding that
computation's input scan, and the planner can only do that for a plain scalar
row predicate. Two kinds of earlier predicate are rejected at parse time when a
*later* stage computes across rows:

| Earlier stage contains | Why it cannot travel |
| --- | --- |
| an aggregate or window | delivering an aggregate gate into another computation's input rows needs a feeder join we do not build |
| `x in (select ...)` | the input scan cannot re-plan the subquery |

Both are errors rather than silent drops: dropping one returns flat-`WHERE`
rows for a query that asked for staged semantics, which is a wrong answer with
no signal. Write the earlier condition as an inline filter (`sum(x ? cond)`) or
flatten the stages instead. Neither restriction applies when no later stage
computes across rows — there the predicate is only a conjunct of the row gate,
so `where sum(z) by x > 5 then where f = 1` and a trailing `x in (select ...)`
stage are both fine. A literal membership (`x in (1, 2)`) is a plain row filter
and is never restricted.

`then where` cannot be combined with a second positional `where` slot: the two
pre-`select` and post-select-list slots AND into one stage, which cannot express
stage ordering. A scoped join written before `select` also separates the stages,
so put `subset join` after the select list when using a chain.

## How it is implemented

### Parse

`where_series` in both grammars (`trilogy/parsing/trilogy.lark` and
`trilogy/scripts/dependency/src/trilogy.pest`) collects `where (then where)*`
into a `SelectStatement.where_clauses` list. `where_clause` is set to the AND
fold of that list and stays the canonical row gate everything plans against.

The pair is only meaningful together, so `__post_init__` re-derives it through
`normalize_where_stages`: stages that no longer fold to the gate collapse back
to the flat single stage it is. That makes `dataclasses.replace(stmt,
where_clause=...)` — which copies the old stage list verbatim — safe by
construction. Code that means to widen the gate *and* keep the staging uses
`prepend_where_stage`, which ANDs into stage 1 (narrowing stage 1 narrows every
later stage's input population, which is what a statement-wide gate means).

`_validate_staged_where` in `parsing/v2/select_finalize.py` enforces the table
above. It runs from `_validate_syntax`, so it covers rowset bodies and
multiselect arms as well as plain selects.

### Build

`Factory.build` builds the stages with the **same** `where_factory` as the
combined clause, immediately after it. The shared caches make every stage
expression resolve to the exact `BuildConcept` address it has in the combined
clause, which the delivery pass depends on to match atoms and hosts.
`BuildSelectLineage.where_clauses` is populated only for a chain that actually
stages something (2+), so downstream code tests it for truth rather than
re-deriving the length rule.

### Discovery

Two sites act on the stages, sharing their matching rules through
`processing/v4_helper/staged_where.py`:

- **`condition_placement._staged_precondition_placements`** delivers earlier
  stages' atoms onto the groups computing a later stage's aggregates and
  windows — preferring the computation's `ROOT_D1` feeder scans, which re-plan
  from datasources and so can source columns the host does not yet carry (and
  can admit a `complete where`-matching partial datasource). A host with no such
  feeder takes the atom directly and the strategy builder's pre-filter peel
  applies it below the computation. Placements are tagged
  `PlacementReason.STAGE_PRECONDITION` so phase coloring is left to the atom's
  own `UPSTREAM_MOST` placement; this is an input filter on a d1 side channel,
  not a gate host.
- **`root._staged_precondition_clauses`** re-applies the same bound when ROOT
  re-sources such a computation standalone. That copy plans in a sub-search
  where the host is the search output itself, outside the delivery pass's D1
  reach, so the bound has to ride the sub-search's own WHERE — without it the
  re-sourced copy computes over unfiltered rows and silently replaces the
  bounded one.

`hosting_stage_index` is the subtle part. It matches a cross-row arg's own
address against each stage's lineage expansion, never the reverse. Asking
instead whether a stage mentions anything in the arg's lineage matches any
stage reading a column that merely *feeds* the arg, so in

```sql
where f = 1 then where sum(z) by x > 5 then where z > 1
```

the trailing scalar stage would answer for `sum(z) by x` and drag that
aggregate's own gate into the conditions used to re-source it.

The lineage expansion exists for the other direction: a stage can reference its
computation through a scalar wrapper (`1.3 * avg(x) by k > 5`), and it is the
inner anonymous aggregate — not the wrapping concept — that gets bucketed and
re-sourced.

Two exclusions remain in the delivery pass, both because the atom does not mean
what it says at the host's input: probe and scoped-join-axis atoms read an axis
that only exists post-merge, and an atom over the host's **own** output is a
gate that becoming a pre-filter would change.

`V4History` keys staged searches separately, so a sub-search planned with stage
bounds never reuses a cache entry built without them.
