# Handoff: a region's rows below an arbitrary aggregate

Branch `extension-row-null-semantics` (PR #702). The two shapes this handoff was
opened for are fixed, and so are seven of the eight items the first pickup
left open (bef77f16c and the commit after it); what follows is the rule that
fixed the originals, the rules that fixed the rest, what is still open, and a
design note on ROLLUP.

Every item below was also run against `origin/main` (a detached worktree of
it, probed with this branch's test models): items 4 and the `count(customer_id)
by status` output were REGRESSIONS of this branch; everything else was wrong
on main too.

Models: `_DERIVED` / `_MATERIALIZED` in `tests/engine/test_derived_key_domain.py`
(customers 1 ann, 2 bob, 3 cat; orders 100 cust1 delivered, 101 cust1
undelivered, 102 cust2 delivered; `orders.customer_id` is `~`). `status` is a
CASE over order columns on `_DERIVED` and a stored column on `_MATERIALIZED`, so
the two must agree. The twin is blind wherever both models pad alike: hand-check
rows before asserting.

## What was wrong

A demanded extension region fell back to the padded plan (`DomainKind.PADDED`)
in two cases, and a derivation over the fact was then evaluated on the
orderless customer's padded row:

- a ROLLUP over a key the region carries (`note="rollup key"`);
- a statement-wide gate the region's rows feed (`count(customer_id) by * > 2`).

## The rule now

The padded plan is right exactly while nothing takes a value on a padded row,
and it is the smaller plan, so both cases keep it until something does
(`_named_value_on_padding` / `_inline_value_on_padding` in `region_domains.py`).
When something does, the region gets its own domain and:

- **ROLLUP.** The domain feeds the pass below it, on the pass's input
  (`evaluated_over_region(one_pass=True)`); its named BASIC arguments are
  projected on the solid side first (`_project_basic_aggregate_inputs`). Nothing
  above the pass is a region reader: a group reading the pass is not solid
  (`rows_above_a_rollup`), so `coalesce(sum(qty), 0)` over the pass no longer
  keeps the pass off the domain, and regraft then hands a renamed key back to
  the pass instead of the domain. A pass grouped by a key *absent* on the region
  takes the region's rows when one member counts them (`count(customer_id) by
  rollup (status)`); the `grouping()` flag is not an aggregate over rows. A
  per-member atom (`count(order_id) by customer_id < 2`) is hosted on the pass's
  input, since FINAL would test the subtotal rows.
- **`by *` gate.** `keyless` no longer excludes a gate the region feeds. The
  gate joins the *united* rows of an aggregate's input, not the solid stream
  (it was NULL on the padded row there), and a WHERE aggregate reading only
  what the domain holds reads the domain alone (`_counts_the_domain`): its
  shared condition scan, joined to a fact, holds only the customers an order
  references.
- **Extent election** (padded plans). A scan the WHERE reads for itself loses a
  tie to the statement's row stream, and a group holding the span as a member
  stands for election even when it does not output it. Both were INNER-joining
  the fact under the padded plan (`select name, status where count(customer_id)
  by * > 2` on the materialized model).

TPC-H q22 keeps its plan (nothing there takes a value on padding); the corpus
A/B moved 0 of 200 statements.

## Fixed in the second pickup (2026-10-02, bef77f16c)

Numbered as the first pickup left them.

1. **Inline argument under a ROLLUP.** Where a region domain feeds a pass, the
   strategy builder stands a concept in for an inline function argument that
   takes a value on a padded row (`_name_inline_arguments`) and
   `_project_basic_aggregate_inputs` computes it on the solid rows first, as
   it does a named argument. Whether an argument takes a value is the
   keyspace's test at any depth of its lineage (`_takes_a_value_beside`:
   `1 + coalesce(amount, 0)` does; the old top-operator check said no), and
   `region_reads.nameable` says which inline arguments can be named (a
   function; anything else still keeps the padded plan). `array_agg` keeps
   nothing solid any more: every dialect drops its NULL elements, so the
   `NULL_COLLECTING_AGGREGATES` carve-out was stale, and under a pass it
   forced the padded plan that evaluated `status` on the padded row. A
   build-time version of the same hoist (naming the argument in the Factory
   under any pass) was tried first and backed out: it split ROLLUP passes over
   `union join`-ed rowsets, and moved q67/q80's SQL by an alias name.
3. **Two facts under one ROLLUP** (counts only). One pass reads one row stream,
   so the facts are joined below it and each key repeats per row of the other.
   A count of a key renders `COUNT(DISTINCT ...)` when the pass's stream is
   finer than the key: a grouping key the counted key does not determine, or a
   sibling's finer or foreign input (`ConceptAttrs.counted_key`,
   `_partition_grouped_aggregates`). Not fixed for other aggregates: see open
   item C.
4. **`where name = 'cat'`.** Both FINAL sides had applied the WHERE, and the
   solid `sum(amount)` side was taken for the statement's population, so the
   side holding the region's rows was INNER-joined to it. A side is not the
   population against a partner that applied the WHERE itself and holds a
   region's rows the side lacks (`grain_utility._is_filter_population`).
5. **`by *` aggregate as an OUTPUT.** As the author note below suspected, the
   aggregate needed no new modelling: the OTHER fields were the problem. The
   root feeding it (`customer_id`) was sourced apart from the order columns
   because an aggregate read it, although it is itself a row of the statement
   (`group_rules._is_row_stream_output`); and in the renamed spelling
   (`customer_id as c2`) the row stream reading the domain advertised no
   grain at FINAL, so the solid stream was projected off the span
   (`_group_final_grain_contribution` adds the domain's spans a reader
   carries).
6. **Region demanded only by a WHERE aggregate.** Decided: a WHERE filters the
   statement's rows and never adds one, so `_region_is_demanded` ignores
   condition-phase aggregates and the orderless customer is a row of `select
   status, count(order_id)` on neither model (`select order_id, status where
   count(customer_id) by customer_id > 0` had a padded `(None, 'in-transit')`
   row on `_DERIVED`).
7. **`count(<key>)` on subtotal rows.** The same `COUNT(DISTINCT)` rule as item
   3; it was wrong at the finest level too (`count(customer_id) by rollup
   (has_amt)` counted order rows).
8. **Bare skips.** Both `continue`s record the group as unbuilt, so
   `_raise_if_unbuilt_group_owed` judges them; and `_raise_if_output_unrendered`
   fails a plan whose FINAL renders no column for a requested concept (the
   sole-contributor path returned a narrower result silently).

Found and fixed beside them:

- `select customer_id, status, count(customer_id) by status as per_status`
  (an aggregate over the region grouped by a key ABSENT on it, as an output
  beside the rows) gave `(3, None, 0)` and `(None, None, 1)`: the region's
  padded row and the aggregate's NULL group, which IS the region's rows, did
  not pair. `join_resolution._pairs_region_padding`: a key NULL on rows an
  earlier join of the merge padded for a region pairs null-safely with a side
  that holds the region and is NULL on the key. A regression of this branch
  on the materialized model.
- `select status as s2, sum(amount) as a, sum(amount_or_zero) as t by rollup
  (status)` rendered `quizzical.s2` beside `GROUP BY ROLLUP(status)` and failed
  to bind (on main too): the collapse of a BASIC rename into its GROUP parent
  rebound the column to the same-address column computed BELOW the pass.
  `apply_child_merge` keeps a folded column rendering from its lineage.
- `select customer_id, sum(double_amount) as t, count(status) as n` (a named
  arithmetic argument beside a projected CASE) returned TWO columns: the
  projection kept only the direct arguments, `double_amount`'s inputs were
  dropped, and `satisfiable_outputs` pruned `t` without a word. The inlined
  argument's row inputs are kept, and the prune is no longer silent (item 8).
- A second fact binding the span (`returns ~customer_id` beside `orders`)
  cross-joined `select customer_id, status, sum(amount) by status`: the
  concept-graph FD no longer said the order determines its customer (the
  span's keys name every FK path), so the row stream stopped carrying the span
  and FINAL paired nothing. A pointwise child of a scan carries a region's span
  the model's FD says its fact determines (`_compute_concept_sets`).

## Still open

A. (was 2) **A value-NULL key absent on the region, beside an aggregate the
   region does not feed.** `pstatus <- case when amount > 15 then 'big' end`.
   `select customer_id, pstatus, sum(amount) by pstatus as s` gives `(3, None,
   10)`: the padded row's NULL `pstatus` pairs null-safely with the real NULL
   group (want `(3, None, None)`: the key is absent, so the aggregate is).
   `select customer_id, pstatus where coalesce(sum(amount) by pstatus, 0) = 0`
   drops cat for the same reason (main raises `Missing source map entry` on
   it). The null-safe pairing is right for the solid rows, so the fix is join
   ORDER, not modifiers: sides that hold no region pair before the region's
   rows unite with them (`resolve_join_order_v2` pivots a held span first
   today; a value-nullable key whose sides are all solid should pivot before
   it). The WHERE spelling joins the aggregate as a feeder ABOVE the united
   rows (`_apply_final_conditions`), so it needs the same below FINAL.
B. **The WHERE spelling of a region-fed aggregate by an absent key.** `select
   customer_id, status where count(customer_id) by status = 1` returns
   `[(1, 'in-transit')]` and drops cat, whose NULL-status group counts 1 (the
   output spelling above is right). Same on main.
C. **Other aggregates under a fanned-out pass.** With two returns for ann,
   `select customer_id, sum(amount) as a, count(return_id) as r by rollup
   (customer_id)` gives `(1, 60, 2)`: the sum doubles over the join below the
   pass (the plain `select` gives 30). `sum(<customer-grain property>) by
   rollup (name, status)` doubles the same way on subtotal rows. Needs a pass
   per input grain joined on the pass's row identity (keys plus grouping
   flags), or the rowset-body spelling below. Same on main.

Fixed in the first pickup and no longer open: the `avg(customer_id) by *` +
`status is null` shape, and padding nobody claims when the region key is not
selected.

## Design note: ROLLUP as a function over a relation

Every fix above is a carve-out for one fact: the same address (`customer_id`)
means a row key below the pass and a nullable subtotal label above it, and the
keyspace's questions (defined on, carried on, who reads the domain) only make
sense below. About 80 sites already special-case this (`nulls_grouping_keys`,
`rollup_padded_keys`, `nonstandard_grouping_lineage`).

Spelling the pass's input as an explicit rowset already gives the right rows,
at baseline, with no ROLLUP-specific region logic:

```
with b as select customer_id, order_id, status;
select b.customer_id, count(b.status) as n by rollup (b.customer_id);
```

The body is a row-level statement, so the region's rows unite with the solid
ones at the body's FINAL, which is the one place the planner already joins a
domain back; above the boundary the pass reads handles keyed on the rowset and
no region exists. That spelling is right for every shape in this handoff, and
was for items 1, 4 and 5 before they were fixed directly; it renders the same
SQL the fix above produces for `count(status) by rollup (customer_id)`.

So modelling ROLLUP (and a `by *` aggregate) as a function over a body relation
with outputs of its own would retire these carve-outs rather than add to them.
It does not need new syntax: `by rollup` can desugar to the body at plan time.

AUTHOR NOTE: suspicious that by * aggregate needs to be a function? that is a function - a scalar 
without any row association - and so ti's probably the OTHER fields in discovery that are the problem

(Confirmed in the second pickup: item 5 was the root partition sourcing the
aggregate's input apart from the row stream, and a renamed reader of the
domain advertising no grain. The `by *` aggregate itself is left as it was: a
single row broadcast onto every output row.)

What it does not buy:

- plan size: the body takes an OWN domain where the padded plan is smaller
  (the partial-date fixture goes 4 -> 6 CTEs), so the "padded while nothing
  takes a value" economy has to move into the boundary decision
  (`DomainKind.BOUNDARY` / `_needs_solid_rows` is that rule for rowsets);
- open item C: two facts cannot share one body without fanning out; a key
  counted on a subtotal row is handled by COUNT(DISTINCT) now, any other
  aggregate needs a pass per input grain joined on the pass's row identity
  (keys plus grouping flags).

## Do not

- Guard on a key's value (`CASE WHEN key IS NULL THEN NULL ...`): `?` keys are
  NULL on real rows, ROLLUP grand-total rows have NULL keys, and a property is
  never a witness.
- Invent a new presence marker; the keyspace has what is needed.

## Working notes

- Run tests one directory at a time; the machine is memory-tight. A full
  local suite is ~25 minutes alone and twice that with probes running beside
  it.
- Verify by rows on both models, then a corpus SQL A/B
  (`local_scripts/sql_ab/README.md`). A probe script run from a worktree must
  put cwd first on `sys.path`, or it imports the main tree's `trilogy`.
- Classify a bug before sizing it: a detached worktree of `origin/main`,
  probed with the branch's test models (`importlib` the branch's test module
  by path), says whether it is this branch's regression or main's.
- A passing modeling run rewrites `tests/modeling/**/zquery<N>.log` to the
  new plan; after backing a change out, restore the log from HEAD or the
  budget check fails against your own smaller plan.
