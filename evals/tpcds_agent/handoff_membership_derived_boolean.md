# Handoff: cross-model `in` membership fails when wrapped in a derived boolean

> **FIXED 2026-06-12.** Two-layer fix: (1) discovery — `gen_basic_node`
> (`basic_node.py`) now splits a `BuildSubselectComparison` lineage's
> `existence_arguments` (the RHS) out of the row parents and wires them as an
> existence parent + `add_existence_concepts`, mirroring `append_existence_check`
> (the inline-WHERE path); (2) render — `_render_concept_sql` (`dialect/base.py`)
> gained a `SUBSELECT_COMPARISON_ITEMS` branch *before* the generic
> `BuildComparison` branch so a projected/named membership renders its RHS as an
> existence subquery, not a bare column ref. All four rows of the table below
> now pass (+ projected boolean). Test: `test_derived_membership_existence` in
> `tests/engine/test_duckdb.py`.

## Summary

A cross-model membership (`a.key in b.key`) resolves as an existence semi-join
**only when it is a top-level `WHERE` atom**. The moment the same membership
becomes a *derived boolean concept* — stored in an `auto`, or projected in the
`SELECT` — discovery loses the existence semantics, treats the two models as
unconnected, and dead-ends with `UnresolvableQueryException: Could not resolve
connections for query with output [...] from current model.`

This is the real blocker behind several q64 agent attempts (not the demographic
join / not a grain issue — both were red herrings during triage). Agents
naturally write `auto has_store_return <- store_sales.item.id in
store_returns.item.id; ... where has_store_return`, which is exactly the broken
shape.

## Minimal reproduction (unit-testable, no TPC-DS)

```trilogy
key sale_item int;
property sale_item.amt float;
datasource sales (id: sale_item, amt: amt) grain (sale_item)
query '''select 1 id, 10.0 amt union all select 2 id, 20.0 amt''';

key ret_item int;
property ret_item.refund float;
datasource returns (id: ret_item, refund: refund) grain (ret_item)
query '''select 1 id, 5.0 refund''';
```

| query | result |
|---|---|
| `select amt where sale_item in ret_item;` | **OK** (inline WHERE — existence join wired) |
| `select amt where sale_item not in ret_item;` | **OK** (anti-join wired) |
| `auto has_return <- sale_item in ret_item; select amt where has_return;` | **FAILS** |
| `select amt, sale_item in ret_item as has_return;` | **FAILS** |

Error in the failing cases:
`Could not resolve connections for query with output ['local.amt<...>'] from current model.`

So the boundary is precise: **inline top-level membership works; a *named/derived*
membership boolean does not.**

## Expected behaviour

`key in/not in other_model.key` is the documented existence/anti-join idiom for
relating otherwise-unconnected models (see memory `reference_trilogy_existence_idiom`).
A derived boolean over that membership should carry the same existence semantics —
`auto flag <- a.key in b.key; where flag` should plan identically to
`where a.key in b.key`. Projecting the boolean (`select ... as flag`) should
likewise resolve (flag computed via the existence join).

## What this is NOT

- **Not** a true disconnection. The user expressed the relationship via `in`; the
  models are related. The disconnected-components error (and its "are you missing
  a join or merge?" message) must **not** fire here — that would be actively
  misleading. Confirmed: the failing cases currently raise the *bare*
  `UnresolvableQueryException`, not `DisconnectedConceptsException`, so we are not
  (yet) mislabeling it — but any future tightening of the disconnected detector
  must keep it that way.
- **Not** a grain/aggregation issue.
- **Not** specific to the catalog aggregate set — the bare two-model membership
  reproduces it.

## Known-related context

- Memory `project_membership_in_having_unsupported`: "`x in feeder` as a
  projected/HAVING boolean collapses/errors; only top-level WHERE works.
  Const-fold+render layer fixed 2026-06-02; **output-driven existence wiring
  deferred.**" — this handoff is that deferred work surfacing again via `auto`
  (not just HAVING/projection).
- Memory `reference_trilogy_existence_idiom` and the dual-existence pushdown docs
  (`project_dual_existence_pushdown_cycle`) describe how inline existence is wired
  in `predicate_pushdown` — that is the machinery the derived-boolean path needs
  to reach but currently doesn't.

## Where to look

- How inline top-level `WHERE a in b` becomes an existence subquery: search the
  existence-argument handling in discovery — `append_existence_check` in
  `trilogy/core/processing/concept_strategies_v3.py`, and `existence_arguments` /
  `BuildWhereClause` plumbing. The inline path populates existence feeders; the
  derived-concept path does not.
- The membership concept's derivation: a bare `a in b` is a `Derivation.FILTER`
  comparison concept. When it is the WHERE conditional directly, its
  `existence_arguments` are extracted; when it is wrapped in an `auto` (so it
  becomes a *referenced* concept that must be *sourced* like any other output),
  discovery tries to source `b.key` co-grained with `a.key` and finds no join —
  hence the unconnected dead-end.
- Likely fix direction: when sourcing a derived concept whose lineage is a
  cross-model membership, route it through the same existence-subquery wiring the
  inline WHERE path uses, rather than demanding a join path between the two
  models. I.e. lift the membership's RHS into an existence feeder at the point the
  derived boolean is built, mirroring `append_existence_check`.

## Suggested first step

Add the four-row table above as a parametrized test (xfail the two failing rows),
then trace `auto_flag_then_where` vs `inline_membership_where` through discovery
to see exactly where the existence_arguments are dropped for the derived form.
The two queries differ only in whether the membership is named, so a side-by-side
trace isolates the divergence quickly.
