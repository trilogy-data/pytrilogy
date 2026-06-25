# Bug: ORDER BY `grouping(x)` rejected though `x` (and `grouping(x)`) is a projected output (q05)

**Status:** OPEN — confirmed by code reading; surfaced live in q05 enriched eval thrash.
**Surfaced by:** TPC-DS q05 enriched eval, agent message ~86.
**Severity:** Medium (usability / token sink). Emits a *misleading* syntax error that tells
the agent to do the thing it already did, causing repeated rollup-ordering thrash. Not a
crash and not a wrong answer — but it blocks the canonical rollup-subtotal ordering idiom.

## Symptom

This query (rollup over `eff_channel`, `entity_id`; the dimension is projected under an
alias and the `grouping()` levels are projected as hidden `--` outputs):

```trilogy
select
    eff_channel as channel_type,
    entity_id,
    coalesce(gross_sales, 0) as total_gross_sales,
    coalesce(returns, 0) as total_returns,
    coalesce(sales_net_profit, 0) - coalesce(ret_net_loss, 0) as net_profit,
    --grouping(eff_channel) as g_chan,
    --grouping(entity_id) as g_ent
order by
    grouping(eff_channel) asc,
    eff_channel asc nulls last,
    grouping(entity_id) asc,
    entity_id asc nulls last
limit 100;
```

fails with:

```
Syntax error in query05.preql: ORDER BY contains aggregate `grouping(local.channel_type)`
(line 51), which is not a SELECT output. Aggregates cannot be computed in the ordering
scope; add it to SELECT — prefix with `--` to keep it out of the output rows — and order
by the alias, e.g. `select ..., --grouping(local.channel_type) as g order by g desc`.
```

The hint is wrong on two counts: (1) `grouping(eff_channel)` **is** a SELECT output —
projected hidden as `g_chan` (and its argument `channel_type` is projected too); (2) it tells
the agent to add `--grouping(...) as g`, which is exactly what's already there. The agent
followed this hint in a loop.

## Root cause

`_fix_projection_grouping_mode` (`trilogy/parsing/v2/select_finalize.py:788`) and
`_validate_order_by_aggregates` (`:818`) run in this order inside
`finalize_select_statement`:

```
1058  _fix_projection_grouping_mode(select, context)   # mutates SELECT grouping wrappers
1059  _validate_syntax(select, context)                # -> _validate_order_by_aggregates (:946)
```

`_fix_projection_grouping_mode` aligns every `grouping()` **reached from `select.selection`**
to the query's rollup spec — it mutates each wrapper's `.by`, `.grouping`, `.grouping_sets`
in place (`:812-815`). It **never touches ORDER BY** grouping wrappers.

Validation then matches by `_aggregate_full_signature` (`:249`), whose third component is
`tuple(sorted(by-addresses))`:

- Projected `grouping(eff_channel) as g_chan` → `(GROUPING, ('local.channel_type',), <full rollup by-tuple>)` (mode-aligned)
- ORDER BY `grouping(eff_channel)` → `(GROUPING, ('local.channel_type',), ())` (NOT aligned)

The `by` tuples differ, so `sig_to_alias.get(sig)` (`:836`) returns `None`, and the validator
takes the "not in SELECT" branch with the misleading hint — even though the two expressions
are semantically identical.

### Second, independent defect

Even when the signatures *do* match (`alias is not None`, e.g. a non-grouping aggregate that
happens to be mode-stable), `_validate_order_by_aggregates` **still raises unconditionally**
(`:847-851`) — the `alias is not None` branch only swaps in a nicer hint, then falls through
to the same `raise`. An ORDER BY aggregate that is byte-identical to a projected output is
never allowed; the user must manually rewrite it to the alias. For `grouping()` over a
projected grouping key — the standard `ORDER BY grouping(a), a, grouping(b), b` rollup idiom
(matches the reference `query05.sql`) — this should just resolve.

## Expected

`order by grouping(eff_channel)` should resolve when `grouping(eff_channel)` is a projected
output (hidden or not) and/or `eff_channel`/`channel_type` is a projected grouping key. These
are identical expressions; the ordering scope can compute them in the rollup grain.

## Suggested fix direction

Both halves want fixing:

1. **Normalize the match.** Before signature comparison, apply the same rollup-mode alignment
   to ORDER BY (and HAVING) `grouping()`/`grouping_id()` wrappers that
   `_fix_projection_grouping_mode` applies to projections — or compare grouping() on a
   mode-insensitive signature (operator + args only). Then ORDER BY `grouping(eff_channel)`
   matches its projected twin `g_chan`.
2. **Resolve instead of raise.** In `_validate_order_by_aggregates`, when the ORDER BY
   aggregate matches a projected output (`alias is not None`), rewrite the ORDER BY item to
   reference that alias and **do not raise**. Reserve the error for genuinely unprojected
   aggregates.
3. Optionally, accept `grouping(x)`/`grouping_id(x)` in ORDER BY whenever every argument
   resolves to a projected grouping key, aligning its mode like the projection path — this
   makes the canonical rollup-ordering idiom work without the `--grouping(...) as g` dance at all.

Note: `tests/engine/test_duckdb.py:84` currently *asserts* `order by grouping(brand) desc`
raises `"ORDER BY contains aggregate"`. That test encodes the current (wrong-for-projected-
grouping) behavior and will need revisiting — it orders by `grouping(brand)` where `brand`
is projected, which is exactly the idiom this bug is about.

## Relation to prior notes

- `evals/tpcds_agent/q05_union_measure_broadcast_bug.md` lists "ORDER BY contains aggregate
  not in SELECT (msg 33)" under *not-bugs / clear correct messages*. This finding
  **reclassifies** that line: the message is wrong when the grouping is projected under an alias.
- Same validator family as the B3 `grouping()`-in-ORDER-BY rollup fix
  (`project_b3_grouping_in_orderby_rollup`) and the HAVING dim/window validator-bypass walker
  (`project_having_dim_window_validator_bypass_bug`, `_row_grain_arguments` in author.py) —
  the mode/grain normalization between projection and ordering/HAVING scopes is the recurring
  weak spot.
```
