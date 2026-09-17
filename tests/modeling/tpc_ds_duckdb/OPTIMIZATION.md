# TPC-DS Generated SQL Optimization Notes

This document tracks optimization work that is likely to improve measured
TPC-DS execution time. It intentionally weights patterns by current timing
regressions against DuckDB's reference SQL, not by how strange the SQL looks.

Dotted variant query ids such as `97.1` are excluded from the priority ranking;
those are intentionally testing alternate shapes and are known-bad performance
cases.

TPC-H and thelook are not listed: every delta there is single-digit
milliseconds, inside timer jitter.

Snapshot (2026-09-16, sf=1, `amd64-...-16`; `exec` and `comp` from
`zquery_timing_*.log`, ratios reproduced standalone with DuckDB profiling):

| Query | Trilogy | Reference | Ratio | Root cause (measured, not guessed) |
| --- | ---: | ---: | ---: | --- |
| q05 | 0.87s | 0.08s | 10.8x | 5M-row FULL JOIN chain at line grain before the date filter; model-inherent (see 1) |
| q83 | 0.26s | 0.05s | 5.6x | FIXED: dead sibling stitch to the sales union under a returns-only pin (see 2) |
| q78 | 0.62s | 0.43s | 1.4x | one 9-CASE GROUP BY over the 3-channel union vs three narrow per-channel aggregates (see 3) |
| q75 | 0.23s | 0.10s | 2.2x | DuckDB join order inside the shared union CTE; no planner lever found |
| q23 | 0.63s | 0.42s | 1.5x | inside run-to-run noise when interleaved (min 0.32s vs 0.24s) |

Everything the previous snapshot listed (q09, q97, q28, q66, q65, q73, q25,
q85, q29, q81) now sits within +0.05s of the reference; those sections are
kept below only where the pattern is still live.

## Highest-impact Patterns

### 1. q05: sales x returns unified at line grain, filtered by an OR across sides

`all_sales` unifies sales and returns onto one (item, order, channel) row, so
`entity_text <- coalesce(sale dim, return dim)` couples the two sides and the
row gate `sale_date in window OR return_date in window` cannot be pushed into
either side. The generated plan FULL JOINs every sales row to every returns row
(5,041,336 rows through four hash joins, 7.7s of operator time) and filters to
55K rows afterwards; the reference filters each UNION ALL branch by date first
and never joins sales to returns.

This is the query's semantics, not a planner defect: splitting the OR into
per-side pre-filters would move a return whose sale is out of window from the
sale's entity to the return's entity. Two smaller, real inefficiencies remain:

- `young` and `vacuous` are two unions over the same returns tables (split
  only because the WEB return dim id lives on `web_sales`), FULL-joined to each
  other on the same keys. Merging same-source union arms would save one
  5M-row join (~1.3s operator time).
- The return-date `date_dim` join renders FULL instead of LEFT; harmless for
  cost, but every unmatched calendar row becomes an all-NULL line.

Rewriting the query as a `union`-first shape is the only route to the
reference's cost.

### 2. q83: a pin only the partial fact can satisfy heals its `~` keys (landed)

`where sales.return_date.week_seq in (...)` can only be true on a returns
row, yet the plan FULL-joined the 3.4M-row sales union to the returns union to
"complete" the returns' `~order_id`/`~item.sk`, then dropped every sales-only
row at the date join. The sibling-anchor guard in `heal_pinned_partials`
blocked healing whenever any sibling carried the key in a larger grain.

The guard is now conditional (`_anchors_dispensable`): an anchor is
dispensable when (a) some proven-non-null concept is outside what the anchor's
rows can carry by keyed lookup (`_lookup_supply`, which stops at `~`
bindings - the FD closure is the wrong tool because a same-grain sibling puts
its columns in the closure), and (b) everything the statement references in
the fact's component is reachable from the fact without an anchor.
Partition-disjoint `complete where` siblings never anchor and never count as
suppliers. q83: 0.45s -> 0.11s, planning 0.99s -> 0.14s, rows unchanged.

Not healed on purpose: q78 (`sale_date.year = 2000` is suppliable by the
sales anchor) and any q83 variant selecting a sales measure (guard (b)).

### 3. q78: per-channel filtered aggregates over a partitioned union

All nine measures are `sum(metric ? sales.channel = C) by keys`. Each
`channel = C` filter implies exactly one `complete where channel = C` arm,
but `_datasource_materializes` only prunes arms against the statement WHERE,
so the aggregate runs over the 3-arm union (970K rows, 873K groups, nine
`CASE` sums; 3.5s of GROUP BY operator time vs 1.8s for the reference's three
narrow groups). A hand-written per-arm shape (three aggregates on the group
keys, LEFT-joined onto the STORE side) returns identical rows in 0.55s vs
0.74s.

Planner work: when a FILTER derivation's condition implies a partition
predicate, plan its parent with that condition as the row bound so the arm
qualifies alone and its siblings are excluded; a same-grain aggregate over the
filtered concept then becomes a per-arm aggregate and the existing by-key merge
joins the channels. Gate on the group keys being bound on every arm.

### 4. Filtered aggregate expansion (q09/q28) - resolved

The wide `CASE WHEN ... THEN value` scan is now within noise of the reference
(q09 +0.03s, q28 +0.10s). Keep as the fuzz shape for pattern 3.

The same heal also fires on q01 (`ss.return_date.year = 2000`) and q91
(`cs.return_date.year = ...`): the sales-fact INNER join that only served to
"complete" the returns' `~` keys is gone (q01 0.047s -> 0.019s, now under the
0.027s reference).

## Retained Patterns (no longer measured regressions)

These shapes still appear in the generated SQL but no longer cost measurable
time on this corpus. Keep them as watch items, not work items.

- **Outer-join merge where the reference inner-joins** (q25, q29, q17, q65):
  null-rejection analysis converting LEFT/RIGHT/FULL to INNER remains a valid
  cleanup; all four are now within +0.03s of the reference.
- **Repeated filtered fact graphs** (q65, q73): shared filtered CTE extraction
  and aggregate-first/decorate-later planning. q73 is +0.03s.
- **Over-broad GROUP BY without local aggregates** (q53, q59, q64): a
  report-only no-op GROUP BY detector first; any cleanup must preserve rollup
  grain and `grouping()` (see `STATUS.md` for q36/q70/q86).
- **Scalar `FULL JOIN on 1=1`** (q09, q59, q77, q76, q66): cosmetic when both
  inputs are single-row aggregates; render as CROSS JOIN only after tracking
  scalar cardinality.

## Suggested Implementation Order

1. Per-arm filtered aggregates over a partitioned union (pattern 3; q78 first,
   q09/q28 as the fuzz shape).
2. Same-source union arm merging for q05's `young`/`vacuous` pair.
3. Null-rejection outer-to-inner join simplification (retained pattern).
4. Report-only no-op GROUP BY detector, then selective cleanup.

## Known Semantic Gap (pre-existing, pinned)

`tests/engine/test_duckdb_partial_key_assembly.py::test_anchor_needed_keeps_saleless_return`
(strict xfail): a `~`-keyed fact row with no anchor row (a return whose sale is
absent) is dropped when the pin sits beside an anchor-only measure, because the
anchor merge renders INNER. Not exercised by TPC-DS data (every return has a
sale) and unchanged by the q83 heal, which never fires in that shape.

## Query Size Minimization

`query67.preql`: let a bare rank/rollup inherit the grain dimensions from the
select, same as a bare aggregate. This saves redefining those dimensions.

## Notes

- Be careful with rollups. `STATUS.md` documents q36/q70/q86 planner behavior
  where hidden grouping bits and rollup NULL semantics matter. Any group-by
  cleanup pass must preserve rollup grain and `grouping()` behavior.
- Be careful with `NOT IN` and NULLs. Anti-semijoin rewrites must preserve SQL
  null semantics unless the filtered key is known non-null.
- DuckDB may already optimize some generated shapes internally. Prioritize
  changes that improve the slower-than-reference queries above, not just SQL
  aesthetics. Interleave trilogy/reference runs when measuring: q23's gap
  vanished under interleaving.
