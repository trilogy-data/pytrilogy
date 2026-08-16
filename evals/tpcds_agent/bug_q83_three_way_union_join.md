# P0: 3-way chained `union join` breaks key rendering in generated SQL (q83)

## Symptom(s)

A query-scoped 3-way chained union join (`union join a.k = b.k = c.k`) over three
rowsets, with a HAVING that references a per-side column from every side, produces
invalid SQL. Two of the three previously observed symptoms reproduce on the current
tree; the third does not:

1. **REPRODUCES - ParserException**: `(_duckdb.ParserException) Parser Error:
   zero-length delimited identifier at or near """"`. The generated SELECT (and
   ORDER BY, when present) contains a coalesce of the merged join key whose
   catalog-side arm is a rendered error-message sentinel:
   `coalesce("concerned"."INVALID_ALIAS: [MODELS_EXECUTE] Concept local"."item_code@Grain<...> not found on cr"...."","kaput"."item_code","puffy"."web_ch_item_code")`.
   The sentinel is dot-split and re-quoted, and its tail produces an empty `""`
   identifier.
2. **REPRODUCES - BinderException**: `Binder Error: Values list "concerned" does
   not have a column named "_combined_item_code"` (3-way chain inside a rowset), and
   the sibling shape `Binder Error: Values list "uneven" does not have a column
   named "item_code"` (plain select, one extra store-side column). The coalesce/
   reference names the merged key by the CONSUMER's column name on a side-CTE that
   exposes it under its own namespaced name (`catalog_ch_item_code`).
3. **DOES NOT REPRODUCE - "ORDER BY silently ignored"**: every shape that executes
   renders its ORDER BY and returns correctly sorted rows (variants A/I/J/W below).
   Re-examining the original 20260813-125008_ingest run: the "unsorted" 24-row
   answer output is byte-identical in order to today's correctly-sorted output
   (`AAAAAAAAAHFBAAAA` (9 A's) legitimately sorts before `AAAAAAAACLJBAAAA`
   (8 A's + C)); the agent miscounted leading A's. Symptom 3 was a mis-observation,
   not a bug, and there is nothing to fix.

Per the standing rule, Parser/Binder errors from generated SQL are framework bugs
regardless of how awkward the authored Trilogy was.

## Reproducible-on

`2435237d4` (branch `more-benchmarking`, post-rebase onto latest main).

Harness: `sys.path.insert(0,'evals'); from common import scoring;
eng = scoring.make_scoring_engine(db, ws, 'tpcds')` with
`ws = evals/tpcds_agent/results/20260813-125008_ingest/workspace` and a scratchpad
copy of that workspace's `warehouse.duckdb`. Full variant script:
scratchpad `repro_q83.py` (variants named below).

## Minimal repros

Shared preamble (the ingest model; three aliased imports of the same fact-shape
models plus per-channel rowsets):

```trilogy
import root.store_returns as sr;
import root.catalog_returns as cr;
import root.web_returns as wr;

rowset target_weeks <- where sr.date_dim.date in ('2000-06-30'::date, '2000-09-27'::date, '2000-11-17'::date)
    select sr.date_dim.week_seq as week_seq;

rowset store_ch <- where sr.date_dim.week_seq in target_weeks.week_seq
    select sr.item.item_id as item_code,
           count(grain(sr.item.item_sk, sr.ticket_number)) as rows_cnt,
           sum(sr.return_quantity) as qty,;
rowset catalog_ch <- where cr.date_dim.week_seq in target_weeks.week_seq
    select cr.item.item_id as item_code,
           count(grain(cr.item.item_sk, cr.order_number)) as rows_cnt,
           sum(cr.return_quantity) as qty,;
rowset web_ch <- where wr.date_dim.week_seq in target_weeks.week_seq
    select wr.item.item_id as item_code,
           count(grain(wr.item.item_sk, wr.order_number)) as rows_cnt,
           sum(wr.return_quantity) as qty,;
```

**Symptom 1 minimal (variant `Q_min_rows_only`) - zero-length identifier:**

```trilogy
select
    store_ch.item_code as item_code,
    store_ch.rows_cnt as store_rows,
    catalog_ch.rows_cnt as catalog_rows,
    web_ch.rows_cnt as web_rows,
union join store_ch.item_code = catalog_ch.item_code = web_ch.item_code
having store_ch.rows_cnt > 0 and catalog_ch.rows_cnt > 0 and web_ch.rows_cnt > 0
limit 5;
```

No ORDER BY needed; the broken coalesce is in the SELECT list itself.

**Symptom 2 minimal, plain-select flavor (variant `N`) - Binder "item_code":**

```trilogy
select
    store_ch.item_code as item_code,
    store_ch.qty as store_qty,
    catalog_ch.qty as catalog_qty,
    web_ch.qty as web_qty,
    store_ch.rows_cnt as store_rows,
union join store_ch.item_code = catalog_ch.item_code = web_ch.item_code
having store_ch.rows_cnt > 0 and catalog_ch.rows_cnt > 0 and web_ch.rows_cnt > 0
limit 200;
```

**Symptom 2 exact historical flavor (variant `D`) - Binder "_combined_item_code":**
wrap the failing select (key + qty x3 + rows_cnt x3 + having) in
`rowset combined <- ...;` and select from `combined` in an outer query.

## Trigger matrix

| Variant | Chain | Select refs | ORDER BY | HAVING | Result |
|---|---|---|---|---|---|
| E/F | 2-way | key + per-side cols | bare / qualified | yes | OK, sorted |
| M | 2-way inside rowset | key + per-side cols | none | no | OK |
| A / J | 3-way | key + qty per side (+pct math in J) | bare alias | yes | OK, sorted |
| I | 3-way | key + qty per side | qualified | yes | OK, sorted |
| G | 3-way | key + qty per side | none | yes | OK (unsorted as expected) |
| H | 3-way | qtys only, NO key | on qty | yes | OK |
| P | 3-way | key + qty x3 + rows_cnt x3 | none | **no** | OK |
| N | 3-way | key + qtys + **store** rows_cnt | none | yes | **Binder: no column "item_code"** |
| O | 3-way | key + qtys + **catalog** rows_cnt | none | yes | OK |
| B / C / K / Q | 3-way | key + rows_cnt from all 3 sides | bare / qualified / none | yes | **Parser: zero-length identifier** |
| L | 3-way inside rowset | key + qty per side | none | no | OK |
| D | 3-way inside rowset | key + qty x3 + rows_cnt x3 | outer | yes | **Binder: no column "_combined_item_code"** |
| W | two stacked 2-way clauses | key + qty per side | qualified | yes | OK, sorted (landed answer) |

Boundary: 3-way chain alone is fine; it breaks when the HAVING (which references
all three sides and lets the planner upgrade the merge joins to INNER) is combined
with per-side columns beyond one qty each, so that the merge CTE must read the
merged key from all three parents. 2-way chains never break. ORDER BY is
irrelevant to both symptoms.

## Root cause (file:line per symptom)

Both symptoms share one planning defect and diverge only at the failure surface.

**Shared defect - pairwise pseudonyms cannot bridge a 3-member union key group.**
`BuildEnvironment.__init__` registers scoped-join key pseudonyms as mutual
source<->canonical edges only (star around the union-find canonical):
`trilogy/core/models/build.py:2899-2909`. In the failing plan the materialized
side columns end up with pairwise edges only - instrumented state in the final
merge CTE (`hard`):

- consumer concept `local.item_code` pseudonyms = `{store_ch.item_code}`
- catalog side column `catalog_ch.item_code` pseudonyms = `{web_ch.item_code}`

The render-time pseudonym-closure walk in `CTE.get_alias`
(`trilogy/core/models/execute.py:510-545`) builds edges only from columns present
in the parent CTE plus the target concept itself. Its documented absent-intermediate
handling (execute.py:514-517) bridges ONE absent hop; here the path
`local.item_code -> store_ch.item_code -> web_ch.item_code -> catalog_ch.item_code`
has TWO absent intermediates whose connecting edge lives only in the environment
pseudonym_map, so resolution fails for the catalog (middle) member. A 2-way chain
is always one absent hop, which is why it works.

**Symptom 1 surface (Parser zero-length identifier):**
- `QueryDatasource.get_alias` raises ValueError
  (`trilogy/core/models/execute.py:1460-1463`);
- `CTE.get_alias` swallows it into an `"INVALID_ALIAS: ..."` sentinel string
  (`trilogy/core/models/execute.py:554-557`);
- `safe_get_cte_value`'s multi-source coalesce fallback renders the sentinel
  verbatim (`trilogy/dialect/base.py:825-830`). The INVALID_ALIAS filter exists
  only on the outer-join key-class branch (`trilogy/dialect/base.py:786-790`),
  which never engages here because the HAVING let the optimizer upgrade the merge
  joins to INNER and `CTE.outer_join_key_class`
  (`trilogy/core/models/execute.py:439-473`) only considers LEFT/RIGHT/FULL.
  `safe_quote` then dot-splits the sentinel (which contains `.` and quote chars),
  yielding a trailing empty `""` identifier -> DuckDB ParserException.

**Symptom 2 surface (Binder wrong column name):**
`QueryDatasource.get_alias`'s membership fallback
(`trilogy/core/models/execute.py:1454-1456`): after the per-`source` loop fails,
`if concept in existing: return concept.name` checks the WHOLE joined datasource's
outputs while ignoring the requested `source`, and returns the consumer's own
column name (`_combined_item_code` / `item_code`) for a side-CTE that exposes the
key only under its namespaced name (`catalog_ch_item_code`) -> BinderException.
(Instrumented: `get_alias cte=hard concept=local._combined_item_code
source=concerned -> _combined_item_code`, while `source=puffy` correctly resolves
to `web_ch_item_code` via the pseudonym walk.)

## Working alternatives the agents used

The ingest agent's landed answer (`results/20260813-125008_ingest/workspace/query83.preql`)
replaces the single 3-way chain with **two stacked 2-way union join clauses**
(verified working + sorted on this tree, variant `W`):

```trilogy
union join store_ch.item_code = catalog_ch.item_code
union join store_ch.item_code = web_ch.item_code
```

It also validated a **two-stage rowset staging** form (probe_i2): a
`rowset sc <- select ... union join store_ch.item_code = catalog_ch.item_code having ...;`
2-way pre-join, then a 2-way `union join sc.item_code = web_ch.item_code` in the
outer query.

The enriched leg (agent_log.q83 msg ~19-25) hit the same INVALID_ALIAS coalesce
with `with <name> as` rowset syntax + 3-way chain + hidden `--x.ret_cnt` columns,
and recovered by rewriting to `rowset <name> <- ...` syntax with the 3-way chain,
keeping per-side extra columns hidden (`--store_ret.ret_cnt,`) and the HAVING as
`is not null` checks - i.e. it stayed on the 3-way chain but its model
(`raw.store_sales` with `is_returned`) produced a plan where each side only
projects key + one measure, which is inside the working region of the matrix
above.

## Verdict

Framework bug, P0. Two of three symptoms confirmed on `2435237d4`:

- (1) zero-length delimited identifier: real, reproducible, root-caused
  (sentinel error-string rendered into SQL - a should-never-happen path with two
  independent guards missing);
- (2) `_combined_item_code` binder error: real, reproducible, root-caused
  (source-ignoring membership fallback returns the consumer's column name);
- (3) ORDER BY silently ignored: **gone / never existed** - mis-read of correctly
  sorted output; ORDER BY renders and applies in every executing shape.

Fix direction (planner, per the standing placement rule): make the merged-key
resolution robust for >=3-member chained union groups - either close the pseudonym
star transitively when propagating onto build concepts (build.py:2899-2909), or
teach the closure walk / `QueryDatasource.get_alias` to consult the environment
pseudonym_map instead of only CTE-local edges. Independently, execute.py:1454-1456
must not answer for a specific `source` from whole-datasource membership, and
dialect/base.py:825-830 should hard-fail (like `raise_invalid`) rather than render
an INVALID_ALIAS sentinel into SQL.
