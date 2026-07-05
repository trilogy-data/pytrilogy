# q14 re-check — run 20260705-142435 (723,609 tokens, FAIL)

Re-verified on today's engine (workspace DB of the failing run). q14 exit 0,
100 rows, `"result set differs from reference"` — **silent wrong rows**, same
class of failure as prior runs but a **different, still-open** driver.

## Verdict per documented hypothesis

| Hypothesis (prior doc / memory) | Current verdict | Evidence |
|---|---|---|
| expr-RHS membership `<expr> in (<rowset.col>::string ‖ …)` → BinderException "vacuous not found" | **FIXED** | repro runs, 3 rows, correct (CATALOG 235070713.24 / STORE 320226392.86 / WEB 119438599.19) |
| `by rollup` + `having sum(x) > <cross-rowset scalar>` drops ALL subtotals (direct ref) | **FIXED** | grand-total + channel subtotals present (2692 rows, (None,None,…) and (CATALOG,None,…) rows emitted) |
| …same, `auto`-wrapped scalar (memory says still drops) | **FIXED** | auto-wrapped form now identical to direct — subtotals retained |
| numeric `parameter` unsupported | **N/A / FIXED** | q14 uses no parameters; not exercised this run |
| prior PRIMARY: scoped `inner join` onto rowset renders FULL | **MOOT / SUPERSEDED** | scoped `inner join` is now REMOVED from the language — `HydrationError: 'inner' join is not supported … use SUBSET/UNION/LEFT/FULL` |
| **CANONICAL** `tests/modeling/tpc_ds_duckdb/query14.preql` (concat composite key + rollup + having-vs-rowset-scalar) | **WORKS** | 100 rows, correct rollup shape (grand total 673409655.64 / 155567) |

The canonical idiom (encode `(brand,class,category)` as a single `concat(… '|' …)`
`tuple_key`, filter fine query with `tuple_key in cross_tuples.ci_tuple_key`)
builds and executes correctly. Nothing the task listed as a hypothesis is still
broken.

## What actually drove THIS run's 724k-token churn

The agent never found the concat-composite-key idiom. It needed **multi-column
(brand,class,category) tuple membership** and tried every other shape; each hit a
wall:

1. `auto qualifying <- s.item.brand_id, s.item.class_id, …` (comma RHS) → parse
   error (correct rejection, but no hint toward `concat`).
2. **multi-key `subset join` onto the rowset → ENGINE BUGS (still open):**
   - full q14 shape (3-key subset join + `by rollup(4 keys)` + `case grouping()`
     projections + outer `having sum > overall_stats.overall_avg`) →
     **DuckDB BinderException**: `column "all_channel_bcc_category_id" must appear
     in the GROUP BY clause`. Reproduced today (agent msg 45; my `hh2`).
   - 2-key subset join, no rollup → `DisconnectedConceptsException`
     (`{all_channel_bcc.brand_id, nov_2001_bcc.channel}` "not joinable").
   - 2-key subset join + `rollup(channel only)` → `DisconnectedConcepts`.
   - reduced 2-key subset + rollup **executes but is silently wrong**: subset
     lowers to `LEFT OUTER JOIN … coalesce(k)` with `WHERE coalesce(k) is not
     null` (always true) → non-matching rows preserved, no restriction.
3. cross-rowset scalar in outer `having` reached via the join → `DisconnectedConcepts`
   (`{…}; {overall_avg}` "missing a join or merge"); `(select * from ov).x`
   workaround → parse error.
4. Gave up and submitted three **independent** `in` filters (`brand_id in …
   and class_id in … and category_id in …`) — a cross-product the agent itself
   flagged as wrong (msg lines 3471-3477). It validated (exit 0, 100 rows) and
   was submitted → **wrong rows** (also emits UPPERCASE channel vs ref lowercase;
   count_distinct(channel)=3 applied per-column not per-tuple).

## Minimal repro of the still-open engine bug (BinderException)

Multi-key `subset join` onto a rowset, rolled up over the join keys, with a
cross-rowset scalar `having` + `grouping()` projections:

```trilogy
import raw.all_sales as s;
rowset all_channel_bcc <- where s.date.year between 1999 and 2001
select s.item.brand_id as brand_id, s.item.class_id as class_id, s.item.category_id as category_id
having count_distinct(s.channel) = 3;
rowset overall_stats <- where s.date.year between 1999 and 2001
select avg(s.quantity * s.list_price) as overall_avg;
rowset nov_2001_bcc <- where s.date.year = 2001 and s.date.month_of_year = 11
select s.channel as channel, s.item.brand_id as brand_id, s.item.class_id as class_id,
    s.item.category_id as category_id, sum(s.quantity * s.list_price) as total_sales,
    count(s.sale_line_item_counter) as sale_count;
select nov_2001_bcc.channel as channel,
    case when grouping(nov_2001_bcc.brand_id)=1 then null else nov_2001_bcc.brand_id end as brand_id,
    case when grouping(nov_2001_bcc.class_id)=1 then null else nov_2001_bcc.class_id end as class_id,
    case when grouping(nov_2001_bcc.category_id)=1 then null else nov_2001_bcc.category_id end as category_id,
    sum(nov_2001_bcc.total_sales) as total_sales, sum(nov_2001_bcc.sale_count) as total_count
subset join nov_2001_bcc.brand_id = all_channel_bcc.brand_id
    and nov_2001_bcc.class_id = all_channel_bcc.class_id
    and nov_2001_bcc.category_id = all_channel_bcc.category_id
where all_channel_bcc.brand_id is not null and all_channel_bcc.class_id is not null and all_channel_bcc.category_id is not null
having sum(nov_2001_bcc.total_sales) > overall_stats.overall_avg
by rollup (nov_2001_bcc.channel, nov_2001_bcc.brand_id, nov_2001_bcc.class_id, nov_2001_bcc.category_id)
limit 100;
```
→ `BinderException: column "all_channel_bcc_category_id" must appear in the GROUP BY`.

### Trigger matrix (today's engine)

| shape | result |
|---|---|
| canonical concat-`tuple_key` membership + rollup + having-vs-rowset-scalar | **OK (100 rows, correct)** |
| single-col `in` membership + rollup + having-vs-rowset-scalar | OK |
| 1-key subset join + rollup | OK (row-preserving/​loose, not restricted) |
| 2-key subset join, **no** rollup | DisconnectedConcepts (build) |
| 2-key subset join + rollup(channel only) | DisconnectedConcepts (build) |
| 2-key subset join + rollup(keys) + case/grouping | OK (loose) |
| 2-key subset join + rollup(keys) + case + having-vs-scalar | OK (loose) |
| **3-key subset join + rollup(keys) + case + having-vs-scalar** | **BinderException (exec)** |
| scoped `inner join` onto rowset | HydrationError — syntax removed |

The crash needs the **3rd** join key: the extra rowset-boundary join layer it
adds is what leaks an ungrouped exposed key into the rollup CTE.

## Root cause (file:line)

`MergeNode._inject_scoped_join_key_exposure`
(`trilogy/core/processing/nodes/merge_node.py:311-348`) force-adds every
coalescing-join-key **group member** (`all_channel_bcc.category_id`, …) as an
output of each merged parent so shared-canonical join inference can pair the
sides. It is **grain-blind**: when the parent it augments is (or feeds) a
`ROLLUP`-grouped aggregate node, the injected key column is emitted in that CTE's
`SELECT` but is neither aggregated nor in the `ROLLUP(...)` GROUP BY list → the
generated SQL is illegal (`… all_channel_bcc_category_id as … FROM … GROUP BY
ROLLUP(2,1,3)`), which DuckDB rejects. The subset join itself lowers to a
coalescing outer join via `MergeNode.create_full_joins`
(`trilogy/core/processing/nodes/merge_node.py:246`); stacking that across three
rowset boundaries (one layer per additional key, plus the cross-rowset-scalar
`having` join) is what produces the ungrouped-column CTE. Coalescing-scoped-join
key handling lives in `trilogy/core/processing/node_generators/rowset_node.py:395`
(`_expose_coalesced_key_contents`).

Secondary, still-open, contributed to the churn:
- **Masking bug (still present):** an alias that appears ONLY inside a `--`
  comment resolves as a referenceable concept. `having channel_count = 3`, with
  `--count_distinct(s.channel) as channel_count` commented out, builds and runs
  (916 rows); the submitted file's `order by _level` (also only in a comment)
  likewise resolved. This let a half-edited/incorrect file "validate," hiding the
  agent's error. (Same finding as the prior doc; unfixed.)
- **Guidance gap (model defect):** `agent-info syntax example staged-membership`
  demonstrates only **single-column** `in`. There is no example of composite
  (concat / `tuple_key`) multi-column membership — the exact pattern q14 needs and
  the canonical `.preql` uses. The agent read the example (msg 17) and still could
  not derive the multi-column form.

## Classification

- **Engine bug (open):** multi-key `subset join` onto a rowset is unusable —
  DisconnectedConcepts in most shapes, silently row-preserving (wrong) when it
  does compile, and BinderException in the full q14 rollup shape. Root cause
  `merge_node.py:311-348` (grain-blind key exposure) + `:246` (coalescing lowering).
- **Guidance/model defect:** no composite-membership example → agent never reached
  the working canonical idiom.
- **Masking bug (open):** `--`-commented aliases resolve in HAVING/order_by.
- **Agent error:** final submission used independent per-column `in` (a
  cross-product) + uppercase channel labels → wrong rows; would have been caught
  without the masking bug.

The prior doc's PRIMARY (scoped INNER→FULL) is now moot: scoped `inner join` was
removed from the language. All three named hypotheses (expr-RHS membership,
rollup+having-vs-scalar subtotals, numeric parameter) are **FIXED/verified**. The
current 724k sink is a **new/residual engine bug in multi-key subset-join-onto-
rowset**, amplified by a guidance gap and the comment-alias masking bug. Not fixed
(per task).
