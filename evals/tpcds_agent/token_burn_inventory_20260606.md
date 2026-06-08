# Token-burn / retry inventory — full 99-query run `20260606-032822`

Source run: `evals/tpcds_agent/results/20260606-032822_{enriched,ingest}` (deepseek-chat,
sf=1, max_iterations=75). Method: per-query token/iteration/error counts extracted from
`agent_log.qNN.jsonl`, then the top outliers diagnosed from the conversation logs +
generated `.preql` + reference (one subagent per query, "diagnose from artifacts").

Pass rates: **enriched 47/99 (47.5%)**, ingest lower (many exhausted/timeout).

Category taxonomy (from `EVAL_LOOP_INSTRUCTIONS.md`): **1 BUG** (framework crash / invalid
codegen) · **2 IDIOM** (agent misused supported syntax / missing language idiom) ·
**3 TOOL** (tool thrashing) · **4 QUESTION** (bad/ambiguous question) · **5 MODEL**
(missing model description / precalc / conformed model).

---

## Top token outliers (combined, >~800K tokens in either leg)

| q | leg | tokens | iters | status | primary cause | category |
|---|-----|-------:|------:|--------|---------------|----------|
| 78 | both | 5.7M | 75 | exhausted | dangling-CTE codegen crash (tracked bug MD) | **1 BUG** |
| 64 | enr | 4.4M | 72 | fail | merge/align/derive self-pair idiom; +ORDER-BY aggregate BinderException | **2 IDIOM** (+1) |
| 77 | ing | 4.9M | 71 | timeout | rollup cluster (documented hard) | 1/2 |
| 80 | ing | 4.2M | 75 | exhausted | rollup cluster (documented hard) | 1/2 |
| 49 | both | 3.9M(ing)/1.9M(enr) | 75/44 | exhausted/**pass** | **planner recursion crash** on rank-over-aggregate-ratio; ingest variant = INVALID_REFERENCE codegen + scoped-join bleed | **1 BUG** |
| 2  | enr | 3.8M | 75 | exhausted | dangling-CTE codegen crash (tracked bug MD) | **1 BUG** |
| 23 | ing | 3.8M | 68 | fail | no conformed all_sales → merge-chain, 0 rows | **5 MODEL** |
| 76 | both | 3.7M/3.4M | 66/75 | fail/exhausted | N-arm union idiom; +all_sales missing CS_SHIP_ADDR_SK | **2 IDIOM** (+5) |
| 75 | ing | 3.7M | 63 | fail | UNION-distinct dedup (documented hard) | 2 |
| 44 | enr | 3.6M | 75 | fail | **planner recursion crash** (rank+threshold) | **1 BUG** |
| 59 | both | 3.3M/2.3M | 75/47 | exhausted/fail | cross-period self-pair (merge/align/derive) idiom | **2 IDIOM** |
| 97 | ing | 3.1M | 75 | exhausted | no conformed all_sales; **`list index out of range` planner crash** on composite merge | **1 BUG** + 5 |
| 17 | enr | 3.0M | 64 | fail | query-scoped JOIN inner-vs-left misuse; abandoned correct model on 0-row | **2 IDIOM** |
| 38 | ing | 2.9M | 75 | exhausted | N-way tuple intersect; no conformed model; 1× cross-model-aggregate recursion | **2 IDIOM** + 5 |
| 54 | ing | 2.5M | 54 | fail | **INVALID_REFERENCE codegen bug** on OR-combined `in <derived>` in WHERE | **1 BUG** |
| 14 | ing | 2.4M | 41 | fail | **`grouping()` in ORDER BY + rollup codegen crash** | **1 BUG** |
| 50 | enr | 1.9M | 47 | fail | fact-to-fact ticket+item+customer pairing model-discoverability | **2 IDIOM**/5 |
| 86 | enr | 1.7M | 51 | **pass** | net-paid rollup+rank (documented hard) — passed but costly | 2 |
| 60 | ing | 1.6M | 41 | fail | "item-id group" prefix = no-op on this data | **4 QUESTION** |
| 31 | ing | 1.5M | 44 | fail | merge/align/derive cross-fact pivot idiom | **2 IDIOM** |
| 58 | ing | 1.5M | 43 | fail | cross-fact merge collapses to 0 rows; accepted wrong | **2 IDIOM** |
| 77 | enr | 1.3M | 34 | fail | rollup cluster | 1/2 |
| 5  | both | 1.2M/0.9M | 29/26 | timeout/fail | **600s hang on fact-fact merge** (ing); rollup-coalesce nulls (enr) | **1 BUG**(ing)/2 |
| 75 | enr | 1.2M | 32 | fail | UNION-distinct dedup (documented hard) | 2 |
| 66 | ing | 1.1M | 32 | fail | **ingest enum-typed `warehouse_sq_ft`** breaks coalesce/case; pivot shape | **5 MODEL**/1 |
| 6  | enr | 1.1M | 39 | **pass** | costly but passed | — |
| 51 | ing | 1.0M | 30 | fail | window-in-HAVING + align idiom; wrong date filter | **2 IDIOM** |
| 82 | enr | 0.9M | 32 | fail | inventory grain ambiguity ("100–500 units" per-wh vs summed) | **4 QUESTION** |
| 70 | enr | 0.9M | 37 | **pass** | rollup cluster — passed but costly | 2 |
| 13 | enr | 0.8M | 32 | fail | "per-line sales price" → ext vs per-unit trap | **4 QUESTION** |

---

## NEW framework bugs found (priority — these crash on valid-looking input)

### B1. Planner recursion crash on rank/window over an aggregate-ratio  ⭐ highest
- **q44 enriched, q49 enriched.** `Unexpected error: maximum recursion depth exceeded` /
  `Recursion error building concept ... lineage divide(sum(a),sum(b)) ... circular reference`.
- Trigger: a `rank() over (order by sum(a)/sum(b))` (or `rank()`+threshold filter) where the
  ordering/filter expression is **address-identical** to a SELECT projection. Planner keys by
  address → both collapse to one node → unbounded recursion.
- This is an **opaque crash, NOT** the clean self-reference-alias rejection
  (`SELECT output X references X itself`). q44 reproduces with no self-ref alias at all.
- Workaround the agents eventually found: name each operand as a distinct `by`-grained `auto`
  so addresses differ. Both q44/q49-enriched eventually produced output (q49 passed) — pure
  budget burn.
- **Fix:** reuse the already-built node when a window `order by` expr is address-identical to a
  projection; at minimum convert the crash to the clean self-ref rejection.

### B2. INVALID_REFERENCE_BUG / dangling-CTE codegen — broader than the tracked MD
- Tracked: `bug_invalid_reference_codegen_having_membership.md` (q2/q78, membership-in-HAVING).
- **NEW trigger shapes:**
  - **q49 ingest:** `having rank(...) over (order by sum(a)/sum(b)) <= 10` → render emits
    `rank() over (order by INVALID_REFERENCE_BUG / INVALID_REFERENCE_BUG)`; `ValueError`.
  - **q54 ingest:** OR-combined `where X in cat_qual or X in web_qual and ...` over filtered
    `auto` concepts → membership feeders never materialized → dangling
    `INVALID_REFERENCE_BUG."cat_qual_customers"` CTE refs.
- The invariant is the same; the underlying gap is **membership/aggregate feeders not getting
  CTEs** whenever they appear OR-combined or inside a window/HAVING. Generalize the fix.

### B3. `grouping()` in ORDER BY + `by rollup` → invalid SQL
- **q14 ingest.** Single-statement `rollup(...)` with `order by grouping(col) desc` emits
  `grouping(col)` in the ORDER BY of a CTE that has no GROUP BY → DuckDB
  `GROUPING statement cannot be used without groups` (2×). Part of the rollup hard-cluster but
  a concrete, reproducible codegen scope bug.

### B4. align/derive aggregate field in outer ORDER BY → BinderException
- **q64 enriched.** `column "cnt_00" must appear in the GROUP BY clause` (2×) — an
  align/derive aggregate alias emitted unguarded in the outer ORDER BY.

### B5. Query-scoped JOIN cross-statement bleed  ⭐ (relates to current `join-as-merge-spike` branch)
- **q49 ingest.** Three SELECTs in one file each using `inner join`: the scoped joins from
  statement 3 bled into statements 1–2 — all rows came back labelled `channel="store"` with
  identical store data. The join scope is not isolated per-statement.

### B6. `list index out of range` planner crash
- **q97 ingest.** Composite-key (customer,item) `merge` across store+catalog facts →
  `Unexpected error: list index out of range` (2×). Worth a minimal repro.

### B7. Silent 600s hang on fact-to-fact merge on a non-unique key
- **q05 ingest.** `merge sr.store.store_sk into ~ss.store.store_sk` (two large facts on a
  non-unique dimension key) fans out near-cartesian → `subprocess timed out after 600s`, no
  early guard/error. Ate the whole run. A planner guard + clear error would save the budget.

### (tracked, recurring) cross-model-aggregate recursion
- **q38 ingest** (1×) hit the known `recursion_bug_handoff.md` shape (cross-model `by`-grouped
  `auto` + `sum(1) by same dims`). Single detour, not the dominant sink there.

---

## Dominant IDIOM gap: the `merge / align / derive` multi-select construct  ⭐ biggest sink overall

Recurs as the #1 token sink across **q64, q76, q59, q31, q51, q38, q23, q49** (cross-period
self-pairing, cross-channel UNION, cross-fact pivot/intersection). DeepSeek has no reliable
mental model for Trilogy's aligned multi-select and loses 30–75 iterations cycling through
malformed variants. Specific sub-mistakes, each recurring:

- `derive` requires a function/conditional — agents write a **bare ref or constant**
  (`derive store_amt_s` / `derive 1999`) → `Invalid derive expression ref:..., must be a
  function or conditional` (q64, q76, q59, q31).
- **self-reference alias** `<expr> as foo` then reading `foo` (q64, q49).
- **every projection needs `as`** — agents write bare `min(x)`, `count(x)`, `select distinct`.
  The error quotes its own *suggested* fix (`min(x) as x_min`), which made several look like a
  parse bug but is correct behaviour (q59, q50, q17, q23, q58, q60).
- `align` keys joined with `and`, not commas; `quarter` is reserved (q31).
- **window functions:** must go in a hidden-select + HAVING, never WHERE; `derive` can't host a
  window (q51).
- `having` can't follow `derive`; HAVING refs must be in SELECT projection (multiple).

**Highest-leverage fix:** a worked `merge … align … derive` example in `agent-info` /
RULE_PROMPT covering (a) output columns as `--`/normal select items not `derive`, (b) derive =
function/conditional only, (c) align keys with `and`, (d) window-in-HAVING, (e) per-arm column
shapes must match. This one construct dominates the long tail.

## Secondary IDIOM: query-scoped JOIN discoverability (current branch feature)
- **q17:** agent self-discovered the new join clause but chose `left join` where the reference
  is INNER → catalog counts all 0; also tried a filter after `join` (the parse error we just
  added). agent-info needs an **inner-vs-left correctness** example. Also a meta-lesson: a
  0-row result is a debugging signal, not a dead end (agent abandoned the correct conformed
  model after one empty run).

## Recurring failure mode: confidently-wrong "success"
Agents return control on a clean-but-wrong result instead of recognizing a red flag:
q58 (0 rows vs 5), q13 (all-null), q23 (0 rows), q66 (wrong pivot shape), q60. Worth a
RULE_PROMPT note + possibly a harness guard that flags empty/degenerate result sets.

---

## MODEL gaps
- **Ingest leg lacks a conformed cross-channel `all_sales`** — the single biggest ingest theme.
  References `import all_sales`; the auto-ingested `raw/*` are three disconnected facts, forcing
  `merge` chains that hit the planner limits above (q23, q97, q38, q49, q54, q58).
- **Enriched `all_sales` missing `CS_SHIP_ADDR_SK`** mapping → q76 catalog-ship-address null
  is unrepresentable; agent fell back to a wrong `bill_address` proxy.
- **Ingest auto-typing types numeric/id columns as ENUM** (`warehouse_sq_ft` q66, `store_id`
  q05) → breaks `coalesce(x,0)`, `case`, and cross-channel `align`
  (`Value 0 is not valid for enum field`, `Datatypes do not align`). Gate enum inference by
  semantics/name, not just cardinality.

## BAD QUESTION
- **q82** — "had between 100 and 500 units of inventory on hand" is per-warehouse-row in the
  reference; agent oscillated per-wh vs summed. State the grain.
- **q60** — "same item-id group" → agent used `substring(item_id,1,5)`, but all ids share
  prefix `AAAAA` here → no-op (returns all items). Say "matches the item code of any
  Music-category item" (full equality / `in`).
- **q13** — "per-line sales price" → agent used `ext_sales_price`; reference uses per-unit
  `sales_price`. The recurring per-unit trap (documented). Say "per-unit sales price".

---

## Already-tracked / known-hard (not re-investigated here)
- **q78, q2** — dangling-CTE codegen bug → `bug_invalid_reference_codegen_having_membership.md`.
- **Rollup cluster q70/q77/q80/q86** — `grouping()`+rollup mis-plan; documented in
  EVAL_LOOP_INSTRUCTIONS "Open / harder than a question fix".
- **q75** — UNION-DISTINCT dedup before aggregate (documented).
- **q87** — anti-join on derived concat key (~3% under; documented).
</content>
</invoke>
