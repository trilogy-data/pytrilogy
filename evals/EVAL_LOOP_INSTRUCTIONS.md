## Context TPC-DS/H

Goal: improve our CLI + tooling for general use cases by evaluating on curated datasets.

Oure eval harness uses a deepseek agent to answer questions.


Loop:
Run a target portion of the test set for enriched/unenriched. Typically pick ~10 questions.

Identify the following, in order of priority:

### Bugs
Unexpected frameowrk level errors or bugs (generally, unhandled exceptions) -> Bug report MD to hand off to another agent

### Agent Prompt Issues
Does answering the question require a language feature we have not exposed?

### Agent Tool Issues
Is the agent thrashing on tool use - truncated results, too many results, unclear what to use, args, tool failuer?

### Bad Questions
Not all the questions fully specify criteria or accurately map to the output or are phrased generically. Review 
them so they seem to generic business questions, not "how to write SQL" but contain all required info for a correct
answer

### Model Problems
Particularly for our enriched model, is there a comment/guidance/precalculation we could add to help an agent answer this question?


## Data Collection
When we have identified a candidate, it will typically be a single question. To measure improvement, run the same question repeatedly
using our 10x harness to validate. 

Overall goals:
- Success rate
- Token minimization


## Strategies / Lessons (append as you learn)

### Method
- **Single-shot `run_eval` is NOISY. Never conclude from one run.** Per-query pass/fail
  flips across identical re-runs (deepseek variance). Validate every before/after with
  `repeat_query.py --query-id N --repeats 10 --scale-factor 1` and compare `pass_rate`.
- **Diagnose from artifacts, not theory.** Read the agent's generated query
  (`results/<run>/workspace/queryNN.preql`), its run output, and the reference
  (`tests/modeling/tpc_ds_duckdb/queryNN.sql` AND `.preql` — the `.preql` is the
  hand-authored canonical Trilogy answer; the scorer prefers `.sql`). I misdiagnosed q96
  as a count bug off one trace; the passing vs failing reps revealed it was the demographic
  path. Compare a passing rep to a failing rep.
- Fan out diagnosis with subagents, one per query, each reading that query's
  `agent_log.qNN.conversation.txt` + generated preql + reference.
- `repeat_query.py` defaults to `--scale-factor 0.1`; pass `--scale-factor 1` for queries
  whose filter literals (store/company/item names) only exist at full scale.
- Never edit source/model/`.preql` files while a repeat run is in flight — in-flight worker
  subprocesses import the half-edited tree and crash.
- **To pin down WHY a result differs, score candidate forms directly** instead of running the
  agent. Build the engine once and diff rows:
  ```python
  import sys; sys.path.insert(0,'evals'); from common import scoring
  ws=Path('.../<run>/workspace'); eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')
  rows = lambda body: list(eng.execute_raw_sql(eng.generate_sql(body)[-1]).fetchall())
  ```
  Then `scoring.score_query(eng, ws, NN, 'tpcds', custom_refs_dir=Path('tests/modeling/tpc_ds_duckdb'))`.
  This is how q93 (else-branch live) and q99 (empty-bucket null-vs-0) were nailed in minutes.
- TPC-DS gotcha: `d_month_seq BETWEEN 1200 AND 1211` is IDENTICAL to `d_year = 2000` here (366
  days, full overlap). Don't chase a "month_seq vs year" date-filter theory — verify it first.

### Recurring TPC-DS root causes (and the fix that worked)
- **Sale-recorded vs customer-current demographic.** Facts carry a point-of-sale demographic
  FK (`SS_HDEMO_SK`, `WS_SHIP_HDEMO_SK`); agents instead navigate the customer's *current*
  demographic (`billing_customer.household_demographic`). Fix the QUESTION: "households
  **recorded on the sale**", not "customers whose household". (q96: 10%→80% N=10; also q90, q13.)
- **NULL-FK / inner-join vs left-join.** Reference comma-joins silently drop null-FK rows;
  Trilogy keeps them as NULL groups. Fix with BUSINESS phrasing: "only for sales **where the
  warehouse/ship-mode/call-center are recorded**" / "**where we have data**" — never "where
  FK is not null". (q97, q99.)
- **Counting line items.** Agents count a single grain key (`count(order_number)`,
  `count(ticket_number)`) which auto-dedups (keys are distinct) and undercounts vs the
  reference's row count. Fix BOTH: expose a named `line_item_count <- sum(row_counter)`
  measure on the fact model, and define "line item (a unique order + item combination)" in
  the question.
- **SQL null-propagation in the reference.** Some references KEEP nulls (a null price/qty makes
  the line's amount null; the customer's sum is null and sorts first). Agents `coalesce(...,0)`
  defensively → mismatch. Reframe the population so the agent writes the clean formula and
  doesn't coalesce (e.g. "where we have a recorded return quantity"). (q93.)
- **Spurious LIMIT.** The agent system prompt says "Always use a reasonable LIMIT", so it caps
  full-report queries (e.g. at 1000). For exhaustive outputs, state it in the QUESTION: "return
  every qualifying row; do not cap or limit the number of rows". (q98.)
- **Stale `impossible` grade = scale-factor artifact.** `impossible`-graded queries were
  ungradeable at sf=0.01 (filter literals not sampled); at the current default **sf=1** they
  work. Check the entry's `comment`, then re-grade (they're filtered out by `active_prompts`
  until you do). (q94/q95/q96/q88 were all this.)
- **Multi-branch formula — describe EVERY branch.** A "(qty - return_qty)*price, otherwise
  qty*price" formula has a LIVE else branch (there are reason-28 lines with null return_quantity).
  Don't let the question collapse it to one branch — the agent then coalesces or filters and the
  null-customer set diverges. State both branches; nulls from missing price are preserved naturally.
  (q93: 0%→60%.)
- **AND-of-ORs filter algebra.** When the reference is `(A1 or A2 or A3) AND (B1 or B2 or B3)`
  (a 3×3 = 9-way cross product), agents collapse it to `(A1+B1) or (A2+B2) or (A3+B3)` (3 combos).
  Write the two filter groups as explicitly INDEPENDENT conditions. (q85.)
- **Entity-role confusion (returning vs refunded customer/address).** catalog_returns has
  `billing_customer` (=RETURNING, `CR_RETURNING_CUSTOMER_SK`) vs `refunded_customer`, and
  `return_address` vs `refunded_address`. The import aliases had NO descriptions, so explore
  couldn't disambiguate and agents grabbed `refunded_customer`. Fix = add trailing-comment
  descriptions to the model imports (they render in explore). (q81, q84.)
- **Presence = a return ROW exists, not `sum(quantity) > 0`.** "Items with at least one return in
  each channel" should test row existence, not positive summed quantity (a return with qty 0/null
  is dropped by `sum > 0`). Say "at least one return record, regardless of quantity". (q83.)
- **Per-unit vs extended price — RECURRING.** Many references sum the per-unit `sales_price`, but the
  phrase "per-line sales price" lures the agent to `ext_sales_price` (line-extended). Say "the per-unit
  sales price (not the line-extended amount)". (q63, q65, q66, q89.)
- **q62 is the web-sales twin of q99** (shipping-lag buckets): same fix — count line items via the
  `line_item_count`/`row_counter` measure (not `count(order_number)`) + "report 0 for empty buckets".
  Added `line_item_count` to web_sales.preql to match catalog_sales/physical_sales.
- **Grain-inheritance idiom (agent thrash → timeout).** A derived concept combining two grained
  aggregates INHERITS their grain — do NOT append `by (...)` to it (e.g. `coalesce(a,0)+coalesce(b,0)
  by (...)` is a parse error). q66 timed out partly relitigating this. Also: a `where` is a clause that
  must immediately precede `select` — a standalone `where ... ;` is a parse error.
- **`date_diff(a, b, unit) = b - a` in Trilogy.** Agents reliably get the arg order backwards for a
  "ship date minus sold date" lag (`date_diff(ship, sold)` gives NEGATIVE). When a sibling fact has a
  precomputed lag field (catalog_sales `days_to_ship`), provide the SAME field on the others —
  I added `days_to_ship <- date_diff(date.date, ship_date.date, day)` to web_sales for q62. With a
  negated lag, every row falls in the `<=30` bucket → wrong distribution, same row count.
- **When porting a fix from a sibling query, copy ALL its guards.** q62 is q99's twin; I copied the
  count-lines + empty-bucket=0 fixes but FORGOT q99's null-FK guard ("only where warehouse/ship-mode/
  web-site are recorded"). q62 only passed once BOTH the lag sign AND the null-FK guard were added
  (10x: 90%). Re-investigate empirically (score the candidate, diff rows) when a "should-pass" fix doesn't.
- **Per-unit price is the single most common bug** — confirmed across q51, q53, q59, q63, q65, q66, q89.
  Any "sales price" / "per-line sales price" sum where the reference uses `ss_sales_price`/`ws_sales_price`/
  `cs_sales_price`. Always say "the per-unit sales price (not the line-extended amount)". Consider a
  systemic model-description tweak if it keeps recurring.
- **item code = `text_id` (business id), not `id` (surrogate).** Agents group/output `item.id` (i_item_sk)
  when the reference outputs `i_item_id`. Grouping by the surrogate also splits rows (multiple sks per
  business id). Say "the item code (the stable business identifier, not the surrogate key)". (q58.)
- **Sale-vs-store address for GMT/geography filters.** "billing or sale addresses with GMT offset -5" →
  the STORE channel's address is the SALE address (`ss_addr_sk` → `sale_address`), NOT the store's own
  `store.gmt_offset`. Agents grab the store's timezone. (q56.)
- **Multi-key fact-to-fact match the model doesn't enforce.** physical_sales fuses store_sales↔store_returns
  at `(item.id, ticket_number)` only; a "matched by ticket, item, AND customer" question needs an explicit
  `billing_customer.id = return_customer.id` filter the agent omits. (q50.)
- **Store-count multiplication quirk (q54).** The reference joins the customer to ALL stores in their
  county/state (no `ss_store_sk` join) and multiplies revenue by the store count — not "store sales at
  matching stores". Hard to convey as a sensible business question; flag as quirky-reference.

### Open / harder than a question fix
- **Anti-join on a DERIVED concat key undercounts (~3%).** q87 (set-difference: store names+dates
  minus catalog minus web) — once the agent builds the right `(last,first,date)` key and uses
  `not in`, it returns 45692 vs ref 47298. Suspected NULL-name / `NOT IN`-with-null semantics on a
  derived `concat` expression (vs the SQL `EXCEPT`). Not fixed by wording — needs a framework look at
  how `not in` over a non-key derived concept compiles. Empirically the structure is correct.
- **q83** (three-channel return-quantity percentages via cross-model `merge` + per-channel presence)
  and **q80** (3-level rollup with channel relabel + null-FK drop) and **q86** (net-paid rollup with
  rank) remain 0% even with question fixes — multi-model grain / rollup-emulation depth, candidates
  for a framework or model-precalc look, not just wording.
- **q84** is model-navigation: the agent must match the customer's current cdemo to the cdemo
  recorded ON the store return (`customer_demographic.*` namespace), but grabs
  `billing_customer.demographics` (trivially self-equal). Question is already clear; needs a model/
  explore-description nudge toward the return-recorded demographic.
- **"never returned" = `is_returned is null`, NOT `not is_returned`.** On a nullable `is_returned`
  flag (true only when a return matched, else NULL), `not is_returned` evaluates NULL→filtered, so it
  DROPS the never-returned rows you want. The anti-join idiom is `where sales.is_returned is null`.
  (q78.) Same family as the existence/anti-join idiom.
- **The QUESTION can be wrong about sale-vs-current.** q72 literally said "current customer-demographic
  marital status" but the reference uses the SALE-recorded demographic (`cs_bill_cdemo_sk`). Fix the
  QUESTION text to say "recorded on the sale", not just trust it. Don't assume the prompt is right.
- **Rollup family (q70/q77/q80) is the hard cluster.** `grouping()`+`sum()` over a ROLLUP mis-plans
  (trilogy splits it into separate CTEs → malformed NULL-level rows); the canonical `.preql` derives the
  level from the rollup output's NULL pattern instead. Plus channel relabel + null-outlet drop +
  return-date-vs-sale-date scoping. These need a framework rollup fix, not just wording.
- **UNION DISTINCT dedup before aggregating.** q75's reference dedups identical N-tuples across channels
  (`UNION`) BEFORE summing; the agent sums raw lines (double-counts). Needs a `rowset deduped <- select
  <distinct cols>` then aggregate. The model's `row_one`/`row_counter` is constant-1, NOT a dedup.
- **null-FK selection needs the per-channel base model.** q76 (count rows WHERE an fk IS null per
  channel): routing through the unified `all_sales` can inner-join the null-FK rows away; query each
  channel on its own single-channel model where the null-FK row is a plain base row.

### Question-fix philosophy
- Phrase as business intent; never leak implementation (no "is not null", no "inner join",
  no SQL). For nulls use "where we have data" / "where X is recorded".
- Don't over-constrain. A "customers with no such line should not appear" nudge made the agent
  add `customer.id is not null`, wrongly excluding the reference's null-customer group. Say only
  what the business needs.
- Specify the output grain when the reference groups finer than it reports (e.g. q91 groups by
  marital/education but only shows call-center columns → say so) and the exact identifier
  (id vs name vs full_name).


