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
- **Per-unit vs extended price.** Some references sum the per-unit `sales_price` (not
  `ext_sales_price`). If the reference is per-unit, say "per-unit sales price (not the extended
  amount)". (q89.)

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

### Question-fix philosophy
- Phrase as business intent; never leak implementation (no "is not null", no "inner join",
  no SQL). For nulls use "where we have data" / "where X is recorded".
- Don't over-constrain. A "customers with no such line should not appear" nudge made the agent
  add `customer.id is not null`, wrongly excluding the reference's null-customer group. Say only
  what the business needs.
- Specify the output grain when the reference groups finer than it reports (e.g. q91 groups by
  marital/education but only shows call-center columns → say so) and the exact identifier
  (id vs name vs full_name).


