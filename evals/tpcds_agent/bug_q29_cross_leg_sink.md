# q29 cross-leg token sink: metric artifact + question-wording gap, not a framework loop

**Verified 2026-08-20** against run `20260820-031800_{enriched,ingest,sql_schema}_deepseek_deepseek-v4-flash`
and HEAD `c40ef023b`. Both quoted engine errors reproduce byte-identically from a workspace copy;
repro bodies preserved in the session scratchpad (`probe_q29/ws/probe3_repro.preql`,
`probe_q29/ws/attempt1_repro.preql`).

Status: no framework bug behind the sink. One P3 error-quality polish item (below) is the only
engine-side finding. The headline recommendation is a detector/metric fix plus a question rewording.

## Headline numbers

q29 was the only question over 500k in all three legs, yet **all three legs PASSED**
(`status: pass`, 1 row matching reference) with no timeout and no loop:

| leg | raw total | prompt (cached) | completion | fresh (uncached+completion) | iters | tool errors |
|---|---|---|---|---|---|---|
| enriched | 931,602 | 889,679 (862,336 = 96.9%) | 41,923 | **69,266** | 18 | 5 |
| ingest | 501,667 | 464,039 (436,352 = 94.0%) | 37,628 | **65,315** | 12 | 1 |
| sql_schema | 531,822 | 504,173 (489,600 = 97.1%) | 27,649 | **42,222** | 19 | 3 |

Cache-adjusted, q29 cost 42-69k fresh tokens per leg, unremarkable for a three-fact join. The
">500k sink" is raw accounting: full-history replay where 94-97% of prompt tokens were cache hits.

Two mechanisms turn a normal run into a raw-token sink:

1. **Hidden reasoning bursts.** deepseek-v4-flash emitted one huge thinking turn per leg at the
   moment it synthesized the multi-fact join plan: enriched 26,430 completion tokens (115 chars
   visible text), ingest 19,398 (136 chars), sql_schema 7,528 (0 chars).
2. **Mandatory reasoning replay.** DeepSeek V4 thinking mode requires `reasoning_content` to be
   replayed across tool turns (deliberate, documented at `evals/common/main.py:49-51`). The
   enriched burst therefore re-enters all ~14 subsequent prompts: 26.4k x 14 is roughly 370k of the
   890k raw prompt total, nearly all of it billed as cache hits.

**Recommendation (harness):** the >500k sink detector should trigger on fresh tokens
(uncached prompt + completion), or at minimum report both figures. This is a concrete instance of
the open token-metric defect in `project_eval_scoring_carveout_and_token_metric_defects`.

## Per-leg churn

### enriched (agent_log.q29.jsonl, 18 iterations, pass)

Five tool errors, every one the agent's own mistake with a correct, actionable message:

- 2x `file read` refused: config-intended (`allow_file_read = false`), gentle deny pointing at
  `explore`.
- Attempt 1: agent invented abbreviations `qs.`/`qc.` for rowsets it had named
  `qual_store`/`qual_catalog`. Error: `Undefined concept: qs.cust_sk.` with suggestions. Agent
  error; suggestion-quality nit below.
- Attempt 2: bare `item_code` etc. in ORDER BY where the outputs were unaliased qualified names.
  The batch reporter listed all 4 misses with exact did-you-means in one message; fixed in one shot.
- probe3: `where (cs.billing_customer.sk, cs.item.sk) in (select qual_store...)` with outputs drawn
  from both sides. Discovery error `cannot merge ... 2 disconnected subgraphs` including the note
  that membership only filters its left side and that a query-scoped join is the fix. **Correct
  disconnect**: `ss.*` and `cs.*` are separate import namespaces with no declared merge; the
  agent's own working answer (a `union join` on cust/item) proves the concepts connect exactly when
  the join the error asks for is declared. The engine taught the fix and the agent took it.

Attempt 3 ran clean and matched the reference (1 row, 71/41/23).

### ingest (12 iterations, pass)

Cleanest leg. One error total: `subset join` clauses placed after `order by`-position in a
select-before-where body, refused at write time with a precise parse error; fixed on the next
attempt. The agent supplied the sale-return match keys (customer+item+ticket) from its own TPC-DS
q100 knowledge, since the question does not state them (see below).

### sql_schema (control, 19 iterations, pass)

Three DuckDB binder errors, all the agent's own column typos/ambiguity. The interesting signal is
the **18 exploratory probes from a plain-SQL agent with the full schema in hand**: probe [17] is
literally commented `-- Check whether store_returns link to store_sales by (ticket, item) and
whether customer/store match`, followed by pair-count probes for by-ticket-item vs by-customer
matching and repeated validation that a 1-row answer is real. Whatever the trilogy legs paid on top
of this baseline is small (fresh tokens 1.5-1.6x the control, mostly reasoning and doc reads).

## The one framework-level finding (P3, error-message polish)

Repro: attempt-1 body against the run workspace. Two related nits at the same raise site:

1. **Did-you-mean pool contains the statement's other undefined references.** The suggestions for
   `qs.cust_sk` were `['qs.item_sk', 'qc.cust_sk', 'qual_store.cust_sk', 'qual_catalog.cust_sk']`,
   with the two equally-undefined sibling typos ranked above the two real fixes.
   `trilogy/parsing/v2/semantic_state.py:693-695` builds `staged = [c.address for c in
   self.values()]` and passes it as `extra_keys` to `_find_similar_concepts`; `self.values()`
   includes deferred/scoped placeholders staged for the other unresolved references in the same
   statement, not only legitimately staged rowset outputs. Same pool feeds
   `_staged_addresses` in `trilogy/parsing/v2/select_finalize.py:118-126`.
2. **Scoped-join key resolution raises on the first miss instead of batching.** Traceback:
   `select_statement_rules.py:338` (`_resolve_join_key`) -> `semantic_state.py:699`
   (`_raise_undefined`), one address per raise. The batch reporter in
   `select_finalize.py:200-240` exists precisely to "collapse the fix-rerun-hit-the-next-one loop"
   and fired beautifully on attempt 2, but join-clause keys bypass it, so the `qs.`/`qc.` slip cost
   a full extra round trip (~60k raw, ~2k fresh) that batching would have merged into attempt 2's
   single fix.

Neither explains the sink; both are cheap agent-efficiency wins on a path agents hit often
(rowset abbreviation is a natural LLM slip).

## Question-wording defect (the actual cross-leg driver) - APPLIED 2026-08-21

Shipped in `query_prompts.json` (id 29):

> Some customers bought an item in a store during September 1999, returned that same
> purchase between September and December 1999 (the same customer returning the same item
> from the same sales ticket), and also ordered that same item from the catalog in 1999,
> 2000, or 2001, billed to them. For those purchases, report the store and catalog demand
> they represent: one row per item and store, identified by the item business code and
> description and the store business code and name rather than surrogate keys, with the
> total quantity sold in the store, the total quantity returned, and the total quantity
> ordered from the catalog. Order by item code, item description, store code, then store
> name; limit 100 rows.

Two things the filed proposal did not pin down:

- **Which catalog customer.** The spec query matches `cs_bill_customer_sk`; the model
  carries `billing_customer` and `ship_customer`, so "the returning customer bought that
  same item" is a coin flip between them. The shipped text says "billed to them".
- **The spec query's summation grain is a fan-out.** It sums `ss_quantity` and
  `sr_return_quantity` over store_sale x return x catalog_order pairs, so a purchase
  matched by three catalog orders counts three times. No natural wording asks for that.

The fan-out is now gone from every artifact rather than being worded around.
`tests/modeling/tpc_ds_duckdb/query29.sql` and `query29.preql` both sum the catalog side
per (billing customer, item) BEFORE it meets the store side, which is the whole fix: a
customer's second catalog order for an item then adds to that total instead of re-counting
their store quantities. Both stay flat otherwise - one pass, group by the four reported
columns - and `test_twenty_nine` compares them directly (`sql_override=True`) instead of
against `PRAGMA tpcds(29)`, joining q17/q32/q41/q44 as a deliberate spec divergence. The
eval scores against that same `.sql` through the existing `references_dir`, so question,
corpus query and reference all state one semantics.

Verification: preql and sql agree on the spec window (1 row) and with both date windows
widened to all of 1999 (7 rows). They also agree with the old pair-summed spelling, because
at sf=1 neither multiplicity exists: no `(billing customer, item)` pair has more than one
catalog line (max 1 over 265 matched pairs), and no (reported group, customer, item) has
more than one qualifying ticket (0 of 39,300). So nothing about this changes a result at
this scale - it changes what the files mean, and it would change results at a scale factor
where a customer orders the same item twice.

Cost: the `.preql` source got shorter (1,545 -> 1,256 chars, still under its 1,350-char SQL
twin) but its generated SQL grew 4,334 -> 14,113. The per-purchase catalog test is an
aggregate in the WHERE, so the plan renders both the population and the filtered scope of
the catalog side; `test_twenty_nine`'s size guard moved 12,000 -> 15,000 with that noted. A
two-level spelling (rowset at (group, customer, item), then roll up) plans at 10,614 and is
equally correct, but it is machinery the data never needs.

q29 has no `prompt_shifted` entry and is outside the paramshift categories' 20-question
range, so there is no shifted twin to keep in step. Runs before 2026-08-21 are not
prompt-comparable on q29.

Worth knowing before reading much into q29 results: at sf=1 the reference is ONE row, so
the query grades almost nothing. The three legs' matching answers in this run are a weak
signal.

The original diagnosis and the superseded proposal follow.


`evals/tpcds_agent/query_prompts.json` id 29 (grade "medium"):

> "For store sales in September 1999, where returned by the same customer sk between September and
> December 1999, then with customer sk matched to catalog sales billing_sk and item_sk where the
> catalog sale year is 1999, 2000, or 2001, ..."

Two defects, confirmed by the control leg's probing:

- **The sale-to-return match keys are unstated.** Canonical q29 joins store_sales to store_returns
  on customer + item + ticket_number; the question says only "returned by the same customer sk".
  The enriched model hides this (returns are pre-joined at sale-line grain as `ss.return_*`), but
  the ingest and sql legs had to deduce or already know the (item, ticket) half. The sql control
  spent its probes on exactly this question.
- **The catalog clause is garbled.** "customer sk matched to catalog sales billing_sk and item_sk"
  reads as customer-sk matched to item-sk; the intent is (returning customer, returned item)
  matched to catalog (billing customer, item). It also leaks physical column spellings (`sk`,
  `billing_sk`) while failing to convey the actual join semantics.

Proposed rewording (business intent, grain and identifiers explicit, no physical column names):

> "Customers bought items in stores during September 1999 and returned those same purchases (the
> same customer returning the same item from the same sales ticket) between September and December
> 1999. For those returned purchases, also find catalog orders where the returning customer bought
> that same item in 1999, 2000, or 2001. Report one row per item and store, identified by the item
> business code and description and the store business code and name (not surrogate keys): total
> quantity sold on the qualifying store sales, total quantity returned on the qualifying returns,
> and total quantity ordered on the qualifying catalog orders. Order by item code, item
> description, store code, store name; limit 100 rows."

## Model/guidance gap

None found. The enriched model's pre-joined `return_*` namespace made the hard part of the question
free; the discovery error message and the batch undefined-reference reporter both actively steered
the agent to the fix. All three final answers matched the canonical
`tests/modeling/tpc_ds_duckdb/query29.{sql,preql}` result.
