# q84 regression (pass→fail) — silent grain collapse across a `union join` to a rowset

Status: **REFUTED 2026-07-06 — NOT A FRAMEWORK BUG.** The "OLD passing" query's
`--sr.ticket_number as ticket` / `--sr.item.id as item_id` lines are NOT comments — `--` is Trilogy's
select-item HIDE modifier. Those hidden outputs are what put the returns grain into the select grain
(and GROUP BY); with identical authored outputs the datasource and rowset forms behave identically in
both directions. 15 rows is the correct answer to the Trilogy the new agent wrote. Verdict + corrected
trigger matrix: `handoffs/handoff_q84_union_join_rowset_grain_collapse.md`; parity guards:
`tests/engine/test_duckdb_union_join_rowset_grain.py`. Original (faulty) analysis below.

Class (original, wrong): FRAMEWORK bug (silent wrong-rows), PRE-EXISTING (not caused by
`4e69c5547`). The pass→fail is agent authoring **variance** landing on the defect.

## Symptom
- New run `results/20260706-222300` q84 = FAIL, "result set differs", ref **16** rows / cand **15**, 749k tokens.
- Old run `results/20260706-135542_enriched` q84 = PASS, 654k.
- No hard error in the jsonl (grepped `Binder|Catalog|Invalid Input|Unexpected error|could not resolve|Recursion` → none). Pure silent dedup: one row is dropped.
- The dropped row is a **fan-out duplicate**: customer `AAAAAAAAAAPDAAAA` "Benson, Floyd" whose demographic matches **two** store returns (tickets 126030 and 85482). Reference keeps both (comma-join fan-out); candidate collapses to one.

## Root cause
The two runs' agents wrote **different Trilogy**, both selecting only `customer_code` + `full_name`:

- OLD (passing) — direct scoped join to the base datasource:
  `union join sr.customer_demographic.id = cust.demographics.id`
  Generated SQL final `GROUP BY 1, 2, sr_store_returns.SR_ITEM_SK, sr_store_returns.SR_TICKET_NUMBER`
  → the joined **datasource's physical grain (item, ticket) is preserved in the group** → 16 rows (fan-out kept). CORRECT, matches reference.

- NEW (failing) — wraps store_returns in a rowset first:
  ```
  with return_demos as select sr.ticket_number, sr.item.id as item_id, sr.customer_demographic.id as demo_id where ... ;
  select c.text_id ..., concat(...) as full_name
  union join return_demos.demo_id = c.demographics.id
  where ... and return_demos.ticket_number is not null ...
  ```
  Generated SQL final `GROUP BY 1, 2` only → the rowset's declared grain `(ticket_number, item_id, demo_id)` is **NOT** carried into the consuming select's grouping. `item_id` is even pruned entirely from the rowset CTE projection; `ticket_number` is kept only for the `is not null` filter. The fan-out is silently dedup'd → 15 rows.

So: **a scoped `union join` onto a rowset drops the rowset's non-referenced grain components from the final GROUP BY, whereas the same join onto a base datasource preserves that datasource's physical grain.** This asymmetry silently collapses legitimate row multiplicity.

The select-level grain is computed purely from outputs (`_calculate_grain`, `select_finalize.py:239-253` → `Grain.from_concepts(targets, ...)`), identical for both forms — grain of `(customer_code, full_name)`. The divergence is downstream in planner/merge grain handling: a joined **Datasource** contributes its physical grain keys into the final group (visible as SR_ITEM_SK/SR_TICKET_NUMBER in OLD), while a joined **rowset/CTE** (RowsetNode → CTE `abundant`) does not — its declared grain is treated as merely a set of available columns and pruned to what's referenced, then the group collapses to the output tuple. Locus: rowset grain declaration (`processing/node_generators/rowset_node.py`) + group/merge grain resolution (`node_generators/group_node.py` `target_grain`/`grain_components`, and the datasource-grain-preservation path in the join/merge planner). No single fix line; the contract to fix is "a scoped-joined rowset must contribute its declared grain to the consumer's grain the same way a datasource does."

## Trigger matrix (all on new workspace, `generate_sql` + execute)
| form | final GROUP BY | rows |
|---|---|---|
| OLD direct `union join` to store_returns, output = cust fields only | `1,2,SR_ITEM_SK,SR_TICKET_NUMBER` | **16** ✅ |
| NEW `union join` to rowset return_demos, output = cust fields only | `1,2` | **15** ✗ |
| NEW + also SELECT `return_demos.ticket_number, return_demos.item_id` | `1,2,3,4` | **16** ✅ |

Removing the rowset (direct join) OR pulling the rowset grain into the output both restore 16. The minimal failing combination = scoped join to a **rowset** whose grain columns are not in the SELECT output.

## Regression provenance — NOT `4e69c5547`
- Ran the NEW query against the parent commit `4e69c5547^` = `1c7fed75a` in a worktree → **15 rows** (same collapse). Pre-existing.
- `4e69c5547`'s engine changes (`union_node.py`, `union_select_node.py`, `union_dim_pushdown.py`, `base_node.py`) add a `set_operator` to the `union(...)`/`except(...)`/`intersect(...)` **TVF** path — unrelated to the `union join` **scoped-join** path this query uses.
- q84's prompt is **unchanged** in `4e69c5547` (only q67/q70/q73/q76/q81/q82/q87 prompts changed).
- The commit did expand rowset/set-operator guidance (`ai/syntax_examples.py` +53, `ai/constants.py`), which plausibly nudged the agent toward a rowset-first authoring — the proximate reason the pass→fail surfaced now — but the defect itself is pre-existing.

## Classification
FRAMEWORK bug (silent grain collapse across a rowset scoped join) + a guidance/authoring contributor (the rowset form is a legal way to express the intended `(customer, ticket, item)` grain but silently loses it). The direct-join idiom is the working form. Do NOT fix here.

## Repro
`evals/tpcds_agent/results/20260706-222300/workspace/query84.preql` (fails, 15) vs
`evals/tpcds_agent/results/20260706-135542_enriched/workspace/query84.preql` (passes, 16),
scored via `common.scoring.make_scoring_engine` + `generate_sql`/`execute_raw_sql` against each run's own `workspace/tpcds.duckdb`.
