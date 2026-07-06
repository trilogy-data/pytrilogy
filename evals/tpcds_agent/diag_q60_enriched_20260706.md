# Diagnosis: q60 FAIL in enriched leg, run 20260706-135542

## Classification: AGENT error (concrete proof below). Framework and model both exonerated.

## Symptom
`score_query` → `QueryResult(id=60, status='fail', ref_rows=100, cand_rows=100, detail='result set differs from reference')`.
Candidate ran cleanly and returned 100 rows, but the wrong 100 rows in the wrong key domain.

## Agent-attempt error inventory (agent_log.q60.conversation.txt, 15 messages)
Zero errors. Every tool call returned exit_code 0:
1. `trilogy` help (m2-3)
2. `trilogy file list raw --recursive` (m4-5)
3. `trilogy explore raw/all_sales.preql` (m6-7)
4. `trilogy explore raw/item.preql --regex category` (m8-9) — regex-filtered, returned ONLY the two category concepts
5. `trilogy file write query60.preql` (m10-11) — succeeded, SQL echoed back
6. `trilogy run query60.preql` (m12-13) — succeeded, 100 rows (927 pre-limit)
7. `return_control_to_user` (m14)

One attempt, first-try success, no framework pushback at any point.

## Root cause
The question asks for the **item code** (`i_item_id`, the 16-char business code) and a semi-join on item code. The agent used the **surrogate key** `sales.item.id` (`i_item_sk`, int) for both the semi-join and the reported column:

```trilogy
auto music_item_ids <- sales.item.id ? sales.item.category = 'Music';
... and sales.item.id in music_item_ids
select sales.item.id as item_code, sum(sales.ext_sales_price) as total
```

Three compounding wrongnesses vs the reference (tests\modeling\tpc_ds_duckdb\query60.sql):
1. **Wrong output domain**: candidate rows are `(int, Decimal)` e.g. `(1, 16213.31)`; reference rows are `(str, Decimal)` e.g. `('AAAAAAAAAABBAAAA', 8522.56)`. Fails scoring unconditionally.
2. **Wrong semi-join scope**: `i_item_id IN (music codes)` covers 4017 item SKs (Music-coded items across all SCD revisions, some revisions non-Music); `i_item_sk IN (Music sks)` covers only 1860 — i.e. it degenerates to `category='Music'`, dropping the deliberate SCD-sibling inclusion the question's "item code matches the item code of any item" phrasing encodes.
3. **Wrong grouping grain**: grouping by surrogate splits one item code across SCD versions (candidate had 927 pre-limit groups on the sk grain).

## Why the model is NOT at fault
`workspace\raw\item.preql` explicitly documents the trap, in exactly the right place:
- `key id int; # Surrogate key (one row per SCD version of an item; SEVERAL ids share one text_id). Do NOT group/report per-item by this — it splits an item across versions. Use text_id.`
- `text_id string, # The item's business "item code" (i_item_id, 16-char). The typical per-item identifier: use this for per-item results, not the surrogate id. Not unique - this table is an SCD.`

The agent never read this guidance: its only look at item.preql was `explore --regex category`, and the `explore raw/all_sales.preql` output lists imported-namespace concepts (`item`: "brand_id, ... id, ... text_id") as bare names WITHOUT descriptions, so the warning was not surfaced there. (Possible tooling improvement — surface imported-concept comments in explore — but the information was one unfiltered explore away.)

## Why the framework is NOT at fault
- Canonical `tests\modeling\tpc_ds_duckdb\query60.preql` built through the same scoring engine (against a copy of `.cache\tpcds_sf1.duckdb`, with the test-harness `"memory".` schema qualifier stripped) matches query60.sql exactly: `canonical == ref multiset: True`.
- **Minimal trigger matrix (one ingredient)**: taking the agent's candidate verbatim and swapping `sales.item.id` → `sales.item.text_id` (3 occurrences) in the same enriched workspace produces SQL that returns the reference multiset exactly: `fixed candidate == ref multiset: True`. The framework compiles the identical shape (filtered-set `auto`, `in` semi-join over the all_sales union, group + order + limit) correctly.

## Why the SQL legs passed
The sql_bare agent (results\20260706-135542_sql_bare\workspace\query60.sql) read the raw schema and correctly picked `i_item_id` for "item code", with the semi-join on `i_item_id` — matching the reference. Ambient TPC-DS familiarity with raw column names ("item id" vs "item sk") made the right choice obvious there, while the enriched model's friendlier `item.id` name misled the agent into treating the surrogate as the code.

## Evidence commands
- Scoring repro: `scoring.score_query(...)` → fail; row-level diff shows int-vs-string key column.
- SK-scope counts (DB copy): Music SKs = 1860; SKs whose text_id matches a Music text_id = 4017; distinct Music codes = 1751.

## Contributing factors (not the classification)
- `trilogy explore` on a composite model omits per-concept doc comments for imported namespaces; the item-code guidance was invisible in the all_sales explore the agent relied on.
- The question's "item code matches the item code of any item in the 'Music' category" phrasing only makes sense for a non-unique code; the agent's own reasoning ("items where item.id is in the set of item ids where category='Music'") collapsed it to a tautology without noticing.
