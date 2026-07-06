# Diagnosis: q62 FAIL in enriched leg (run 20260706-135542_enriched)

## Symptom
Scorer: `fail`, 100 ref rows vs 100 candidate rows, "result set differs from reference".
Raw-SQL legs (db-only, db+schema) passed the same question.

## Attempt / error inventory (transcript: agent_log.q62.conversation.txt)
Clean single-shot run, **zero errors**:
1. msg 2-4: `trilogy --help` style discovery.
2. msg 5-6: `trilogy explore raw/web_sales.preql` (441 concepts, incl. `days_to_ship`, `line_item`, guidance to use `count(line_item)`).
3. msg 7-8: wrote `workspace/query62.preql`.
4. msg 9-10: `trilogy run query62.preql` succeeded (100 rows).
5. msg 11: returned control.
No framework error, no retry, no wrong-result feedback loop — the agent never saw the reference.

## Candidate vs reference diff (re-scored against .cache DB copy)
Reproduced `fail` via `common.scoring.score_query`. Multiset diff of the two 100-row sets:
- **30 ref-only rows**: all with `w_substr = NULL` — e.g. `(None, 'EXPRESS', 'site_0', 266, 293, 315, 321, 0)`. Exactly 6 ship-mode types x 5 web sites = 30 groups for **one warehouse**.
- **30 cand-only rows**: prefix `'Social, royal laws m'` (warehouse sk=3) — rows that slid into the `LIMIT 100` window (with `nulls first` ordering) once the NULL-name groups were dropped.
All overlapping rows match exactly; buckets, ordering, counting (`count(ws.line_item ? ...)`), and the `ship_date.year = 2000` ≡ `d_month_seq BETWEEN 1200 AND 1211` filter are all correct.

## Root cause
Data fact (scoring DB): `warehouse` has 5 rows; **sk=5 is a recorded warehouse whose `w_warehouse_name` is NULL**:
`[(1,'Conventional childr'), (2,'Of course ot'), (3,'Social, royal laws m'), (4,'Terms overcome instr'), (5, None)]`
(`sm_type` and `web_name` contain no NULLs, so those two extra filters were harmless.)

The task says: *"only for sales where the warehouse, shipping mode, and web site are all **recorded**"* and *"Order by those three attributes, all with **nulls first**"* (the nulls-first wording is itself the hint that NULL attribute values survive the filter). "Recorded" means the FK/entity is present (`ws_warehouse_sk IS NOT NULL`, i.e. the reference's inner join). The agent instead filtered at the **attribute** level:

```
ws.warehouse.name is not null      -- candidate (wrong level)
ws.warehouse.id   is not null      -- canonical tests\modeling\tpc_ds_duckdb\query62.preql (correct)
```

This drops warehouse sk=5 (recorded, NULL name), removing the 30 NULL-prefix groups the reference keeps.

## Proof / falsification of alternatives
- **Single-edit flip**: taking the agent's own file and swapping only `.name/.type is not null` -> `.id is not null` (all three dims), regenerated SQL **passes** (multiset == reference). Everything else the framework produced — filtered-count buckets, INNER joins from `.id is not null`, year-vs-month_seq filter, null-safe ordering — is correct.
- **Canonical still passes**: `tests\modeling\tpc_ds_duckdb\query62.preql` vs `query62.sql` on the same DB copy: PASS (100 == 100). No framework regression, no scorer issue.
- **Model is not hiding anything**: workspace `raw/warehouse.preql` explicitly declares `name string?  # Warehouse display name.` — the nullability of name independent of the warehouse being recorded was visible to the agent (unlike the q60 invisible-guidance case).
- **SQL legs**: the passing `sql_bare` answer uses `ws.ws_warehouse_sk IS NOT NULL` + inner joins on the sks — key-level "recorded", matching the reference.
- **Known sibling patterns**: NOT q60 (surrogate vs text_id — no identifier is output here) and NOT q73 (implicit never-mentioned inner-join filter — here the filter IS explicitly stated in the question; the agent just applied it one level too deep). It is the *mirror* of q73: an explicit entity-presence filter mis-translated into an attribute-presence filter.

## Classification
**AGENT error** (concrete proof above). One-line cause: the agent translated "warehouse ... recorded" into `ws.warehouse.name is not null` instead of `ws.warehouse.id is not null`, excluding the recorded-but-unnamed warehouse (sk=5) whose 30 NULL-prefix groups lead the reference's nulls-first LIMIT 100.

Secondary observation (not a defect, possible hardening): the question could pre-empt this misreading by saying "recorded (the dimension reference is present; its name may still be null)" — the "nulls first" hint was present but subtle. Model and framework behaved correctly.
