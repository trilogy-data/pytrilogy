# Bug: q02 v4 renders an `INVALID_ALIAS` sentinel → invalid SQL (empty identifier)

Status: OPEN, pre-existing (NOT caused by the 2026-06-25 size fixes — reproduces
with both reverted). Masked in the suite because `test_two` is xfail-listed
(`_TPCDS_SIZE`), so the failure currently looks like a size entry.

## Symptom

Under v4 (`TRILOGY_V4_DISCOVERY=1`), generating/executing TPC-DS q02 fails:

```
_duckdb.ParserException: Parser Error: zero-length delimited identifier at or near """"
```

Repro:

```bash
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python -m pytest \
  "tests/modeling/tpc_ds_duckdb/test_queries.py::test_two" --runxfail -q
```

Or generation-only: `engine.generate_sql(query02.preql)` and grep the output for
`INVALID_ALIAS` / `""`.

## Root cause

The generated SQL contains a **rendered error string used as a column reference**.
In the union scan CTE (`cheerful` = `catalog_sales UNION ALL web_sales`), the WHERE
that applies `sales.date.week_seq in relevent_week_seq` renders as:

```sql
WHERE "questionable"."INVALID_ALIAS: [MODELS_EXECUTE] Concept sales.date.week_seq@Grain<sales.date.id>
       not found on sales.catalog_sales_unified_join_sales...; have [...week_seq@Grain<sales.channel,sales.item.id,sales.order_id>...]"."" in (...)
```

The embedded `"` in that sentinel produce the `""` zero-length identifier the parser
rejects.

Two distinct defects stacked:

1. **Planner (the real bug).** The virtual-filter condition `week_seq in
   relevent_week_seq` is placed on the **union-member scan** (`cheerful`), where the
   source map only exposes `sales.date.week_seq@Grain<sales.channel,sales.item.id,
   sales.order_id>` (the fact's row grain). The lookup asks for
   `sales.date.week_seq@Grain<sales.date.id>` (date grain) → grain-keyed address
   mismatch → not found. Note the same filter is **also applied correctly downstream**
   in the `cooperative` CTE (`"sales_date_date"."D_WEEK_SEQ" in (select ... )`), so the
   placement on `cheerful` is both **wrong and redundant** — the condition should not
   be injected at the union scan at all (or should be injected at the grain where
   week_seq is resolvable, i.e. after the `date_dim` join). Likely a
   condition-placement / grain-resolution issue specific to:
   union sources (`all_sales` = web+catalog) + a virtual filter (`week_seq ?
   year in (2001,2002)` → `relevent_week_seq`) whose membership RHS is itself a
   subselect.

2. **Renderer (masks the bug).** `trilogy/core/models/execute.py:420-423`:

   ```python
   try:
       return self.source.get_alias(concept, source=source)
   except ValueError as e:
       return f"INVALID_ALIAS: {str(e)}"
   ```

   A source-map miss (a genuine planner error: "Concept ... not found on ...") is
   **swallowed and emitted into the SQL** as the alias, so the failure surfaces as a
   confusing DuckDB parse error rather than a clear planner exception. This should
   raise (or at least propagate a typed error) so planner gaps fail loudly at build
   time — see the repo's "no belt-and-suspenders / fail at source" guidance. Fixing
   this alone turns the silent invalid-SQL into an actionable exception, which is
   worth doing independently of (1).

## Where to look

- Condition placement / injection for union scans + virtual-filter membership:
  `trilogy/core/processing/v4_helper/condition_placement.py` and the
  `condition_injection` path; check why the `week_seq in relevent_week_seq` atom lands
  on the union member scan instead of (or in addition to) the post-`date_dim`-join
  group (`cooperative`).
- Grain-keyed concept resolution on a union member: `get_alias` /
  `BuildDatasource.get_alias` (`build.py:2111`) and `execute.py:420`.
- Renderer robustness: `execute.py:423` (the `INVALID_ALIAS` fallback).

## Acceptance

- `test_two` under v4 no longer raises a `ParserException`; q02 generates valid SQL
  with rows matching the reference (then it reverts to a pure `_TPCDS_SIZE` entry, if
  still over ceiling).
- No `INVALID_ALIAS` substring can appear in any generated SQL (the renderer raises
  instead).
- Full v4 sweep stays at 0 failed.
