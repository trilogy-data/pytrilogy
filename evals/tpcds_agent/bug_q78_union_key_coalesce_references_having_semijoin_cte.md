# q78 — union-join output-key coalesce references the HAVING semijoin CTE (Binder "Referenced table not found")

**STATUS: FIXED 2026-07-20.** `safe_get_cte_value` now restricts multi-source
renders to the CTE's FROM scope (`CTE.from_scope_aliases()` = base + join
participants) in both leak sites: the `outer_join_key_class` coalesce (the
Binder error below) and the plain multi-source coalesce (which also leaked the
semijoin CTE into the EXISTS correlation operand as a valid-but-tautological
self-reference, silently dropping that key from the correlation). Guard:
`tests/join_matrix/test_union_key_having_semijoin.py`; min3e verified against
the run workspace matching a hand-written reference 510/510 rows.

- **Run:** `evals/tpcds_agent/results/20260720-140600` (INGEST leg, workspace model `raw/*.preql`)
- **Query:** q78 — store/catalog/web *never-returned* sales, ratio of store qty to (catalog+web) qty, grouped by year/item/customer.
- **Token cost:** 4,195,841 tokens (worst sink in the run; +792k vs 3.4M baseline). Scored **PASS** — silent friction: the engine kept emitting invalid SQL for the natural spelling, and the agent only escaped by rewriting the HAVING to dodge the bug.
- **Class:** FRAMEWORK bug, LOUD (Binder error from generated SQL) — but presents as silent churn because the agent works around it.

## Symptom

The agent's natural spelling of the HAVING clause produced invalid SQL:

```
(_duckdb.BinderException) Binder Error: Referenced table "waggish" not found!
Candidate tables: "premium"
LINE 233: ...,"uneven"."cat_nr_cust_sk","waggish"."cat_nr_cust_sk","waggish"."store_nr_cust_sk"...
```

The final `SELECT` projects the union-join key `customer_sk` as:

```sql
coalesce("premium"."store_nr_cust_sk","premium"."web_nr_cust_sk",
         "uneven"."cat_nr_cust_sk",
         "waggish"."cat_nr_cust_sk","waggish"."store_nr_cust_sk") as "customer_sk"
```

but the outer query's FROM is only `premium LEFT OUTER JOIN uneven`; `waggish` is wired in
**only** through a HAVING semijoin `... WHERE ... exists (select 1 from waggish where ...)`.
Referencing `waggish.<col>` in the SELECT/GROUP BY is therefore an out-of-scope table ref.

## What made the agent burn 4.2M tokens

`waggish` is the **post-aggregation semijoin CTE** that Trilogy builds when HAVING references a
concept **not in the SELECT outputs**. The agent's working spelling was:

```
having
    coalesce(cat_nr.cat_qty, 0) + coalesce(web_nr.web_qty, 0) > 0
    and store_nr.is_store is not null      -- is_store = `1 as is_store`, NOT projected
```

`store_nr.is_store` is a non-output concept → semijoin CTE → invalid SQL. The agent thrashed,
and only PASSed after rewriting HAVING to reference an **output** column instead
(`store_nr.store_qty is not null`), which needs no semijoin. The saved
`workspace/query78.preql` is that workaround.

## Minimal repro (against the run workspace, `.venv/Scripts/python.exe`)

`scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds')`, `ws = <run>/workspace/_worker_0`.
`min3e.preql` (three arms, 3-col composite union key, one cross-arm ratio output, HAVING on a
non-output concept) reproduces:

```trilogy
import raw.store_sales as ss; import raw.catalog_sales as cs; import raw.web_sales as ws;
with store_g as where ss.date_dim.year = 2000
select ss.date_dim.year as yr, ss.item.item_sk as item_sk,
       ss.customer.customer_sk as cust_sk, sum(ss.quantity) as store_qty, 1 as is_store;
with cat_g as where cs.sold_date.year = 2000
select cs.sold_date.year as yr, cs.item.item_sk as item_sk,
       cs.bill_customer.customer_sk as cust_sk, sum(cs.quantity) as cat_qty;
with web_g as where ws.sold_date.year = 2000
select ws.sold_date.year as yr, ws.item.item_sk as item_sk,
       ws.bill_customer.customer_sk as cust_sk, sum(ws.quantity) as web_qty;
union join store_g.yr = cat_g.yr    union join store_g.item_sk = cat_g.item_sk
union join store_g.cust_sk = cat_g.cust_sk
union join store_g.yr = web_g.yr    union join store_g.item_sk = web_g.item_sk
union join store_g.cust_sk = web_g.cust_sk
select store_g.yr as year, store_g.item_sk as item_sk, store_g.cust_sk as customer_sk,
       store_g.store_qty as store_qty,
       coalesce(cat_g.cat_qty, 0) + coalesce(web_g.web_qty, 0) as other_qty,
       round(store_g.store_qty::numeric
             / nullif(coalesce(cat_g.cat_qty,0)+coalesce(web_g.web_qty,0),0)::numeric, 2) as ratio
having coalesce(cat_g.cat_qty, 0) + coalesce(web_g.web_qty, 0) > 0
       and store_g.is_store is not null;
```
→ `BinderException: Referenced table "kaput" not found!` (alias randomized per run).

## Trigger matrix (one ingredient toggled at a time)

| Variant | HAVING refs non-output? (semijoin) | cross-arm `ratio` output | Result |
|---|---|---|---|
| `fail.preql` (agent's spelling) | yes (`is_store`) | yes | **FAIL** Binder |
| `workspace/query78.preql` (saved workaround) | no (`store_qty` is an output) | yes | PASS |
| `varB` = fail with `is_store`→`store_qty` | no | yes | PASS |
| `varC` = fail, drop `is_store` HAVING conjunct | no | yes | PASS |
| `min3d` = min3e minus the `ratio` output | yes (`is_store`) | **no** | PASS |
| `min3e` (minimal) | yes (`is_store`) | yes | **FAIL** Binder |
| `min3` single-col key, no ratio | yes (`is_store`) | no | PASS |

Necessary-and-sufficient combination:
1. **union join with a coalesced output key** — the projected key's `outer_join_key_class` has ≥2 members (so it renders as a `coalesce(...)` across arms);
2. **HAVING references a non-output concept** → Trilogy emits a post-aggregation **semijoin CTE**;
3. **an output (the cross-arm `ratio`) that forces the semijoin CTE to also carry the union-join key concepts**. The two cleanest single-toggle pairs: `fail`↔`varB` (HAVING non-output vs output) and `min3d`↔`min3e` (with/without the `ratio` output).

## Root cause

For the failing final CTE (`divergent` in the minimal repro):

- `base_name = scrawny`; `joins = [LEFT_OUTER → cooperative]` ⇒ FROM scope = `{scrawny, cooperative}`.
- `parent_ctes = [cooperative, kaput, scrawny]` — **`kaput` (the HAVING semijoin CTE) is a parent but neither the base nor a join participant**; it is referenced only by the `exists (select 1 from kaput …)` in the WHERE.
- But `source_map["store_g.cust_sk"] = {kaput, scrawny, cooperative}` — **`kaput` leaked into `source_map` for the union-join key concept** (it is *also*, correctly, in `existence_source_map`).

The invalid ref is emitted in **`trilogy/dialect/base.py:747-764`** (`safe_get_cte_value`, the
`outer_join_key_class` coalescing branch):

```python
key_class = cte.outer_join_key_class(address)          # >1 member → coalesce path
if len(key_class) > 1:
    for member in key_class:
        for source in cte.source_map.get(member.address, []):   # <-- includes kaput
            rendered = cte.get_alias(member, source)
            renders.append(_format(source, rendered))           # emits "kaput"."store_g_cust_sk"
```

It iterates **every** `source_map` source per key-class member with no check that the source is
actually in FROM scope (the base or a join participant). The EXISTS-only semijoin source is
therefore rendered as a real table reference.

**Fix locus (do not fix here — report only):**
- Primary: `trilogy/dialect/base.py:751-759` — restrict the coalesce members to sources in the CTE's FROM scope (base + join participants), or exclude any source present in `existence_source_map` / not among `joins`/base.
- Upstream contributing cause: the post-agg HAVING semijoin registers its CTE in `source_map` for the union-join key concepts (not only `existence_source_map`). See the QueryDatasource merge in `trilogy/core/models/execute.py:1186-1224` (`final_source_map` / `final_existence_source_map` assembly) and the HAVING-semijoin attachment that seeds those key entries. An existence-only source should not be a plain `source_map` provider for an output key.

## Repro assets (scratchpad, not committed)
`q78_repro.py` (generate+execute), `q78_inspect2.py` (dumps base_name/joins/parent_ctes/source_map),
`fail.preql`, `min3d.preql`, `min3e.preql`, `varB/varC/t2`.
