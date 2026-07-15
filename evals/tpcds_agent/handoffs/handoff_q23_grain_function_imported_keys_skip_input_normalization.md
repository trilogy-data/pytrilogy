# Handoff — `count(grain(...))` skips composite input-grain normalization for imported dimension keys

**Status:** OPEN framework silent-wrong-result bug. Discovered in the enriched
TPC-DS q23 rerun `20260715-033011_enriched` (1.85M tokens, candidate 100 rows
versus 4 reference rows).

## Summary

An aggregate over a `grain(...)` expression must first normalize its input to
the expression's declared composite grain. In q23, the expression combines an
imported item key, a derived item property, and an imported date key:

```trilogy
count(grain(
    ss.item.id,
    substring(ss.item.desc, 1, 30),
    ss.date.sk
))
```

The planner instead renders the hash directly over physical `store_sales` rows.
Repeated fact rows with the same `(item id, description prefix, date sk)` are
therefore counted repeatedly. This violates Trilogy's aggregate-input grain
contract and the behavior already asserted for direct composite keys in
`tests/engine/test_grain_function.py`.

This is a silent bug: the SQL executes successfully and returns plausible but
incorrect results.

## Artifacts

- Run: `evals/tpcds_agent/results/20260715-033011_enriched`
- Candidate: `workspace/query23.preql`
- Trajectory: `agent_log.q23.{jsonl,conversation.txt}`
- Canonical Trilogy: `tests/modeling/tpc_ds_duckdb/query23.preql`
- Canonical SQL: `tests/modeling/tpc_ds_duckdb/query23.sql`

## Reproduction

The verified reproduction uses the archived run workspace. `raw.store_sales`
is the namespace staged inside that workspace; do not paste this import into a
different model layout and assume it is equivalent.

Run this exact harness from the repository root with the local venv:

```powershell
@'
import sys
from pathlib import Path
sys.path.insert(0, "evals")
from common import scoring

ws = Path("evals/tpcds_agent/results/20260715-033011_enriched/workspace")
engine = scoring.make_scoring_engine(ws / "tpcds.duckdb", ws, "tpcds")
query = """import raw.store_sales as ss;
rowset frequent_items <-
where ss.date.year in (2000, 2001, 2002, 2003)
select
    ss.item.sk as item_sk,
    count(grain(
        ss.item.id,
        substring(ss.item.desc, 1, 30),
        ss.date.sk
    )) as triple_count
having triple_count > 4
;
select count(frequent_items.item_sk) as cnt;
"""
sql = engine.generate_sql(query)[-1]
print(sql)
print(engine.execute_raw_sql(sql).fetchall())
'@ | .venv\Scripts\python.exe -
```

This was rerun on 2026-07-15 against the current checkout. It printed the SQL
shape below and returned `[(15000,)]`.

The Trilogy body alone is:

```trilogy
import raw.store_sales as ss;

rowset frequent_items <-
where ss.date.year in (2000, 2001, 2002, 2003)
select
    ss.item.sk as item_sk,
    count(grain(
        ss.item.id,
        substring(ss.item.desc, 1, 30),
        ss.date.sk
    )) as triple_count
having triple_count > 4
;

select count(frequent_items.item_sk) as cnt;
```

Observed result at scale factor 1:

```text
15000
```

## Actual SQL shape

The generated aggregate has no intermediate normalization to the composite
argument grain:

```sql
SELECT
    item.I_ITEM_SK
FROM store_sales
JOIN date_dim ON ...
JOIN item ON ...
GROUP BY item.I_ITEM_SK
HAVING count(md5(concat_ws(
    separator,
    item.I_ITEM_ID,
    substring(item.I_ITEM_DESC, 1, 30),
    date_dim.D_DATE_SK
))) > 4
```

Consequently, `count(grain(...))` is evaluated once per physical sale row, not
once per row at the expression's declared grain.

## Expected planning behavior

The `grain(...)` expression is a property at:

```text
(ss.item.id, substring(ss.item.desc, 1, 30), ss.date.sk)
```

Before a coarser aggregate consumes that property, its source must be normalized
to that grain, equivalent to:

```sql
WITH composite_rows AS (
  SELECT item_id, description_prefix, date_sk, composite_hash
  FROM ...
  GROUP BY item_id, description_prefix, date_sk, composite_hash
)
SELECT item_sk, count(composite_hash)
FROM composite_rows
GROUP BY item_sk
```

The exact SQL can vary, but duplicate physical rows at the same declared
composite grain must not inflate the outer count.

## Existing passing control

`tests/engine/test_grain_function.py::test_grain_over_coarser_keys_dedupes_to_the_tuple`
already establishes the contract for direct keys:

```trilogy
select count(grain(item_id, store_id)) as c;
```

Seven physical rows containing four `(item_id, store_id)` tuples correctly
return `4`, and the test asserts that the tuple arguments survive into a
`GROUP BY`. The q23 shape should behave consistently.

## Trigger matrix to pin down

| Shape | Current behavior |
|---|---|
| Direct local keys: `grain(item_id, store_id)` | Correctly normalizes/dedupes |
| Imported role keys: `grain(ss.item.id, ss.date.sk)` | Suspected failing path |
| Imported key + derived property + imported key (q23) | **Fails; counts physical fact rows** |
| Same q23 expression materialized as an `auto` before `count` | Test whether materialization restores normalization |
| `count_distinct(grain(...))` | Compare as a control; must not mask the `count` planning bug |
| Explicit `by` at the composite grain | Compare with inferred aggregate-input grain |

Reduce this matrix to identify whether the discriminator is imported-role
lineage, the derived `substring`, or their combination.

## Likely fix area

Start with aggregate input-grain calculation and source planning:

- `trilogy/core/processing/v4_helper/concept_graph.py` (`_aggregate_input_grain`)
- aggregate source/node generation under `trilogy/core/processing/`
- preservation of function argument grain through imported-role and derived
  expression lineage

The function renderer is probably not the primary bug: rendering the composite
hash is correct. The missing step is the source normalization/grouping that
must occur before the outer `count` consumes it.

## Required regression coverage

Add a focused engine test with:

1. a fact datasource whose physical grain is finer than two imported dimension
   keys;
2. duplicate physical fact rows sharing the same imported-key tuple;
3. a derived property expression included in `grain(...)`;
4. `count(grain(...))` at a coarser output grain;
5. assertions for both the result and an intermediate/generated `GROUP BY` that
   proves normalization happened.

Keep the existing direct-key tests as controls. The regression must fail if the
planner merely renders `count(hash(...))` over the physical fact scan.

## Separate q23 wording issue

Do not conflate this framework defect with q23's business wording. Even with
correct normalization, counting composite tuples per item is not the same as
the canonical metric, which counts orders **within** each
`(item, description-prefix, sold-date)` group. The prompt will be corrected
separately after this framework bug handoff.
