# q84: subset-join side-presence filter loses the joined datasource

## RESOLUTION (2026-07-10)

Split verdict — one framework bug fixed, one expectation re-ruled:

- **FIXED (framework)**: presence probes were bespoke to ROWSET-derived
  members. A null test on a datasource-bound (ROOT) member of a subset/union
  key group silently no-op'd: the member's binding is substituted onto the
  group canonical, so the test read whichever side sourcing picked and the
  member's datasource could vanish from the plan. Probes now mint for ROOT
  members (`Factory._coalescing_presence_probe`) and materialize on a scan
  pinned to a datasource that physically carries the member's authored
  column (`gen_presence_probe_node`, side identity recovered via
  `BuildColumnAssignment.origin_address`). When the member is bound in both
  its defining dimension (PK) and a fact (FK), the FK carrier is probed —
  probing the dimension is a tautology. Matrix:
  `tests/join_matrix/test_root_presence_probe.py`.
- **RE-RULED (expected behavior below is wrong for the anchor side)**: the
  authored `subset join c.demographics.sk = ss.return_customer_demographic.sk`
  declares c ⊆ ss, making ss the trusted superset anchor. An anchor-side null
  test is a genuine no-op and pruning the unreferenced anchor is sound under
  the declaration; the wrongness is the LYING declaration, an author error per
  docs/subset_union_join_design.md (matches the earlier q84 join-prune
  NOT-A-BUG verdict). The correct authoring is the reversed direction —
  `subset join ss.return_customer_demographic.sk = c.demographics.sk` — where
  `ss.return_customer_demographic.sk is not null` is a subset-side test.
  With the fix, that form returns exactly the SQL-oracle count (104 distinct
  Edgewood customers with a matching return on the SF1 db; the reference's 16
  rows include q84's additional income-band/household predicates).
- **DISCOVERED (pre-existing, separate)**: projecting a ROOT union-relation
  key with no other side-specific output single-sources one member's table
  instead of coalescing both — pinned as strict xfails in
  `test_root_presence_probe.py`
  (`test_union_bare_axis_projection_unions_domains`).

## Classification

Framework bug: silent wrong-result SQL generation.

In a scoped `subset join`, an explicit `is not null` predicate on the subset
side is intended to require that side to be present. Instead, the planner
canonicalizes the subset key to the preserved-side key before retaining a
side-specific presence probe. The subset datasource can then disappear from
the generated SQL entirely.

This caused enriched TPC-DS q84 to return 204 rows (limited to 100) instead of
the reference's 16 rows.

## Original q84 shape

```preql
import store_returns as sr;
import customer as c;

where
    c.address.city = 'Edgewood'
    and sr.customer_demographic.sk is not null
    and c.demographics.sk is not null
select
    c.id
    subset join c.demographics.sk = sr.customer_demographic.sk
limit 100;
```

The predicates explicitly require both join-key members. In particular,
`sr.customer_demographic.sk is not null` should eliminate preserved customer
rows without a matching return-side row. It does not.

## Minimal current-model reproduction

The standalone `store_returns` model has since been folded into `store_sales`,
so the equivalent current-model reproduction is:

```preql
import customer as c;
import store_sales as ss;

where
    c.address.city = 'Edgewood'
    and ss.return_customer_demographic.sk is not null
    and c.demographics.sk is not null
select
    c.id
    subset join c.demographics.sk = ss.return_customer_demographic.sk
limit 10;
```

Generate it against the q84 SF1 database with:

```python
import sys
from pathlib import Path

sys.path.insert(0, "evals")
from common import scoring

model = Path("tests/modeling/tpc_ds_duckdb").resolve()
db = Path(
    "evals/tpcds_agent/results/20260709-105517_enriched/"
    "workspace/tpcds.duckdb"
).resolve()
engine = scoring.make_scoring_engine(db, model, "tpcds")
print(engine.generate_sql(body)[-1])
```

## Generated SQL symptom

The generated SQL contains `customer` and `customer_address`, but no
`store_sales` or store-return input. Its inner CTE projects the customer key
under the return-side alias:

```sql
SELECT
    c_customers.C_CURRENT_ADDR_SK AS c_address_sk,
    c_customers.C_CURRENT_CDEMO_SK AS ss_return_customer_demographic_sk,
    c_customers.C_CUSTOMER_ID AS c_id
FROM customer AS c_customers
WHERE c_customers.C_CURRENT_CDEMO_SK IS NOT NULL
```

The outer predicate then checks the substituted/coalesced customer value:

```sql
coalesce(
    highfalutin.ss_return_customer_demographic_sk,
    ss_return_customer_demographic_customer_demographics.CD_DEMO_SK
) IS NOT NULL
```

Thus the authored return-side presence test is satisfied by the customer side.
The SQL cannot possibly enforce that a matching store return exists because the
fact datasource is absent.

## Trigger matrix

| Shape | Result |
|---|---|
| `subset join c.demo = ss.return_demo`, select customer only | Return datasource can be eliminated |
| Add `ss.return_demo is not null` | Still eliminated; predicate is rewritten to the fused/customer key |
| Add both endpoints `is not null` | Still eliminated |
| Select a non-key return fact such as ticket/item | Keeps the return datasource (workaround, but changes output grain/schema) |
| Use SQL equality/EXISTS | Correctly requires a matching return row |

The important trigger is a filter-only subset-side join key whose datasource
has no other selected output.

## Expected behavior

Relation declarations are row-preserving by default, but the documented escape
hatch is an explicit side-specific null predicate. Therefore
`ss.return_customer_demographic.sk is not null` must observe whether the
return-side row matched. It must not be rewritten to the fused join key or be
satisfied by the preserved customer-side value.

The planner should retain a side-specific passthrough/presence probe and keep
the return datasource in the plan.

## Likely root cause

Scoped `subset join` is normalized to a superset-anchored left relation in
`trilogy/parsing/v2/rules/select_statement_rules.py:261-277`.

The builder already has the correct conceptual repair in
`trilogy/core/models/build.py:3364`: `_coalescing_presence_probe` exists because
a member-level null test must not read the fused/coalesced join key. However,
at `trilogy/core/models/build.py:3390` it refuses to create a probe unless the
member's derivation is `ROWSET`:

```python
if member is None or member.derivation != Derivation.ROWSET:
    return None
```

In q84 the subset endpoint comes from an imported/root datasource concept, not
a rowset-derived concept. No side-specific probe is minted, canonical
substitution aliases the customer demographic key as the return demographic
key, and normal source pruning removes the return fact.

The fix likely belongs in this presence-probe path: support scoped coalescing
endpoints backed by root/imported datasources, while ensuring the passthrough is
materialized from the endpoint's own datasource before canonical merging.

## Regression test recommendation

Add a small two-datasource test where:

- the preserved table has keys `{1, 2}`;
- the subset table has only key `{1}`;
- the query selects only a preserved-side label;
- it authors `subset join preserved.k = subset.k`;
- it filters `subset.k is not null`.

Expected output is only key `1`, and generated SQL must reference both
datasources. Also pin the no-filter form to `{1, 2}` so the test distinguishes
the documented row-preserving default from explicit intersection semantics.
