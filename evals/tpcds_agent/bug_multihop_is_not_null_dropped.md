# Engine bug handoff: an `IS NULL` / `IS NOT NULL` predicate on a MULTI-HOP dimension path is silently dropped from the generated SQL

## Summary

A `where <fact>.<dim>.<subdim>.<field> is (not) null` predicate — i.e. a null
check on a **2+-hop chained dimension path** — is **silently omitted** from the
generated SQL. No error, no warning; the filter just doesn't happen, so rows that
should be excluded leak into the result. Equality / comparison predicates on the
**same** path are emitted correctly, and a null check on a **1-hop** path is
emitted correctly — so this is specific to null checks on chained paths.

This is a silent-wrong-results bug. It's why q06's "only customers with a current
home address on record" guard (`billing_customer.address.id is not null`) leaks
~66 anonymous-sale rows into the NULL-state bucket (NULL state count 137 vs
reference 71), failing the query with no error.

## Minimal repro + scope (verified against `results/20260601-175357_enriched/workspace`)

Render SQL with `engine.generate_sql(body)` and inspect for the predicate:

| predicate in WHERE | in generated SQL? |
|---|---|
| `ps.item.id is not null` (1-hop dim path) | ✅ emits `I_ITEM_SK is not null` |
| `ps.billing_customer.id is not null` (1-hop) | ✅ emits `SS_CUSTOMER_SK is not null` (+ join promotion) |
| `ps.billing_customer.address.id is not null` (2-hop) | ❌ **dropped** |
| `ps.billing_customer.address.country is not null` (2-hop, non-key) | ❌ **dropped** |
| `ps.billing_customer.address.country = 'United States'` (2-hop EQUALITY) | ✅ emits `CA_COUNTRY = 'United States'` |

So: **null checks on a 2+-hop path are dropped; equality/comparison on the same
path is kept; null checks on a 1-hop path are kept.** (`ps` = `import
raw.physical_sales as ps`; the enriched model — `billing_customer` →
`address` is the chained customer→customer_address FK.)

```
import raw.physical_sales as ps;
select ps.billing_customer.address.state, count(ps.ticket_number) as n
where ps.billing_customer.address.country is not null;
-- generated SQL contains NO predicate on CA_COUNTRY → filter silently lost
```

## Where to look

- The WHERE-clause lowering / predicate-pushdown path for an `is null` /
  `is not null` comparison whose operand is reached through ≥2 join hops. A
  single-hop null check survives and an equality on the multi-hop path survives,
  so the drop is specific to how the null-comparison node is routed/pushed when
  its concept sits behind a chained join. Likely the predicate is being assigned
  to a join level it then gets pruned from, or the null-comparison isn't being
  recognized as a filter to retain on the multi-hop source.
- Compare the node graph / pushdown decisions for the equality vs the
  is-not-null on the identical 2-hop path — the equality is retained, the null
  check vanishes.

## Suggested fix / regression

- A null check on any path must be preserved as a filter (pushed to the correct
  CTE, promoting the join as needed), exactly as equality is. Or, if a path
  genuinely can't carry the predicate, raise — never silently drop a WHERE term.
- Regression: assert the generated SQL for the 2-hop `is not null` repro contains
  a corresponding `IS NOT NULL`, and that the executed row set matches a manual
  filter.

## Affected eval queries

- **q06 / enriched** (`20260601-175357_enriched`, 0/10): the
  `billing_customer.address.id is not null` guard is dropped, so anonymous sales
  (`SS_CUSTOMER_SK` NULL) leak into the NULL-state group (137 vs 71). The
  canonical `query06.preql` avoids it only because it *also* writes the 1-hop
  `billing_customer.id is not null`, which IS honored and incidentally excludes
  the anonymous rows. Likely contributes to other "where X is recorded" guards on
  chained paths across the suite.

## Notes

- Observed on `trilogy 0.3.275`.
- Distinct from the other open handoffs (`bug_filtered_aggregate_in_inline_filter.md`,
  `bug_aggregate_render_derived_in_by_grain.md`, `bug_merge_in_subselect_missing_cte.md`,
  `bug_select_alias_shadows_auto_concept.md`, `recursion_bug_handoff.md`).
