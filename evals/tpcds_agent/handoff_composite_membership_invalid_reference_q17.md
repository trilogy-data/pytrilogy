# Handoff: composite membership across two facts leaks `INVALID_REFERENCE_BUG` (q17)

**Status: confirmed bad error surface + probable capability gap. From
`results/20260810-211903_enriched_aggregates/agent_log.q17.jsonl`.**

## What happened

The agent tested tuple membership whose right side spans two concepts of a
*different* fact model, inside an inline-filtered aggregate:

```trilogy
import raw.store_sales as ss;
import raw.catalog_sales as cs;

auto cat_in_st <- count(grain(ss.ticket_number, ss.item.sk)
    ? ss.sale_date.year = 2001
      and (ss.customer.sk, ss.item.sk) in (cs.billing_customer.sk, cs.item.sk)
      and cs.sale_date.year in (2001, 2002)
      and cs.billing_customer.sk is not null) by *;

select cat_in_st as match_out;
```

Result (exit 1):

```
Unexpected error in probe6.preql: composite membership right-hand operands
must resolve to a single existence source, got
['INVALID_REFERENCE_BUG<Missing source reference to cs.billing_customer.sk>',
 'dim_item as cs_item_items']
```

Two distinct defects:

1. **Error-quality bug (fix regardless):** an internal placeholder
   (`INVALID_REFERENCE_BUG<...>`) and a physical alias (`dim_item as
   cs_item_items`) leak into a user-facing message. The message also states the
   constraint without the remedy. It should say something like: "the right side
   of a tuple membership must come from ONE model or rowset; stage the
   cross-model pair through a rowset first (`with pairs as select
   cs.billing_customer.sk as c, cs.item.sk as i;` then `(a, b) in (pairs.c,
   pairs.i)`), or use the staged-membership example."
2. **Capability question:** both right-side concepts *do* come from one model
   (`cs`), yet resolution split them across two sources (one of which failed to
   resolve at all). Determine whether same-model tuple membership inside an
   inline-filtered aggregate is supposed to work; if yes this is a resolution
   bug, if no the parser should reject it with the remedy above rather than
   failing mid-planning as "Unexpected error".

Note the aggregate references a mix of `ss.*` and `cs.*` with no scoped join in
scope — the condition crosses two unjoined facts, which may be the actual
root cause of the split.

## Follow-ups landed elsewhere

The language reference (agent-info query) now documents: "Tuple membership
`(a, b) in (m.a, m.b)` requires both right-side concepts to resolve to ONE
source (model or rowset); to test against a cross-model pair, stage it through
a rowset first." Keep the error message consistent with that wording.

## Repro guidance

Models in `tests/modeling/tpc_ds_duckdb/` (`store_sales.preql`,
`catalog_sales.preql`). Reduce first: (a) same-model tuple membership in a
plain `where` (probably works — find the passing baseline), (b) move it into
an inline-filtered aggregate, (c) add the second imported fact. The first
failing step localizes the defect. `grep -rn "INVALID_REFERENCE_BUG"
trilogy/` finds the placeholder's origin; any path that can format it into a
user-facing message is a bug on its own.
