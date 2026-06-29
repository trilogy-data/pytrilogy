# q06 churn (~669k tokens, PASSED) — run 20260629-030015_enriched

## The obstacle

q06 = "items priced > 1.2× the average current price of *distinct* items in the
same category" + per-state line-item-count >= 10. Canonical idiom puts the
category-average comparison directly in WHERE:

```
and store_sales.item.current_price > 1.2 * avg(store_sales.item.current_price) by store_sales.item.category
```

The agent actually wrote essentially this and it **ran at message 21** (51 rows).
But the task wording — "average each item's current price once, **not weighted by
sales volume**" — made the agent (correctly, semantically) distrust the in-fact
`avg`, which averages over store_sales rows (volume-weighted). It then tried two
"average each distinct item once" reshapes, and **both hit
`cannot merge all concepts into one connected query`**:

1. **Separate import** (msg 31/33): `import raw.item as item;`
   `auto cat_avg_price <- avg(item.current_price) by item.category;` used in a
   `store_sales` WHERE.
2. **Rowset of distinct items** (msg 24/27): `WITH distinct_item_prices AS SELECT
   store_sales.item.id ...;` then `auto avg_price_by_cat <-
   avg(distinct_item_prices.item_price) by distinct_item_prices.item_cat;` in WHERE.

It eventually escaped by adding `merge distinct_items.iid into store_sales.item.id;`
(the final `workspace/query06.preql` builds and passed). The merge is the
non-obvious idiom the error never points at.

This is a recurring family in this very run: `cannot merge` also fired in
q20, q37, q56, q84, q89.

## Minimal repro (engine harness against the run workspace)

All via `eng.generate_sql(body)` on `.../workspace` (`tpcds.duckdb`):

- **A — canonical in-WHERE avg-by-category**: builds OK (this is what the agent had working).
- **B — separate `item` import + derived avg**: `DisconnectedConceptsException` —
  `2 disconnected subgraphs: {local.cat_avg_price}; {local.line_item_count, local.state, store_sales....}`.
- **C — rowset-of-distinct-items + derived avg (no merge)**: same, `{local.avg_price_by_cat}` stranded.
- **C + `merge distinct_item_prices.item_id into store_sales.item.id;`**: builds OK.
- **Direct ref control** — `import raw.item as item; select ... item.category ...`
  (no derived wrapper): disconnects too, BUT the error adds the actionable hint
  `` `item.category` is disconnected — did you mean `store_sales.item.category`? ``

Repro script: `scratchpad/repro_q06.py` (+ `repro2.py`, `repro3.py`).

## Classification: (b) correct-by-design disconnect + missing guidance

Not a resolver bug. A separately-imported `item` namespace, and a rowset's output
namespace, are genuinely independent of `store_sales` — relating them requires an
explicit `merge`/scoped-join by design. Same family as q02/q64/q14/q56 cross-import.

The friction is that the **one piece of guidance that would have unstuck the agent
is suppressed the moment the stranded concept is derived**. Compare the control
case: a *direct* reference to `item.category` yields
`did you mean store_sales.item.category?`. But once it's wrapped in
`avg(item.current_price) by item.category`, the stranded subgraph is the derived
node `{local.cat_avg_price}` (suffix `.cat_avg_price`) — which has no connected
twin — so the suggestion engine returns nothing and the message degrades to the
generic `Are you missing a join or merge statement to relate them?`. The agent
then has no pointer to either (a) chain through `store_sales.item.*` or (b) `merge`
the rowset key, and burns the tokens.

## Error site + root cause pointer (do NOT fix here)

- Message built in `trilogy/core/processing/discovery_utility.py:859`
  `format_disconnected_subgraphs_error` — generic fallback at line **897**.
- Suggestion engine `connected_equivalent_suggestions`,
  `discovery_utility.py:804`–856. The loop `for concept in group:` (line 840) only
  matches each stranded concept's **own** `.address` suffix against environment
  twins. It does **not** walk a stranded *derived* concept's lineage/sources, so
  `local.cat_avg_price` / `local.avg_price_by_cat` never resolve to their base
  `item.category` / `distinct_item_prices.item_cat`, and the `store_sales.item.*`
  twin (case B) or the `merge ... into store_sales.item.id` bridge (case C) is
  never surfaced.
- Raised from the discovery pass; reaches the CLI via
  `trilogy/scripts/common.py:639`.

**Fix direction (not implemented):** when a stranded subgraph contains only derived
concepts, descend their lineage to the base concepts and run the same
twin-suggestion on those (e.g. "`cat_avg_price` depends on the disconnected
`item.category`; did you mean `store_sales.item.category`?"), and/or, for a stranded
concept whose lineage traces to a rowset built from a fact key, suggest the concrete
`merge <rowset.key> into <fact.key>` bridge. Secondary: the task-wording trap
("not weighted by sales volume") is what pushed the agent off the working canonical
form — a syntax-guide note that `avg(x) by dim` over a fact is fact-grain (volume-
weighted) and that per-distinct-key averaging needs a rowset+merge would help.
