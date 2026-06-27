# Bug: property sourced via a COARSE shared-key bridge instead of routing through the fine-grained intermediate fact → 77M-row fan-out

**Status:** FIXED 2026-06-26 — see "Resolution" below. Was: deterministic repro + measured
cardinality below. HIGH severity (77-million-row explosion + wrong results from a query whose correct
answer is ~6.5K rows).

## Resolution (2026-06-26)

Root cause confirmed: the minimal-tree source resolver (`resolve_weak_components` →
`determine_induced_minimal_nodes`, `node_merge_node.py`) connects the requested concepts with a
minimum-weight Steiner tree, then `reinject_common_join_keys_v2` reads join keys off whatever columns
each adjacent datasource pair shares. `return_channel_dim_text_id`'s key `return_channel_dim_id` is NOT
on the sales anchor — it only lives on the returns facts — so routing through the returns fact (join on
`item.id, order_id, channel`) costs more than the cheap `channel`-only bridge between the return dim and
the anchor. The resolver took the shortcut; `return_channel_dim_id` never entered the tree, the returns
facts were pruned, and the dim collapsed to `(channel, text)` → join on `channel` (3 non-unique values
on both sides) → 77M cross product. (`channel_dim_text_id` routes correctly because its key
`channel_dim_id` is directly on the sales fact.)

Proof: adding `return_channel_dim_id` to the SELECT (forcing it into the mandatory set) made the planner
route through the returns fact and drop the fan-out (13,807 rows).

Fix: `inject_property_key_terminals` in `node_merge_node.py` forces a requested property's key into the
resolution as a mandatory Steiner terminal — but ONLY when that key is itself a foreign key bound at a
finer grain (`key.keys` non-empty) and materialized. `return_channel_dim_id.keys = {item.id, order_id,
channel}` qualifies → routed through the returns fact. Base entity keys (`launch_tag`, `customer.id`;
`keys = None`) sit directly on the anchor, so they are NOT forced — that narrowing avoids perturbing
unrelated plans (an early broad version split the gcat aggregate into two branches + a FULL join). After
the fix the BOTH query returns 13,792 rows and the join is on `(channel, item, order_id)` through the
returns fact. The join keys (order_id, item.id) are discovered automatically by
`reinject_common_join_keys_v2` once the returns fact is in the tree.

---
**Surfaced by:** the q5/q80 channel-entity logic (`coalesce(channel_dim_text_id,
return_channel_dim_text_id)`). (NOT a `!=` / nullable-FK / pseudonym issue — those were ruled out; see
"How we got here".)

## Symptom (measured against the real all_sales workspace)

| query | rows |
|---|---|
| `select s.row_one, s.channel_dim_text_id` | **6,589** |
| `select s.row_one, s.return_channel_dim_text_id` | **11,718** |
| `select s.row_one, s.channel_dim_text_id, s.return_channel_dim_text_id` (BOTH) | **77,209,902** |

## Root cause: a coarse-key bridge was chosen over the available fine-grained route

The model gives every sales AND returns datasource the grain `(item.id, order_id, channel)`:

```
key order_id int;
datasource ...store_sales   (SS_TICKET_NUMBER: order_id, SS_STORE_SK: ?channel_dim_id)        grain (item.id, order_id, channel)
datasource ...store_returns (SR_TICKET_NUMBER: ~order_id, SR_STORE_SK: ?return_channel_dim_id) grain (item.id, order_id, channel)
# dims (store/catalog_page/web_site) are bound twice — once per dim key:
datasource ...store (S_STORE_SK: channel_dim_id,        S_STORE_ID: channel_dim_text_id)        grain (channel_dim_id, channel)
datasource ...store (S_STORE_SK: return_channel_dim_id, S_STORE_ID: return_channel_dim_text_id) grain (return_channel_dim_id, channel)
```

`return_channel_dim_text_id` is a property of `return_channel_dim_id`, and `return_channel_dim_id` is
provided **at the fact grain `(item.id, order_id, channel)`** by the RETURNS datasources
(`SR_STORE_SK: ?return_channel_dim_id`, etc.). So the correct path to put a return's outlet text on a
sales row is:

```
sales fact (item.id, order_id, channel)
  LEFT JOIN returns fact ON (item.id, order_id, channel)   -- yields return_channel_dim_id at row grain
  LEFT JOIN dim          ON return_channel_dim_id          -- yields return_channel_dim_text_id
```

That is row-level (NULL return-text for a non-returned sale) and does NOT fan out.

**What the planner actually did** (verified from the generated SQL):
- Tables used: `store_sales`, `catalog_sales`, `web_sales` (sales) + `store`, `catalog_page`,
  `web_site` (dims).
- Tables NOT used: **`store_returns`, `catalog_returns`, `web_returns` — entirely absent.**
- `order_id` / `item` are **never used as join keys**.
- It sourced `return_channel_dim_text_id` straight from the dim and bridged it to the sales fact on
  the **only key they share — `channel`** (3 distinct values):

```sql
FROM "concerned"                                  -- sales: s_channel, s_channel_dim_id, s_row_one
INNER JOIN "thoughtful" ON "concerned"."s_channel" = "thoughtful"."s_channel"   -- return text, channel-ONLY bridge
LEFT OUTER JOIN "uneven" ON "concerned"."s_channel" = "uneven"."s_channel"
                        AND "concerned"."s_channel_dim_id" = "uneven"."s_channel_dim_id"  -- channel text, correct
```

`thoughtful` (`SELECT channel, return_text FROM dims GROUP BY channel, return_text`) carries no
id key, so the bridge collapses to `channel` → cross product within each channel → 77M rows.

So the planner skipped the intermediate **returns fact** (which shares the fine
`(item.id, order_id, channel)` grain with the anchor) and instead bridged the return dim directly to
the anchor on the **coarsest shared key, `channel`**. The directly-bound sibling
(`channel_dim_text_id`, whose key IS on the sales fact) is sourced correctly.

## Likely fix area

Source/connectivity selection (discovery): when a projected property's key lives on an intermediate
datasource that shares a FINER grain with the query anchor, route through that datasource rather than
bridging the property's dimension directly to the anchor on a coarser shared key. Here the planner
had a `(item.id, order_id, channel)` route (through the returns facts) but chose a `(channel)` bridge.
Prefer the finest-grain connecting path; at minimum, do not collapse onto a non-unique shared key that
fans out. Likely the same machinery behind earlier q5 duplicate rows and q80-style fan-out.

## Bisection / scoping

- Single property (either alone) → correct, ~6.5K / 11.7K rows.
- Both → the return side bridges on `channel` → fan-out.
- Minimal model with two INDEPENDENT FKs to two SEPARATE dims (no shared coarse key, and the key on
  the fact) → correct (2 LEFT joins, all rows kept). So the trigger is specifically: a property whose
  key is reachable only via an intermediate fact, where the property's dim ALSO shares a coarse key
  (`channel`) with the anchor — the planner takes the coarse shortcut.

## How we got here (ruled out)

- `!=` does NOT promote joins: a single dim property in SELECT is correctly LEFT; `=` and `!=` behave
  identically; a WHERE comparison just excludes nulls via 3VL.
- No pseudonym/merge links the two dim texts (an earlier wrong guess). They are properties of two
  SEPARATE keys with their own datasources; the only thing connecting them is the shared `channel`
  grain component — which is exactly the coarse key the planner wrongly bridged on.

## Repro

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260627-010809/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
sql = eng.generate_sql(
    'import raw.all_sales as s; '
    'select s.row_one, s.channel_dim_text_id, s.return_channel_dim_text_id;'
)[-1]
# returns facts are absent; the return-text bridge is `s_channel = s_channel`
eng.execute_raw_sql(f'select count(*) from ({sql}) z')   # 77,209,902
```
