# q05 — `union(...)` measure broadcasts channel-total onto every entity (SILENT wrong numbers)

**Status:** OPEN. Reproduced on current working tree (editable install, `pytrilogy 0.3.285`,
branch `more_join_improvements`). Same symptom appears in the eval trace
`results/20260624-133456/agent_log.q05.conversation.txt` (messages 60–66), so it is not new.

## Summary

A two-arm `union(...)` aligns an output key (`entity_id`) that is fed by BOTH arms, plus
per-arm measures (`gross_sales` lives only in arm 0, `return_amounts` only in arm 1). When the
outer query does `sum(measure) by (channel_type, entity_id)`:

- the **arm-0** measure (`gross_sales`) groups correctly **per entity**, but
- the **arm-1** measure (`return_amounts`) is summed at **channel grain** and **broadcast**
  identically onto every entity row.

Same SELECT structure for both measures, opposite behavior. The number is silently wrong — no
error, no disconnect raise.

## Minimal repro (the actual Trilogy)

Run from `evals/tpcds_agent/results/20260624-133456/workspace/` (has `trilogy.toml` + `tpcds.duckdb`):

```trilogy
import raw.all_sales as s;

with combined as union(
  (where s.date.date between '2000-08-23'::date and '2000-09-06'::date
    and s.channel_dim_id is not null
   select
     case when s.channel = 'STORE' then 'store channel'
          when s.channel = 'CATALOG' then 'catalog channel'
          when s.channel = 'WEB' then 'web channel' end as ch,
     concat(case when s.channel = 'STORE' then 'store'
                 when s.channel = 'CATALOG' then 'catalog_page'
                 when s.channel = 'WEB' then 'web_site' end,
            s.channel_dim_text_id) as ent,
     s.ext_sales_price as gs,
     0::float as ret_amt,
     s.net_profit as np,
     0::float as rnl
  ),
  (where s.return_date.date between '2000-08-23'::date and '2000-09-06'::date
    and s.return_channel_dim_id is not null
   select
     case when s.channel = 'STORE' then 'store channel'
          when s.channel = 'CATALOG' then 'catalog channel'
          when s.channel = 'WEB' then 'web channel' end as ch,
     concat(case when s.channel = 'STORE' then 'store'
                 when s.channel = 'CATALOG' then 'catalog_page'
                 when s.channel = 'WEB' then 'web_site' end,
            s.return_channel_dim_text_id) as ent,
     0::float as gs,
     s.return_amount as ret_amt,
     0::float as np,
     s.return_net_loss as rnl
  )
) -> (channel_type, entity_id, gross_sales, return_amounts, net_profit, return_net_losses);

select
  combined.channel_type,
  combined.entity_id,
  sum(combined.gross_sales)      as total_gross_sales,
  sum(combined.return_amounts)   as total_returns,
  sum(combined.net_profit)       as total_profit,
  sum(combined.return_net_losses) as total_ret_loss
limit 15;
```

## Buggy output

`total_gross_sales` / `total_profit` (arm 0) vary per entity — CORRECT.
`total_returns` / `total_ret_loss` (arm 1) are the SAME value on every catalog row — WRONG:

```
channel          entity                       total_gross_sales  total_returns   total_profit  total_ret_loss
catalog channel  catalog_page<...A>           245522.45          1093748.93      -6595.61      626986.60
catalog channel  catalog_page<...B>           80553.48           1093748.93      -5243.83      626986.60
catalog channel  catalog_page<...C>           98032.10           1093748.93      -11004.92     626986.60
catalog channel  catalog_page<...D>           0.0                1093748.93      0.0           626986.60
catalog channel  catalog_page<...E>           0.0                1093748.93      0.0           626986.60
...              ...                          ...                1093748.93      ...           626986.60
```

## Why this is a bug, not defensible union semantics

1. **Ground truth varies per entity.** Querying returns directly at entity grain (trace msgs
   43/47/57) gives distinct per-page values, and the agent's eventual working full-outer-join
   version (msg 121) produces per-entity returns (`4645.17`, `326.05`, `410.56`, …). The SELECT
   explicitly groups by `entity_id`; a constant is not the intended answer.

2. **The two arms are structurally identical but behave asymmetrically.** Both arms feed
   `entity_id`; each contributes one measure. If "broadcast a rowset key as an independent
   aggregate" were the real rule, `gross_sales` (also just a key) would broadcast too. It does
   not. The planner binds the aligned `entity_id` to arm 0's source (`channel_dim_text_id`), so
   arm 1's `return_amounts` loses correlation to `entity_id`, is summed at channel grain, and is
   joined back on channel only → broadcast.

3. **The constant is not even a clean channel total.** Broadcast value `1093748.93` vs. the
   correct catalog-channel subtotal `1083573.98` (msg 121 rollup) — ~10k off. Consistent with the
   union's every-column-is-a-key row dedup ALSO dropping duplicate return tuples. Two wrongs
   (broadcast + dedup), not a defensible grain choice.

Correct behavior: either group `return_amounts` per entity, or raise a
`DisconnectedConcepts`-style error. Silently emitting a wrong number is the worst outcome.

## Plan shape (current tree)

`trilogy run repro_union.preql --debug-file <f>` shows each measure getting its own `GroupNode`
over the shared `RowsetNode`, re-merged on `(channel_type, entity_id)`. The `return_amounts`
branch effectively collapses `entity_id` before the merge, so the re-merge pairs channel-grain
returns against entity-grain sales. The aligned `entity_id` output column of the `UnionNode` is the
suspect — it resolves to arm 0 only instead of representing both arms' contributions.

## Workaround (works today)

Don't union at line grain. Pre-aggregate each side to the entity grain in separate rowsets and
combine with a scoped `full join` + `coalesce` (agent's msg 100–121 path). That produces correct
per-entity returns and the rollup subtotals.

## Repro provenance

- venv is an editable install → reflects working tree HEAD (`pytrilogy 0.3.285`).
- Eval that surfaced it: `results/20260624-133456` (deepseek-chat), q05, msgs 60–66.
- Related memory: `project_tvf_union_aggregate_arms`, `project_tvf_union_implementation`,
  `project_rollup_after_wrapped_aggregate_grammar_friction`.

## Not-bugs seen in the same trace (for completeness)

Clear, correctly-messaged parse errors the agent hit and recovered from — leave as-is:
- ORDER BY contains aggregate not in SELECT (msg 33)
- `by` expression needs parens (msg 71)
- `rollup` rejects expression args, wants bare identifiers (msg 117)
- `group by` rejected — grouping is automatic (msg 55)

The agent also spent ~50 messages before reaching the full-join shape — an agent-guidance gap
(prefer pre-aggregate-then-full-join over line-grain union when arms carry disjoint measures),
not a planner defect.
```
