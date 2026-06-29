# q80 churn (run 20260629-030015_enriched): grouping() in ORDER BY — guided friction, not a crash

**Status:** OPEN (minor gap). **Severity:** low. q80 burned ~791k tokens but **FINISHED exit 0**
(per_query id=80, 145.7s, not timed out) and the agent submitted a working query. The framework
*signal* in the prompt (`ORDER BY contains aggregate grouping(local.channel_label)`) is a **clean,
intentional author-time `InvalidSyntaxException` with an actionable, correct remedy** — the agent
followed it and recovered in a **single iteration**. It is NOT the token sink and NOT a framework
crash. A real but **secondary** gap does exist (compound-aggregate ORDER-BY not auto-resolved; case C
below), but it never blocked the agent.

**Run:** `evals/tpcds_agent/results/20260629-030015_enriched/` (`agent_log.q80.{jsonl,conversation.txt}`).
Only **two** error events in the whole q80 log: 1× the grouping ORDER-BY message, 1×
`Undefined concept: cs.return_net_loss` (agent namespace mistake — used `cs.` after importing
`raw.all_sales as s`; not a framework bug). The 791k tokens were spent on schema exploration
(agent-info, ~10 `explore` calls over all_sales/channel facts+dims/returns) and correctness
deliberation, not on framework thrash.

## Symptom (the prompt's framework-looking signal)

Agent's first runnable query (conversation msg 41) ordered the rollup hierarchy with an **inline
`grouping()` aggregate that is not a SELECT output**:

```trilogy
... by rollup (channel_label, outlet_identifier)
order by
    grouping(channel_label) + grouping(outlet_identifier) asc,   -- inline aggregate, not projected
    channel_label asc nulls first,
    outlet_identifier asc nulls first
```

→ `Syntax error: ORDER BY contains aggregate \`grouping(local.channel_label)\` (line 4), which is not
a SELECT output. Aggregates cannot be computed in the ordering scope; add it to SELECT — prefix with
\`--\` to keep it out of the output rows — and order by the alias, e.g.
\`select ..., --grouping(local.channel_label) as g order by g desc\`.`

The agent applied exactly that (msg 45): added `--grouping(channel_label)+grouping(outlet_identifier)
as _level` and `order by _level ...`. Msg 47 ran **exit 0**, producing rollup rows. One bump, one fix.

## Minimal repro + trigger matrix (read-only `generate_sql` against the run workspace)

Harness: `sys.path.insert(0,'evals'); from common import scoring;
eng=scoring.make_scoring_engine(ws/'tpcds.duckdb', ws, 'tpcds'); eng.generate_sql(body)[-1]`,
`ws = results/20260629-030015_enriched/workspace`. Base = the q80 select with
`by rollup (channel_label, outlet_identifier)`.

| # | ORDER BY | projected (hidden) | Result |
|---|---|---|---|
| A | `grouping(a)+grouping(b)` (agent's 1st) | nothing | **FAIL** — correct rejection (no match) |
| B | `_level` (alias **name**) | `--grouping(a)+grouping(b) as _level` | **OK** — agent's fix; no aggregate in order scope |
| C | `grouping(a)+grouping(b)` (inline, **compound**) | `--grouping(a)+grouping(b) as _level` | **FAIL** — *gap*: identical projection exists, not collapsed |
| D | `grouping(a)` (single, inline) | nothing | **FAIL** — correct rejection |
| E | `grouping(a)` (single, inline) | `--grouping(a) as g1` | **OK** — single-aggregate resolver collapses it |
| F | `grouping(a)+grouping(b)` (inline, compound) | `--grouping(a) as g1, --grouping(b) as g2` (leaves) | **OK** — each leaf resolves, leaves `g1+g2` |
| G | `g1+g2` (alias arithmetic) | leaves `g1,g2` | **OK** |
| — | canonical `tests/modeling/tpc_ds_duckdb/query80.preql` | orders by `channel/id asc nulls first` (no grouping) | **OK builds** (6989-char SQL) |

So ordering a rollup by the `grouping()` hierarchy **is supported** — via the alias name (B), via a
single inline grouping over its projection (E), or by projecting the **leaf** groupings and ordering by
the inline/alias arithmetic (F/G). Only one natural phrasing fails: **inline `grouping(a)+grouping(b)`
ordered against an identically-projected *compound* alias (C)**.

## Root cause of the (secondary) C gap — file:line

`trilogy/parsing/v2/select_finalize.py`:
- `_substitute_order_by_aggregates` (1420) auto-rewrites an inline ORDER-BY aggregate to its projected
  alias, via `_select_order_match_outputs` (1406) → `_order_match_signature` (1388) →
  `_aggregate_full_signature` (1278).
- `_aggregate_full_signature` returns a signature **only for a node that is itself a single
  aggregate `Function`/`AggregateWrapper`** (lines 293-307); it returns `None` for an arithmetic
  `add(grouping(a),grouping(b))`. So a SELECT output projected as the **compound** expression
  `grouping(a)+grouping(b) as _level` is **not registered** in `sig_to_ref`, and the matching
  validator map `_select_aggregate_outputs` (326) likewise skips it.
- `_validate_order_by_aggregates` (1450) then walks the ORDER-BY expr with
  `_collect_condition_aggregates` (485), finds the **leaf** `grouping(channel_label)`, finds no
  alias for it (only single-aggregate projections are registered), and raises at **1479-1483**.

So: the resolver/validator operate **per single-aggregate node**, never on a *compound expression of
aggregates*. Case E works because the projected output IS a single aggregate; case F works because the
leaves are projected as single aggregates; case C fails because the projection is the compound `add`,
invisible to both helpers. Fix surface (not applied): teach `_substitute_order_by_aggregates` to also
match an ORDER-BY *sub-expression* whose whole shape equals a projected compound output, or register
compound-expression outputs by structural signature.

## Classification & family relation to q70

**Distinct family from q70.** The two q70 bugs are genuine framework **crashes** on valid queries:
- `bug_q70_recursion_within_parent_rank.md` — uncaught `RecursionError` (build-phase cyclic grain:
  a BASIC concept that references `grouping()` *and* embeds an aggregate); harness reports a crash.
- `bug_q70_invalid_reference_rollup_grouping.md` — `INVALID_REFERENCE_BUG` sentinel (CTE `source_map`
  missing an inline order-by aggregate address in a top-N rank rowset).

q80's grouping-in-ORDER-BY is **not a crash**: it is a deliberate author-time `InvalidSyntaxException`
(the ordering scope cannot recompute aggregates) with a precise, working remedy that the agent applied
once. The query builds and runs. The only real defect — the **compound-aggregate ORDER-BY
auto-resolve miss (C)** — lives in the **same `select_finalize.py` rollup/grouping area** as the FIXED
q14 grouping-normalization and the q70 grouping work, and is conceptually adjacent to the q70
INVALID_REFERENCE "two equal aggregates not collapsed" theme (an aggregate that should resolve to a
projection doesn't). But it is a *mild, non-crashing auto-resolver coverage gap*, well-guided and
worked-around, not a sentinel/recursion bug. Lowest priority of the family.

## Note on the actual q80 outcome

The submitted query runs but is a **correctness** risk vs. the reference (e.g. filter
`s.channel_dim_id is not null` vs. canonical `channel_dim_text_id is not null`; the
`coalesce(sum(return_net_loss),0)` vs. canonical per-row `coalesce(return_net_loss,0)` then sum). Those
are modeling choices, not framework bugs, and are outside this report's scope.
