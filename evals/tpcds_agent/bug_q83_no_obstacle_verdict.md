# q83 — NO FRAMEWORK OBSTACLE (540,074-token sink is agent + guidance churn)

Run: `evals/tpcds_agent/results/20260708-135136_enriched`, q83 status=PASS, 540,074 tokens, no error signature.
Verdict: **NOT a framework bug.** The framework produced an optimal plan and correct results.
The token sink is fixed prompt/exploration bulk re-sent per turn + agent self-reasoning + 3 language-friction parse errors — NOT silent mis-resolution and NOT a slow/runaway plan.

## Proof — plan is optimal, results correct, fast

Timing (workspace duckdb, agent's final `workspace/query83.preql`):
- `generate_sql`: **0.392 s**  (SQL length 4106 chars)
- `execute`: **0.031 s**, 24 rows
- In-agent runs logged `duration_ms` 29.2 / 29.4 — consistent, no slow plan.

Correctness: agent output == reference `tests/modeling/tpc_ds_duckdb/query83.sql` (both **24 rows**, identical values; only column order differs, which the scorer reorders).

Generated plan (no fanout, no huge-fact join, no FULL leak):
- 3 per-channel CTEs, each `returns INNER JOIN item INNER JOIN date_dim WHERE d_week_seq IN (...) GROUP BY item_id` — exactly the canonical `sr_items`/`cr_items`/`wr_items` shape.
- Final: 3-way **INNER JOIN on item_code** + `WHERE *_rows > 0` + order/limit.
- `subset join a=b=c` + the `*_rows>0` HAVING-style filter correctly compiled to inner-join intersection semantics (matches canonical `WHERE sr.item_id=cr.item_id AND sr.item_id=wr.item_id`).

The prior-memory "q83 runaway huge-fact join" concern does **not** reproduce on the current engine/model.

## Where the 540k tokens actually went (transcript = 45 msgs, 130 KB single-pass)

The transcript is ~32k tokens once; 540k is the cumulative re-send of a growing context across ~22 assistant turns (no visible prompt caching). Bulk drivers, in order:
1. **Fixed exploration bulk** — huge model-catalog JSON + full-body explores of `item`, `store_returns`, `catalog_returns`, `web_returns`, `date` (log lines ~980–2000). Re-sent every turn.
2. **Agent semantic self-reasoning** — ~10 messages re-reading the prompt's ambiguous "a channel total is *unknown* only when none of its rows recorded any quantity" clause and the percentage formula (msgs 35/37/41). This is prompt ambiguity, not engine behavior.
3. **3 parse errors, each forcing a full-body rewrite** (bytes re-sent):

## Trigger matrix (the 3 friction points)

| # | Agent wrote | Framework response | Class |
|---|---|---|---|
| 1 | `count(sr.item.sk) as store_rows -- comment` | Parse error `expected metadata, limit, order_by...` — `--` is the HIDE modifier, not a comment | Guidance/language (known `--`=HIDE gotcha) |
| 2 | 3 rowsets, no final `select` | `Nothing was executed: parsed 6 definition statement(s)... add a final select` | Agent (expected; incremental build) — good error msg |
| 3 | `sum(sr.return_quantity is not null ? 1)` | `Cannot use is not with non-null or boolean value <Filter: MagicConstants.NULL where bool(1) = True>` | Agent misuse of `?` filter operator; **error message is cryptic** (minor UX) |

Only #3 touches the engine, and only as a **confusing-error-message** nit: the agent put the boolean on the LHS of `?` (`bool ? value`) instead of `agg(value ? cond)`; the message surfaces internal `MagicConstants.NULL`/`Filter` repr instead of "the `?` filter takes `value ? condition`". Not a silent obstacle — it's a loud, correct rejection with poor wording.

## Classification

- **Framework silent mis-resolution:** NONE (output byte-matches reference).
- **Slow / runaway plan:** NONE (gen 0.39 s, exec 0.03 s, optimal 3-CTE inner-join plan).
- **Guidance:** `--`=HIDE-modifier trap (#1) recurs across queries; cryptic `?`-filter error (#3).
- **Agent:** most churn — repeated full-body rewrites + prolonged null-semantics deliberation over an intentionally ambiguous prompt.

## Optional follow-ups (NOT bugs)
- Improve the `?`-filter error to name the correct form (`aggregate(value ? condition)`) instead of dumping the `Filter`/`MagicConstants.NULL` repr.
- The `--`=HIDE vs comment trap keeps costing agents a rewrite; a one-line hint in the parse error ("`--` starts a HIDE modifier; use `#` for comments") would cut a turn.

No code changed (read-only diagnostic).
