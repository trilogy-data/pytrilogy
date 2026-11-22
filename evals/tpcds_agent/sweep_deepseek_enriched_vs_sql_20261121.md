# DeepSeek v4-flash sweep: enriched Trilogy vs raw SQL (2026-11-21, rerun 2026-11-22)

Rerun: `20261122-145208_{sql_bare,sql_schema,ingest,enriched}`, all 99 questions
fresh on this branch (no splice), concurrency 1 per leg. Baseline is the
spliced 2026-11-21 run (11 fresh, 88 from 0.3.395).

| leg | pass (was) | total tok (was) | median tok/q | p90 tok/q | median turns |
|---|---|---|---|---|---|
| sql_schema | 95 (94) | 8.0M (9.1M) | 60k | 169k | 6 |
| sql_bare | 95 (92) | 10.3M (16.6M) | 74k | 244k | 8 |
| ingest | 92 (91) | 23.5M (23.4M) | 170k | 461k | 9 |
| enriched | 96 (92) | 23.4M (27.2M) | 168k | 500k | 10 |

Enriched now passes the most questions, still at ~3x sql_schema's tokens.
sql_bare's drop likely comes from the workspace file-tool fix (it previously
wandered outside the workspace); not separately measured.

## Where enriched tokens go (2026-11-21 run)

Context tax = each tool result's size x the LLM calls that re-read it.

| enriched source | calls | avg chars | share of tax |
|---|---|---|---|
| `agent-info query` | 99 | 17.9k (19.4k on main) | 35% |
| `explore` (full) | 194 | 7.5k | 26% |
| `explore --ns/--regex` | 128 | 6.0k | 11% |
| `file list` | 113 | 5.0k | 11% |
| `agent-info syntax ...` | 96 | 3.1k | 5% |
| probes (`--run-and-delete`) | 463 | 0.9k | 5% |

sql_schema's whole tax was 2.9M tok-equiv vs enriched 12.7M; 76% of it is one
13k `schema.md` read. Enriched spends 3 turns after its answer first runs clean
(sql 1): it re-verifies, SQL agents do not.

## Enriched failures (3)

| q | class | cause |
|---|---|---|
| 39 | agent | dropped February's repeated `w_warehouse_sk`/`i_item_sk` (8 cols vs 10); values match |
| 83 | agent / wording | computed `store / (total/3) * 100` instead of `store / total / 3 * 100` (9x); "percentage of the three-channel total averaged across channels" invites it. Same miss as the prior run |
| 86 | question | reference returns a 5th `rank_within_parent` column the question never lists; enriched and sql_schema return the 4 listed columns, matching positionally. Only ingest output the rank |

No planner failures. sql_bare also fails q39/q86; sql_schema also fails q86.

## Open

1. **q86 question**: add the rank to the reported columns (or drop it from the
   reference). Fails three of four legs on wording alone.
2. **q83 wording**: state the formula unambiguously.
3. **Rank over a named rollup whose keys omit the grouping level** (q67 passes
   now, but the semantics stand): a window ranks the distinct tuples of its
   keys, so a subtotal row and a data-NULL leaf with equal visible keys
   collapse. Decide between an error and a run warning like
   `window_filter_needs_having`.
4. **Shrink `agent-info query`** (19.4k chars, 35% of tax). Split by topic or
   move set-ops/rowset/membership detail behind the existing `syntax example`s.
5. **`explore` default output**: the full fact explore is 7.5k and usually
   followed by an `--ns` call; consider printing the root namespace only.
6. **Post-answer re-verification** (3 turns vs 1). A/B a prompt line or result
   shape before changing anything.
7. **Grammar gaps from probes**: `select tot as tot` errors as a self-rename
   (q30/q41/q47, could be a no-op); no null-safe compare spelling
   (`is_not_distinct(...)` / `IS NOT DISTINCT FROM`, q50/q74) - needs pest + rebuild.
8. **`trilogy file list ..` lists outside the workspace** for the Trilogy legs
   (file reads/DuckDB are sandboxed now; listing is not).
9. **q59 reference** 4x-duplicates rows under DuckDB 1.5.3.

Kept deliberately: every run spends a round trip on the bare `agent-info`
directory before `agent-info query`. Finding the reference is part of what the
eval tests, so the system prompt does not point at it directly.

## Agent sandbox (this branch, after the rerun)

- Eval agents inherit an allow-listed environment plus their one provider key
  (`agent_runner.agent_env`); the agent's `trilogy` subprocesses never get LLM
  keys (`AgentState.withheld_env`).
- `[engine.config] sandbox = true` (DuckDB) confines file access to the
  `trilogy.toml` directory and locks the config; eval workspaces set it unless
  the benchmark uses Python datasources.
- `[agent] allow_script_write = false` refuses `.py` writes; set for every
  eval without Python datasources.
