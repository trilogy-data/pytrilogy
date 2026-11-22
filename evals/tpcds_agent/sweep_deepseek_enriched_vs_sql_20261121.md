# DeepSeek v4-flash sweep: enriched Trilogy vs raw SQL (2026-11-21)

Runs: `20261121-223355_{enriched,ingest}` and `20261121-230254_{sql_bare,sql_schema}`.
All four are spliced: 11 rollup questions fresh (0.3.529), the other 88 from
`20261017-225406_*` (0.3.395). Read every failure below against that.

| leg | pass | total tok | median tok/q | p90 tok/q | median turns |
|---|---|---|---|---|---|
| sql_schema | 94 | 9.1M | 68k | 153k | 7 |
| sql_bare | 92 | 16.6M | 75k | 248k | 8 |
| ingest | 91 | 23.4M | 184k | 444k | 10 |
| enriched | 92 | 27.2M | 171k | 514k | 11 |

Enriched costs ~3x sql_schema for two fewer passes. The gap is mostly turn
count multiplied by a large re-read context, not authoring errors.

## Where enriched tokens go

Context tax = each tool result's size x the LLM calls that re-read it.

| enriched source | calls | avg chars | share of tax |
|---|---|---|---|
| `agent-info query` | 99 | 17.9k (19.4k on main) | 35% |
| `explore` (full) | 194 | 7.5k | 26% |
| `explore --ns/--regex` | 128 | 6.0k | 11% |
| `file list` | 113 | 5.0k | 11% |
| `agent-info syntax ...` | 96 | 3.1k | 5% |
| probes (`--run-and-delete`) | 463 | 0.9k | 5% |

sql_schema's whole tax is 2.9M tok-equiv vs enriched 12.7M; 76% of it is one
13k `schema.md` read.

Turn shape: enriched median 6 turns before first answer write (sql_schema 5),
and **3 turns after the answer first runs clean (sql 1)**. 307 of 464 enriched
probes come after a clean answer: the agent re-verifies, SQL agents do not.

## Fixed on this branch

- Function list advertised names with no call syntax (`isnull`,
  `is_not_distinct`, `power`, `concat_strict`, `unix_to_timestamp`,
  `grain_pin`, `simple_case`) and a `now()` example that does not parse. Hidden;
  `test_advertised_functions_have_call_syntax` checks names against the pest grammar.
- `_q04_*`/`_q83_*` regression fixtures and a stray fuzzer `repro.preql` were
  copied into the agent's `raw/` and shown by every `file list`. `_` is now a
  skipped prefix; `repro.preql` deleted.
- Named rollup + rank (q67): an alias/expression of `r.gid` beside a rank
  raised a keyless join or silently cross-joined (426 rows vs 142); renamed
  rollup columns beside `r.gid` raised "plan renders no column". Fixed in
  `basic._expose_parent_grain` (a boundary exposes its hidden grouping flags)
  and `_terminal_basic_grouping_provider` (no provider that drops a demanded
  grain key). `tests/core/processing/test_rollup_rowset_window_grouping_key.py`.
- Probe-time planner errors re-checked: 7 of 10 already compiled on main; q29
  is a real disconnect. q05/q38 (renamed role columns over one scan pairing
  keyless) fixed in `group_rules` terminal-sibling merge;
  `tests/modeling/tpc_ds_duckdb/test_role_alias_returns_filter.py`.
- q28: fact measures marked nullable in store/catalog/web models.
- sql_bare sandbox: SQL agent file tools refuse paths outside the workspace.

## Enriched failures (7)

| q | class | cause | lever |
|---|---|---|---|
| 35 | planner, fixed | same .preql matches ref on main | rerun, don't splice |
| 64 | planner, fixed | 600s timeout on 0.3.395; 6s on main | rerun |
| 28 | model | `store_sales` measures (`list_price`, `coupon_amt`, `wholesale_cost`, `quantity`, `sales_price`) declared non-null; ~4.5% NULL. Agent counted grain, ~1% high | fixed (models) |
| 04 | agent + doc | rows right, kept 6 HAVING helper columns (10 cols vs 4) | doc: hide helper columns with `--` |
| 67 | planner + agent | first answer was correct; agent rewrote to two-stage rollup and hit bugs A-C | A/B fixed; C is open item 1 |
| 59 | agent / ref | kept NULL-store bucket; ref itself 4x-duplicates under DuckDB 1.5.3 | audit q59 reference |
| 83 | agent | "corrected" the stated formula, 9x off | none for Trilogy |

sql_schema passes all seven.

## Open

1. **Rank over a named rollup whose keys omit the grouping level** (q67's
   remaining wrong answer): a window ranks the distinct tuples of its keys, so
   a subtotal row and a data-NULL leaf with equal visible keys collapse
   (rank 60 vs ref 72). Current semantics, not a planner bug; decide between an
   error and a run warning like `window_filter_needs_having`.
2. **Shrink `agent-info query`** (19.4k chars, 35% of tax). Split by topic or
   move set-ops/rowset/membership detail behind the existing `syntax example`s.
3. **`explore` default output**: the full fact explore is 7.5k and usually
   followed by an `--ns` call; consider printing the root namespace only.
4. **Post-answer re-verification** (3 turns vs 1). A/B a prompt line or result
   shape before changing anything.
5. **Grammar gaps from probes**: `select tot as tot` errors as a self-rename
   (q30/q41/q47, could be a no-op); no null-safe compare spelling
   (`is_not_distinct(...)` / `IS NOT DISTINCT FROM`, q50/q74) - needs pest + rebuild.
6. **`trilogy file list ..` lists outside the workspace**: the Trilogy legs can
   wander like sql_bare did; no sign of it in these logs.
7. **q59 reference** 4x-duplicates rows under DuckDB 1.5.3.
8. **Rerun all four legs unspliced** on this branch; q35/q64 should flip.

Kept deliberately: every run spends a round trip on the bare `agent-info`
directory before `agent-info query`. Finding the reference is part of what the
eval tests, so the system prompt does not point at it directly.
