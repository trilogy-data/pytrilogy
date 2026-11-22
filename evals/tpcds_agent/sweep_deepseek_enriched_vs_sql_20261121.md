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

- System prompt said bare `agent-info` prints the language reference; it is a
  1.4k directory, so all 99 runs paid an extra round trip. Prompt now points
  query tasks at `agent-info query`.
- Function list advertised names with no call syntax (`isnull`,
  `is_not_distinct`, `power`, `concat_strict`, `unix_to_timestamp`,
  `grain_pin`, `simple_case`) and a `now()` example that does not parse. Hidden;
  `test_advertised_functions_have_call_syntax` checks names against the pest grammar.
- `_q04_*`/`_q83_*` regression fixtures and a stray fuzzer `repro.preql` were
  copied into the agent's `raw/` and shown by every `file list`. `_` is now a
  skipped prefix; `repro.preql` deleted.

## Enriched failures (7)

| q | class | cause | lever |
|---|---|---|---|
| 35 | planner, fixed | same .preql matches ref on main | rerun, don't splice |
| 64 | planner, fixed | 600s timeout on 0.3.395; 6s on main | rerun |
| 28 | model | `store_sales` measures (`list_price`, `coupon_amt`, `wholesale_cost`, `quantity`, `sales_price`) declared non-null; ~4.5% NULL. Agent counted grain, ~1% high | mark fact measures `?` across store/catalog/web |
| 04 | agent + doc | rows right, kept 6 HAVING helper columns (10 cols vs 4) | doc: hide helper columns with `--` |
| 67 | planner + agent | first answer was correct; agent rewrote to two-stage rollup and hit bugs A-C | fix A-C below |
| 59 | agent / ref | kept NULL-store bucket; ref itself 4x-duplicates under DuckDB 1.5.3 | audit q59 reference |
| 83 | agent | "corrected" the stated formula, 9x off | none for Trilogy |

sql_schema passes all seven.

## Open

1. **Planner bugs on main**, named rollup + rank (from q67):
   - A: `--r.gid` projected beside `rank(r.category, r.class) over (...)` raises
     "keyless join between row-bearing sources".
   - B: same plus `having rnk <= 100` raises "plan renders no column for ['r.gid']".
   - C: silent wrong rows: a rank over a named rollup whose keys omit the level
     dedups rollup rows before the window (rank 60 vs ref 72).
   Repro shape:
   ```
   import raw.store_sales as ss;
   with r as rollup((
       select ss.item.category, ss.item.class, sum(ss.quantity) as qty,
           grouping_id(ss.item.category, ss.item.class) as gid
   ), ss.item.category, ss.item.class);
   select r.category, r.class, r.qty,
       rank(r.category, r.class) over (partition by r.category order by r.qty desc) as rnk,
       --r.gid as g;
   ```
2. **Re-check probe-time planner errors from the 10-17 logs on main**: q05
   keyless join, q14 "Could not find CTE", q29 DuckDB binder (`abundant` not
   found), q38 "could not be planned", q44 "missing source reference", q25/q82
   "could not resolve connections".
3. **q28 model nullability** (table above).
4. **Shrink `agent-info query`** (19.4k chars, 35% of tax). Split by topic or
   move set-ops/rowset/membership detail behind the existing `syntax example`s.
5. **`explore` default output**: the full fact explore is 7.5k and usually
   followed by an `--ns` call; consider printing the root namespace only.
6. **Post-answer re-verification** (3 turns vs 1). A/B a prompt line or result
   shape before changing anything.
7. **Grammar gaps from probes**: `select tot as tot` errors as a self-rename
   (q30/q41/q47, could be a no-op); no null-safe compare spelling
   (`is_not_distinct(...)` / `IS NOT DISTINCT FROM`, q50/q74) - needs pest + rebuild.
8. **sql_bare sandbox leak**: on q02 the agent listed `/tmp`, `/tmp/ptmain/evals`
   and read a parameter-shifted reference file and trilogy source. `list_files` /
   `read_file` need to be confined to the workspace before sql_bare scores are trusted.
9. **Rerun all four legs unspliced** on main; q35/q64 should flip, and the
   prompt fix above changes the enriched baseline.
