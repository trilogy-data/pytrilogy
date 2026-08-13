# Handoff: cutting the enriched leg's token cost without hurting accuracy

**2026-08-12.** Follows [`handoff_noise_crossover.md`](handoff_noise_crossover.md).
Target: the enriched path costs 2.08M cache-adjusted tokens per 20 questions vs
~0.85M for a bare-discovery SQL agent (~2.3x). This memo decomposes where those
tokens go (run `20260812-133647_enriched_aggregates`, 20/20 pass), prices each
waste class, and records the fixes landed with it.

## 1. Where the 2.08M adjusted tokens go

Adjusted = fresh input + 0.1×cached rebill + completion:

| component | tokens | share |
|---|---:|---:|
| cache-discounted rebilling (0.1 × 11.5M cached) | ~1.15M | **55%** |
| fresh (uncached) input | ~0.56M | 27% |
| completion (agent's own writing) | ~0.37M | 18% |

Over half the bill is re-carrying accumulated context, so **turn count and
payload size dominate**, and both are heavy-tailed:

| query | iters | raw | adj | note |
|---|---:|---:|---:|---|
| q08 | 44 | 3.21M | 434K | spiral (see §2) |
| q06 | 51 | 2.16M | 286K | spiral |
| q14 | 23 | 1.22M | 192K | chronic |
| q05/q17/q02/q11 | 16–26 | 0.6–1.1M | 100–166K | |
| median 12 queries | 5–13 | 56–337K | 20–70K | lean |

q08+q06 alone are 35% of the leg's adjusted cost. The same queries (q17, q06,
q08, q14) top the 20260811-145002 run too — **the heavy tail is chronic, not
noise**. The floor is fine: an easy enriched query runs 5–7 turns, comparable
to the SQL agent's 8.3 average.

## 2. What the spirals actually are

Classified every failed `trilogy run` across all three enriched legs: only
**37 of 286 runs errored**. The spirals are mostly *successful* runs the agent
re-drafted — semantic self-doubt on the reverse-engineered TPC-DS oddities —
punctuated by error clusters:

- **q08 (08-12) was mostly not the agent's fault**: 2 runs ate a raw
  `NameError` traceback (the eval ran against the live dev tree mid-edit —
  transient import breakage), 3 runs hit `Missing source map entry` planner
  failures (from the same-day aggregate-signature work; **both probe bodies
  now pass on the current tree** — verified), 1 generated-SQL DuckDB parser
  error, and **3 runs where the output cap swallowed the error** (see fix
  below), leaving exit_code 1 with no visible reason → blind retries.
- q06 is genuine agent difficulty: disconnected-subgraph ambiguity errors,
  a rename-shadow error, `nulls first` rejected by the grammar.
- Process rule this hardens: **don't run eval legs against a mid-edit tree**
  — pin a worktree. One transient broken import cost q08 ~13 turns.

## 3. Waste ledger (priced against the 2.08M leg)

| class | measured | mechanism |
|---|---:|---|
| explore payloads | 332K (16.0%) | 54 calls × 10.6K chars avg, rebilled |
| static agent-info refetch | 276K (13.3%) | the 18.7K-char `query` guide + index + cli fetched fresh in EVERY session (20×) |
| delete-only turns | 92K (4.4%) | 11 whole turns that only `file delete`d probes |
| param echo in run results | 14K (0.7%) | tool echoed the 2.6K-char zip list back per run |
| param retype per run call | 14K (0.7%) | agent re-emitted the zip list as completion tokens in every `run --param` call (20× in q08) |
| param duplicated in prompt | 7K (0.3%) | zip list appeared TWICE in the task prompt |

Plus the workflow observation that multiplies across all of it: agents did
write → run → delete as **three separate turns** (91 writes + 101 runs + 54
deletes = 246 calls), never using the existing one-call
`file write --run`/`--run-and-delete` — because the task prompt itself said
"Validate with `trilogy run <file>`" and the CLI doc's own "typical workflow"
taught the two-call pattern. Collapsing write+run pairs and killing separate
deletes removes ~90–140 of 312 turns; at the 55% rebilling share that is
roughly **300–450K adjusted (~15–20% of the leg)** — the single biggest lever.

## 4. Landed with this memo

1. **`truncate_json_events` preserves trailing `error`/`summary` events**
   (`trilogy/scripts/agent_tools.py`): the cap used to drop *trailing* events,
   which is exactly where run diagnostics live — a failed run became
   indistinguishable from a truncated successful one (5 blind-retry incidents
   across the enriched legs). Middle events are dropped instead; oversized
   diagnostics are middle-truncated but always surfaced. Tests in
   `tests/scripts/test_agent.py`.
2. **`environment_params` elides long values** (`display_execution.py`): >120
   char values echo as a preview + length note instead of verbatim.
3. **`file write --run/--run-and-delete` forwards `--param`**
   (`trilogy/scripts/file.py`): the one-call idiom now works for parameterized
   queries too. Tests in `tests/scripts/test_file.py`.
4. **The one-call idiom is now the anchored workflow**: eval task prompt
   (`evals/common/prompts.py`) says "write and validate in ONE call" and
   points probes at `--run-and-delete`; the CLI doc's typical-workflow section
   teaches the same. Long param values now appear once in the prompt (CLI
   suffix), not twice.

Expected combined effect: ~20% adjusted-token cut on the enriched leg
(dominated by item 4), plus whatever the error-visibility fix saves in
avoided blind retries. Accuracy risk: near zero — same operations, fewer
round trips, strictly more error information.

## 5. Not landed — ranked next steps

1. **Trim the `agent-info query` guide** (18,659 chars, fetched every
   session; the whole static-doc class is 13.3%). Halving it saves ~6%/leg.
   Accuracy risk is REAL (this guide is why enriched agents pass) — needs an
   A/B leg, not a hunch. Candidates: prose → dense reference tables, and
   anything duplicated by the on-demand `syntax example` topics.
2. **Explore payload compaction + clarity** (16% pool). q08's transcript
   shows the agent misreading the namespace/roles JSON ("That's confusing.
   Let me re-check") — clearer fully-qualified concept listings cut both
   re-explore turns and payload size. Also consider `--show` defaults.
3. **Chronic-query engine gaps** — the durable fix for the heavy tail:
   q17 tuple-membership planning gap (known), q06-class disconnected-subgraph
   errors should enumerate which concepts landed in which island and suggest
   the bridging join. `nulls first` in ORDER BY is a candidate
   grammar-relaxation (agents reach for it; "friendly SQL" precedent).
4. **Params by file** (`--params-file` or harness-registered defaults) — the
   general form of the zip-list fixes; removes retyping entirely.

## 6. Validation plan

A/B on `enriched_aggregates`, 20q, deepseek-v4-flash, --concurrency 2
(per the rebaseline rule), against a same-day control leg on the SAME tree
(corpus-twice rule): compare adjusted tokens, iterations, and
write/run/delete call mix. Token deltas >±20% are readable at n=20; accuracy
deltas under ±4 passes are not (enriched swung 16↔20 on identical inputs) —
judge accuracy only directionally, or run replicates. Guard metric: pass
count must not drop by more than the replicate floor.
