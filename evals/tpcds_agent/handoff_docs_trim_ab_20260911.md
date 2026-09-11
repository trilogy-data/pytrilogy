# Handoff: `agent-info query` with vs without the example summaries (A/B, 2026-09-11)

The ACME token audit (PR #683, `evals/acme_insurance_agent/trace_audit.md`)
trimmed the per-example summaries out of `trilogy agent-info query` (16.8k ->
14.4k chars) on the argument that the doc sits in every later turn of every
question and `agent-info syntax` already prints the same listing. ACME was
flat on it (425k -> 427k tokens over eleven questions). This is the TPC-DS
check: the `enriched` leg, all 99 questions, sf=1, deepseek-v4-flash,
concurrency 3, run twice on ONE tree (`acme-insurance-eval` at 6622c0d83)
with only `get_query_authoring_output(example_summaries=...)` flipped between
runs. Nothing else changed between A and B, so the two legs compare cleanly;
do not compare either to the 08-20 runs (different tree, model and explore
protocol).

## Results

| | A: names only (`20260911-170057`) | B: full summaries (`20260911-181006`) |
|---|---|---|
| pass | 95/99 | 93/99 |
| fails | q11, q59, q74, q83 | q4, q8, q30, q41, q56, q59 |
| doc size the agent read | 14,449 chars | 16,829 chars |
| raw tokens total | 27.4M | 24.4M |
| cache-adjusted total | 6.49M | 5.93M |
| raw per question, med / mean | 182k / 277k | 181k / 246k |
| cache-adjusted per question, med / mean | 48.4k / 65.5k | 46.9k / 59.9k |
| LLM calls per question, med / mean | 9 / 10.2 | 9 / 9.8 |
| `agent-info syntax` / `syntax example` calls | 205 | 187 |
| tool calls / errors | 1,634 / 47 | 1,570 / 47 |
| wall | 68 min | 76 min |

## Reading

- **The trim does not pay.** Cache-adjusted cost per question is 3% higher
  at the median and 9% higher at the mean WITHOUT the summaries; raw is 12%
  higher. The mechanism is visible in the counts: with only names to go on,
  agents fetch more worked examples (205 vs 187 drilldowns, each 3-6k chars,
  each rebilling the context) and take ~0.4 more turns per question. The
  ~600-token-per-turn saving from the shorter doc is smaller than that.
- **Passes are noise.** 95 vs 93 with disjoint failure sets (only q59 shared)
  is inside one run's spread (the 08-20 handoff put ±2 passes at ~1.8 sigma).
  Nothing here says the summaries change correctness either way.
- **Decision: keep the summaries.** The trim is reverted on this branch;
  `agent-info query` prints the listing again and `TRILOGY_LEAD_IN` was
  never changed. The lesson generalizes the 08-20 finding: the per-example
  summary is the cheap part of the drilldown system, and removing it moves
  cost to the drilldowns rather than removing it.

## Open, for a later session

- q59 failed on both legs; q11/q74/q83 (A) and q4/q8/q30/q41/q56 (B) are
  single-run fails worth a `repeat_query.py` pass before filing anything.
- Both legs plan on the raw() inline gate as it stood at 6622c0d83 (it
  refused to inline any raw-bound fact scan beside a join). The follow-up fix
  (`1c91394fd`, gate keys on what the consumer reads) restores the pre-branch
  SQL shapes for those queries; it landed after both runs and is not
  reflected in them.
