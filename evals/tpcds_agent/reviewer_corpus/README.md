# Reviewer-verdict corpus

A test set for the agent's submit **reviewer** (`_validate_completion` in
`trilogy/scripts/agent.py`), so changes to `REVIEWER_SYSTEM_PROMPT` can be
measured offline instead of by expensive full agent reruns.

## What the reviewer is *for*

When the agent calls `return_control_to_user`, the reviewer decides DONE /
NOT_DONE. Its **only** job is to catch the agent prematurely exiting **when the
agent itself signalled it wasn't finished** (mid-thought farewell, self-noted
uncertainty, an unresolved error). It must **not** grade the work — it has no
reference data, and second-guessing query correctness made it kick back ~87% of
legitimately-finished submissions, driving massive over-iteration.

## Gold-label rule

Each case is labeled from the agent's **own farewell** (`return_control_to_user`
message), independent of whether the work was actually correct:

- `NOT_DONE` — the farewell shows the agent was still working: narrates a next
  step ("now I'll…", "let me…", "I still need to…"), self-notes unresolved
  uncertainty, reports an error it's chasing, or is empty/cut-off.
- `DONE` — the farewell claims completion ("done", "written and validated",
  "runs cleanly"), even if the result may be wrong or a clause looks missing.
- Ambiguous farewells are dropped from the test set (a clean set beats a noisy
  one). `labeled.jsonl` is balanced and capped per class.

Labels are auto-derived by `_gold_bucket`; spot-check and hand-correct
`labeled.jsonl` as the corpus grows.

## Workflow

```bash
python reviewer_corpus.py extract                 # mine logs -> all.jsonl (gitignored, ~22MB)
python reviewer_corpus.py label                   # clear cases -> labeled.jsonl (committed)
python reviewer_corpus.py measure --per-class 30  # replay CURRENT prompt; report TPR / FPR / precision
```

Positive class = NOT_DONE (a kickback). Watch **FPR** (kicking back finished
work — the over-iteration driver) and **recall/TPR** (still catching genuine
premature exits).

## Result of the 2026-05-30 prompt tightening (same 60-case balanced sample)

| metric | old prompt | tightened prompt |
|---|--:|--:|
| recall / TPR | 100% | 93% |
| FPR (kick back done work) | 87% | 30% |
| precision | 54% | 76% |
| accuracy | 57% | 82% |
