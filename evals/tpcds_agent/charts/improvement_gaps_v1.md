# Improvement gaps — base run 20260524-172309

Source: `report.json`, `trilogy_failures.md`, per-query agent JSONL logs.

## Headline

**30/92 pass (33%). The single dominant loss mode is iteration-budget exhaustion during schema exploration, not the language or the model.**

| Outcome | n | Driver |
|---|---:|---|
| pass | 30 | — |
| fail (wrong rows) | 36 | Filter / grain / aggregation misinterpretation |
| missing (no .preql) | 16 | Iteration cap hit during exploration |
| error (Trilogy raises) | 10 | Compiler errors the agent can't recover from |

## A. Iteration-budget thrash (HIGHEST LEVERAGE)

**12 of 16 "missing" queries hit `max_iterations=30` still in exploration mode** — last `trilogy` call was `explore`, never reached `write_file` + `run`. Examples:

| qid | iters | trilogy calls | last call | read_file calls |
|---:|---:|---:|---|---:|
| q49 | 30 | **45** | run | 9 |
| q51 | 21 | **31** | explore | 8 |
| q23 | 30 | 23 | explore | 7 |
| q57 | 30 | 23 | explore | 6 |
| q54 | 30 | 22 | explore | 10 |
| q27 | 30 | 22 | explore | 6 |

Across the whole run: **`trilogy explore`: 494 calls (5.4/query)** vs `trilogy run`: 184 (2/query). The agent spends ~3× more turns inspecting than running.

Why this happens:
1. **No single-shot schema overview.** Agent reads `raw/store_sales.preql`, then `explore` to get reachable concepts, then `explore --grep customer` to drill, then again for `--grep date`, … each call truncates and forces another.
2. **No persistent index in the agent's context.** Re-explore on every refinement.
3. **explore output is verbose.** ~26KB agent-info is truncated to 8KB by default (already raised to 32KB in eval). Same may apply to explore.

Levers (ordered by impact / cost):
1. **Raise `max_iterations` to 50–60** for the per-query budget. Cheapest possible fix.
2. **Add `trilogy describe <fact>` (or similar)** that returns: facts, dimensions reachable by chaining (1- and 2-hop), each concept's name + type + description, in one structured block. Agent then has one call instead of N.
3. **Track explore-output truncation.** If output >limit, the agent will re-explore. Bigger budget + structured output that fits.
4. **Prompt nudge**: "Write a *draft* query as soon as you know the fact table and the dimension chains you need. Iterate on errors — don't pre-explore exhaustively." (Worth A/B testing, may make fails worse if not careful.)

## B. Trilogy compiler / discovery errors (medium leverage)

10 hard errors. The five distinct types:

| Type | qids | Notes |
|---|---|---|
| `UnresolvableQueryException` ("Could not resolve connections") | q09, q31 | Multi-fact joins where grains don't unify; error names the concept list but not WHY they can't merge. |
| `AmbiguousRelationshipResolutionException` | q31, q83 | Multiple FK paths; agent can't pick. Error lists "multiple concept additions" — agent likely doesn't understand what to disambiguate. |
| `InvalidSyntaxException: Cannot reference … in HAVING / ORDER BY` | q47, q67 | Trilogy says exactly what to do ("move to WHERE") — agent reissues the same form. Not a tooling bug; an agent-learning issue. |
| HAVING-in-projection mismatch | q06, q47, q65, q76, q81 | Same root cause, multiple reissues. |
| `MergeNode<...>` internal dump | q81 | **Already flagged + fix agent in flight** — internal resolver state leaking as user error. |

The agent issued the same HAVING-illegal form 3× in some queries before giving up. The compiler error is correct, but the agent's loop doesn't translate "move to WHERE" into an actual edit. **This is largely a model-quality issue**, but improvements possible:
- A "did you mean" line in the existing error that shows the exact rewrite (`having x > 1 over (state)` → `where x > 1 by state` in the inline form).
- For ambiguous-relationship errors, surface the candidate paths as an enumerated list so the agent can `merge` the chosen one.

## C. Result mismatches (36 fails)

| Bucket | n | What likely happened |
|---:|---|---|
| Same row count, wrong values | 9 | Off-by-one in filter (e.g., `>= 2000` vs `> 2000`); wrong aggregation field |
| Zero rows | 5 | Over-strict filter — likely enum value not in sample at sf=0.01 (cf. q96 'ese'). Worth sampling a couple. |
| >5× row disparity | 8 | Wrong grain — missing GROUP-BY column, double-join, or duplicated fact rows |
| Other | 14 | Bigger semantic misses (correlated subqueries, hierarchical rollup) |

Tactical: pick 3–5 of the "wrong rows but same count" failures and diff the expected vs candidate SQL. Often a 1-line filter fix.

## D. Agent loop / CLI ergonomics (mostly fixed now)

| Issue | Status |
|---|---|
| `trilogy file write` blocked by tool wrapper | **Fixed** (with quoting hint for split-content misuse) |
| `trilogy --debug run` is the only accepted order | **Fixed** — hoist accepts both orders |
| `trilogy run -` (stdin) errored | **Fixed** — reads stdin |
| `select count(*)` rejected | Doc'd in RULE_PROMPT line 21; agent ignores |
| `from X` SQL form | Errored 5× clearly; agent ignored |

The `from` and `count(*)` patterns are already covered by clear errors AND in the language reference. Adding more prose to the prompt likely won't help — these are model-discipline issues. The fixes above remove low-value friction.

## E. Prompt-quality regressions (none found in this pass)

The 14 `month_seq → year` rewrites + q6 cleanup landed before this run. No prompt re-issues from agents about "what does month sequence 1200 mean". The leak-test catches new ones.

## Suggested next actions, in order

1. **Bump `max_iterations` to 50** in the eval `trilogy.toml`. ~30 min, expect 5–8 more passes.
2. **Add `trilogy describe <fact>`** (or beef up `explore --show concepts` to be one-shot complete) → ~1 day, big leverage on missing-query budget.
3. **MergeNode bug fix** — fix agent already in flight; should remove q81's noise.
4. **Sample 5 "wrong row but same count" fails**, diff vs reference; if there's a common pattern, document it as a prompt example.
5. (Stretch) **AmbiguousRelationshipResolutionException error UX** — list candidate paths explicitly.

## Appendix: response-size + repeat-call data (gathered after v1)

Across the run (2,363 tool calls):

| Tool | count | avg chars | max chars | truncated | trunc-rate |
|---|---:|---:|---:|---:|---:|
| trilogy | 900 | 6,893 | 32,812 | 72 | **8%** |
| read_file | 753 | 1,654 | 2,703 | 0 | 0% |
| todo | 475 | 378 | 2,703 | 0 | 0% |
| write_file | 180 | 157 | 1,993 | 0 | 0% |
| return_control | 55 | 41 | 413 | 0 | 0% |

**Repeated identical-args calls: 293 (12% of all tool calls).** By tool:

| Tool | repeats |
|---|---:|
| read_file | 146 |
| trilogy | 110 |
| todo | 23 |
| write_file | 13 |
| return_control_to_user | 1 |

### What this tells us

- **Truncation matters but isn't the dominant signal.** 8% of `trilogy` calls came back truncated (max output 32,812 chars — that's the `tool_output_limit` ceiling, almost certainly `agent-info`). The new agent-info-no-truncate path will eliminate that subset.
- **The bigger story is context loss, not truncation.** `read_file` is *never* truncated yet the agent re-reads the same files 146 times. The agent's working memory isn't holding what it has already seen.
- **`trilogy` repeats (110) include re-explores.** Some of these are the agent re-querying the same `explore --grep customer` line after another iteration. A more compact, one-shot schema dump would reduce both repeats and total cost.

### Concrete follow-ups (post-v2 run)

- **Compact explore output.** Group concepts under a namespace header (`store_sales.date_dim → year, month, quarter, …`) instead of one line per concept; cuts response size by ~3–5× and may eliminate the truncation tail.
- **Schema digest tool.** A single call returning every fact + dimension + reachable concept chain for a given import, formatted as a flat tree. Agent calls it once at the start, no re-explores needed.
- **Same-args call detection in agent.py.** Already detect 3-in-a-row identical calls; could extend to "same call seen this query → return 'see prior result' marker" — forces the agent to either work with what it has or change the query.

