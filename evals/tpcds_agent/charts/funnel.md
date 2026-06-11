# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 2/13 | q40, q82 | — |
| enriched | 1/13 | q68 | q40, q82 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.15 | 16,268,165 |
| enriched | 0.08 | 7,391,248 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q29 | ❌ fail | ❌ fail |
| q40 | ✅ | ❌ fail |
| q44 | ❌ fail | ❌ fail |
| q46 | ❌ fail | ❌ fail |
| q54 | ❌ fail | ❌ fail |
| q64 | ❌ fail | ❌ fail |
| q68 | ❌ fail | ✅ |
| q72 | ❌ fail | ❌ fail |
| q75 | ❌ fail | ❌ fail |
| q76 | ❌ fail | ❌ fail |
| q77 | ❌ timeout | ❌ fail |
| q82 | ✅ | ❌ fail |
| q84 | ❌ fail | ❌ fail |
