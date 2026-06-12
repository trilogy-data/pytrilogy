# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 4/13 | q29, q40, q44, q82 | — |
| enriched | 6/13 | q46, q68, q75 | q40 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.31 | 11,120,283 |
| enriched | 0.46 | 7,509,728 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q29 | ✅ | ✅ |
| q40 | ✅ | ❌ fail |
| q44 | ✅ | ✅ |
| q46 | ❌ fail | ✅ |
| q54 | ❌ fail | ❌ fail |
| q64 | ❌ exhausted | ❌ fail |
| q68 | ❌ fail | ✅ |
| q72 | ❌ fail | ❌ fail |
| q75 | ❌ fail | ✅ |
| q76 | ❌ fail | ❌ fail |
| q77 | ❌ fail | ❌ fail |
| q82 | ✅ | ✅ |
| q84 | ❌ fail | ❌ fail |
