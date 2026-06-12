# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 4/13 | q29, q40, q68, q82 | — |
| enriched | 3/13 | q44 | q40, q68 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.31 | 13,546,038 |
| enriched | 0.23 | 10,521,131 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q29 | ✅ | ✅ |
| q40 | ✅ | ❌ fail |
| q44 | ❌ fail | ✅ |
| q46 | ❌ fail | ❌ fail |
| q54 | ❌ fail | ❌ fail |
| q64 | ❌ fail | ❌ exhausted |
| q68 | ✅ | ❌ fail |
| q72 | ❌ fail | ❌ fail |
| q75 | ❌ fail | ❌ fail |
| q76 | ❌ fail | ❌ fail |
| q77 | ❌ fail | ❌ fail |
| q82 | ✅ | ✅ |
| q84 | ❌ fail | ❌ fail |
