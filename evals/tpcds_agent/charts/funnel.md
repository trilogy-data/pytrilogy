# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 3/13 | q29, q40, q82 | — |
| enriched | 4/13 | q44, q68, q75 | q29, q40 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.23 | 12,471,510 |
| enriched | 0.31 | 5,694,137 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q29 | ✅ | ❌ fail |
| q40 | ✅ | ❌ fail |
| q44 | ❌ fail | ✅ |
| q46 | ❌ fail | ❌ fail |
| q54 | ❌ fail | ❌ fail |
| q64 | ❌ fail | ❌ fail |
| q68 | ❌ fail | ✅ |
| q72 | ❌ fail | ❌ fail |
| q75 | ❌ fail | ✅ |
| q76 | ❌ fail | ❌ fail |
| q77 | ❌ timeout | ❌ fail |
| q82 | ✅ | ✅ |
| q84 | ❌ fail | ❌ fail |
