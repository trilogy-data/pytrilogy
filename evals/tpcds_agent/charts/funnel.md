# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 2/13 | q40, q82 | — |
| enriched | 3/13 | q29, q44, q68 | q40, q82 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.15 | 12,895,653 |
| enriched | 0.23 | 12,444,642 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q29 | ❌ fail | ✅ |
| q40 | ✅ | ❌ fail |
| q44 | ❌ exhausted | ✅ |
| q46 | ❌ fail | ❌ fail |
| q54 | ❌ fail | ❌ fail |
| q64 | ❌ fail | ❌ fail |
| q68 | ❌ fail | ✅ |
| q72 | ❌ fail | ❌ exhausted |
| q75 | ❌ fail | ❌ fail |
| q76 | ❌ fail | ❌ fail |
| q77 | ❌ fail | ❌ fail |
| q82 | ✅ | ❌ fail |
| q84 | ❌ fail | ❌ fail |
