# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 1/5 | q25 | — |
| enriched | 4/5 | q17, q50, q84 | — |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.20 | 2,703,420 |
| enriched | 0.80 | 1,326,738 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q01 | ❌ fail | ❌ fail |
| q17 | ❌ fail | ✅ |
| q25 | ✅ | ✅ |
| q50 | ❌ fail | ✅ |
| q84 | ❌ fail | ✅ |
