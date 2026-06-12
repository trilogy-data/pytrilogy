# TPC-H category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| ingest | 20/22 | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q13, q14, q15, q16, q17, q18, q19, q20, q21 | — |
| enriched | 21/22 | q12, q22 | q02 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| ingest | 0.91 | 4,727,745 |
| enriched | 0.95 | 2,839,871 |

## Per-query matrix

| query | ingest | enriched |
|---|---|---|
| q01 | ✅ | ✅ |
| q02 | ✅ | ❌ fail |
| q03 | ✅ | ✅ |
| q04 | ✅ | ✅ |
| q05 | ✅ | ✅ |
| q06 | ✅ | ✅ |
| q07 | ✅ | ✅ |
| q08 | ✅ | ✅ |
| q09 | ✅ | ✅ |
| q10 | ✅ | ✅ |
| q11 | ✅ | ✅ |
| q12 | ❌ fail | ✅ |
| q13 | ✅ | ✅ |
| q14 | ✅ | ✅ |
| q15 | ✅ | ✅ |
| q16 | ✅ | ✅ |
| q17 | ✅ | ✅ |
| q18 | ✅ | ✅ |
| q19 | ✅ | ✅ |
| q20 | ✅ | ✅ |
| q21 | ✅ | ✅ |
| q22 | ❌ fail | ✅ |
