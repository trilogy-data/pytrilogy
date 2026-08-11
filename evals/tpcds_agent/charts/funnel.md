# TPC-DS category funnel

## Funnel (increasing scaffolding)

| category | passing | newly unlocked | regressions |
|---|---|---|---|
| db+schema+aggregates | 17/20 | q01, q02, q03, q04, q05, q06, q07, q09, q10, q11, q12, q13, q15, q16, q17, q18, q19 | — |
| db+schema+aggregates+noise | 14/20 | q14 | q05, q12, q16, q18 |
| enriched+aggregates | 17/20 | q08 | q01, q14 |
| enriched+aggregates+noise | 19/20 | q20 | q11 |

## Metrics

| category | pass rate | total tokens |
|---|---|---|
| db+schema+aggregates | 0.85 | 2,455,252 |
| db+schema+aggregates+noise | 0.70 | 2,610,710 |
| enriched+aggregates | 0.85 | 6,686,260 |
| enriched+aggregates+noise | 0.95 | 5,490,215 |

## Per-query matrix

| query | db+schema+aggregates | db+schema+aggregates+noise | enriched+aggregates | enriched+aggregates+noise |
|---|---|---|---|---|
| q01 | ✅ | ✅ | ❌ fail | ✅ |
| q02 | ✅ | ✅ | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ |
| q05 | ✅ | ❌ fail | ✅ | ✅ |
| q06 | ✅ | ✅ | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ |
| q08 | ❌ fail | ❌ fail | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ | ❌ fail |
| q12 | ✅ | ❌ fail | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ | ✅ |
| q14 | ❌ fail | ✅ | ❌ fail | ✅ |
| q15 | ✅ | ✅ | ✅ | ✅ |
| q16 | ✅ | ❌ fail | ✅ | ✅ |
| q17 | ✅ | ✅ | ✅ | ✅ |
| q18 | ✅ | ❌ fail | ✅ | ✅ |
| q19 | ✅ | ✅ | ✅ | ✅ |
| q20 | ❌ fail | ❌ fail | ❌ fail | ✅ |
