# TPC-DS category funnel

## Category pass coverage

| category | passing | unique passes | shared passes |
|---|---|---|---|
| db-only+aggregates+confusable×2 | 18/20 | — | q01, q03, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q18, q19, q20 |
| db-only+aggregates+confusable×3 | 18/20 | — | q01, q02, q04, q05, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q18, q19, q20 |
| enriched+aggregates+confusable | 18/20 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q13, q15, q16, q17, q18, q19, q20 |

## Metrics

| category | pass rate | total tokens | cache-adj cost | agg used | p50 cand ms |
|---|---|---|---|---|---|
| db-only+aggregates+confusable×2 | 0.90 | 5,323,996 | 1,040,514 | 0/20 | 83 |
| db-only+aggregates+confusable×3 | 0.90 | 5,301,754 | 1,021,959 | 0/20 | 50 |
| enriched+aggregates+confusable | 0.90 | 8,614,060 | 1,699,641 | 1/20 | 112 |

## Per-query matrix

| query | db-only+aggregates+confusable×2 | db-only+aggregates+confusable×3 | enriched+aggregates+confusable |
|---|---|---|---|
| q01 | ✅ | ✅ | ✅ |
| q02 | ❌ fail | ✅ | ✅ |
| q03 | ✅ | ❌ fail | ✅ |
| q04 | ❌ timeout | ✅ | ✅ |
| q05 | ✅ | ✅ | ✅ |
| q06 | ✅ | ❌ fail | ✅ |
| q07 | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ |
| q12 | ✅ | ✅ | ❌ fail |
| q13 | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ❌ fail |
| q15 | ✅ | ✅ | ✅ |
| q16 | ✅ | ✅ | ✅ |
| q17 | ✅ | ✅ | ✅ |
| q18 | ✅ | ✅ | ✅ |
| q19 | ✅ | ✅ | ✅ |
| q20 | ✅ | ✅ | ✅ |
