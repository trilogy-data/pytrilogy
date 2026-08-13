# TPC-DS category funnel

## Category pass coverage

| category | passing | unique passes | shared passes |
|---|---|---|---|
| db+schema+aggregates, renamed cols | 17/20 | — | q01, q02, q03, q04, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q18, q19 |
| db+schema+aggregates, shifted params | 17/20 | — | q01, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q17, q18, q19 |
| db+schema+aggs, renamed+shifted | 15/20 | — | q01, q03, q04, q05, q07, q08, q09, q10, q11, q12, q13, q15, q17, q18, q19 |
| db-only+aggregates+noise×4 | 19/20 | — | q01, q02, q03, q04, q05, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q18, q19 |
| db-only+aggregates+noise×12 | 18/20 | — | q01, q02, q03, q04, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q17, q18, q19 |
| db-only+aggregates+noise×24 | 18/20 | — | q01, q02, q03, q04, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q18, q19, q20 |
| db-only+aggregates+noise×48 | 18/20 | — | q01, q02, q03, q04, q06, q07, q08, q09, q10, q11, q12, q13, q14, q15, q16, q18, q19, q20 |

## Metrics

| category | pass rate | total tokens | cache-adj cost | agg used | p50 cand ms |
|---|---|---|---|---|---|
| db+schema+aggregates, renamed cols | 0.85 | 4,312,059 | 899,720 | 3/20 | 86 |
| db+schema+aggregates, shifted params | 0.85 | 4,208,824 | 905,349 | 3/20 | 132 |
| db+schema+aggs, renamed+shifted | 0.75 | 2,890,935 | 707,089 | 1/20 | 122 |
| db-only+aggregates+noise×4 | 0.95 | 3,962,315 | 824,958 | 0/20 | 120 |
| db-only+aggregates+noise×12 | 0.90 | 5,508,553 | 1,059,529 | 0/20 | 86 |
| db-only+aggregates+noise×24 | 0.90 | 4,068,268 | 851,999 | 0/20 | 121 |
| db-only+aggregates+noise×48 | 0.90 | 4,167,839 | 905,260 | 0/20 | 90 |

## Per-query matrix

| query | db+schema+aggregates, renamed cols | db+schema+aggregates, shifted params | db+schema+aggs, renamed+shifted | db-only+aggregates+noise×4 | db-only+aggregates+noise×12 | db-only+aggregates+noise×24 | db-only+aggregates+noise×48 |
|---|---|---|---|---|---|---|---|
| q01 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q02 | ✅ | ❌ fail | ❌ fail | ✅ | ✅ | ✅ | ✅ |
| q03 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q04 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q05 | ❌ fail | ✅ | ✅ | ✅ | ❌ fail | ❌ fail | ❌ timeout |
| q06 | ❌ fail | ✅ | ❌ fail | ✅ | ✅ | ✅ | ✅ |
| q07 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q08 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q09 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q10 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q11 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q12 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q13 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q14 | ✅ | ✅ | ❌ fail | ✅ | ✅ | ✅ | ✅ |
| q15 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q16 | ✅ | ❌ fail | ❌ fail | ✅ | ✅ | ✅ | ✅ |
| q17 | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ timeout | ❌ fail |
| q18 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q19 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| q20 | ❌ fail | ❌ fail | ❌ fail | ❌ fail | ❌ fail | ✅ | ✅ |
