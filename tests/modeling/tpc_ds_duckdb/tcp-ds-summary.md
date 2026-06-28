# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 78/99 queries. Total PreQL length is 124,642 chars vs 182,326 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,352 | -216 | +114 |
| PreQL vs Reference SQL | -61.1% | -21.1% | +11.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,976 | 1,459 | +517 |
| 30 | 2,007 | 1,507 | +500 |
| 29 | 1,545 | 1,089 | +456 |

Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 14.588s vs 59.400s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.125s | +0.001s | +0.054s |
| Trilogy vs Reference SQL | -54.6% | +3.5% | +66.9% |
| Trilogy / Reference SQL | 0.45x | 1.03x | 1.67x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.893s | 0.086s | +0.806s |
| 28 | 0.246s | 0.044s | +0.202s |
| 76 | 0.211s | 0.041s | +0.170s |
| 25 | 0.225s | 0.075s | +0.150s |
| 78 | 0.308s | 0.189s | +0.119s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,934 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -48 | +287 |
| PreQL vs Reference SQL | -48.6% | -4.1% | +18.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.427s vs 0.902s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.312s | +0.003s | +1.052s |
| Trilogy vs Reference SQL | -74.1% | +8.9% | +1988.3% |
| Trilogy / Reference SQL | 0.26x | 1.09x | 20.88x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.124s | 0.053s | +1.071s |
| 97.2 | 1.076s | 0.053s | +1.024s |
| 30.alt | 0.038s | 0.035s | +0.003s |
