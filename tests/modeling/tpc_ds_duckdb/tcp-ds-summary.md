# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 76/99 queries. Total PreQL length is 124,537 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,430 | -209 | +166 |
| PreQL vs Reference SQL | -60.9% | -21.0% | +12.5% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,351 | 1,745 | +606 |
| 89 | 1,566 | 965 | +601 |
| 29 | 1,530 | 1,089 | +441 |

Trilogy execution is faster than the reference SQL for 37/99 queries. Total Trilogy execution time is 13.451s vs 55.952s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.090s | +0.005s | +0.062s |
| Trilogy vs Reference SQL | -41.7% | +8.3% | +228.5% |
| Trilogy / Reference SQL | 0.58x | 1.08x | 3.28x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 15 | 0.759s | 0.122s | +0.637s |
| 05 | 0.512s | 0.046s | +0.466s |
| 23 | 0.657s | 0.287s | +0.370s |
| 73 | 0.251s | 0.028s | +0.223s |
| 83 | 0.221s | 0.031s | +0.190s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,603 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,243 | -49 | +85 |
| PreQL vs Reference SQL | -48.4% | -4.2% | +5.4% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,670 | 1,507 | +163 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.354s vs 0.272s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.025s | +0.026s | +1.056s |
| Trilogy vs Reference SQL | -35.3% | +85.6% | +2070.6% |
| Trilogy / Reference SQL | 0.65x | 1.86x | 21.71x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.121s | 0.051s | +1.069s |
| 97.2 | 1.087s | 0.051s | +1.036s |
| 30.alt | 0.056s | 0.030s | +0.026s |
