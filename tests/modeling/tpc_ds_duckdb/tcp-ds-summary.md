# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 76/99 queries. Total PreQL length is 124,847 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,430 | -209 | +180 |
| PreQL vs Reference SQL | -60.9% | -21.0% | +12.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,351 | 1,745 | +606 |
| 89 | 1,566 | 965 | +601 |
| 29 | 1,530 | 1,089 | +441 |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 12.470s vs 58.094s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.120s | +0.004s | +0.059s |
| Trilogy vs Reference SQL | -59.0% | +6.6% | +226.0% |
| Trilogy / Reference SQL | 0.41x | 1.07x | 3.26x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.557s | 0.053s | +0.504s |
| 23 | 0.639s | 0.294s | +0.345s |
| 73 | 0.268s | 0.030s | +0.237s |
| 83 | 0.239s | 0.031s | +0.208s |
| 28 | 0.211s | 0.043s | +0.167s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.345s vs 0.279s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.026s | +0.027s | +1.050s |
| Trilogy vs Reference SQL | -35.2% | +96.2% | +2023.4% |
| Trilogy / Reference SQL | 0.65x | 1.96x | 21.23x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.122s | 0.053s | +1.070s |
| 97.1 | 1.072s | 0.051s | +1.021s |
| 30.alt | 0.055s | 0.028s | +0.027s |
