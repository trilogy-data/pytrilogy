# TPC-DS Result Summary

Timing fingerprint: `x86_64-x86_64-4`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 126,749 chars vs 182,755 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -209 | +206 |
| PreQL vs Reference SQL | -59.5% | -21.8% | +14.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,098 | 1,459 | +639 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 64 | 4,382 | 3,783 | +599 |

Trilogy execution is faster than the reference SQL for 24/99 queries. Total Trilogy execution time is 4.407s vs 3.434s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.004s | +0.003s | +0.032s |
| Trilogy vs Reference SQL | -14.7% | +12.0% | +177.5% |
| Trilogy / Reference SQL | 0.85x | 1.12x | 2.77x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 0.178s | 0.062s | +0.116s |
| 78 | 0.251s | 0.148s | +0.103s |
| 83 | 0.087s | 0.015s | +0.072s |
| 23 | 0.194s | 0.137s | +0.057s |
| 67 | 0.316s | 0.259s | +0.057s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,602 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,233 | -39 | +64 |
| PreQL vs Reference SQL | -48.0% | -3.4% | +4.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,629 | 1,507 | +122 |

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 1.212s vs 0.146s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.013s | +0.534s |
| Trilogy vs Reference SQL | -0.3% | +62.3% | +1469.0% |
| Trilogy / Reference SQL | 1.00x | 1.62x | 15.69x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 0.603s | 0.036s | +0.566s |
| 97.2 | 0.522s | 0.036s | +0.486s |
| 30.alt | 0.034s | 0.021s | +0.013s |
| 2.1 | 0.029s | 0.027s | +0.002s |
