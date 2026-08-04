# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 126,332 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -209 | +206 |
| PreQL vs Reference SQL | -60.0% | -21.8% | +14.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 64 | 4,382 | 3,783 | +599 |

Trilogy execution is faster than the reference SQL for 50/99 queries. Total Trilogy execution time is 7.649s vs 7.761s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.027s | -0.000s | +0.035s |
| Trilogy vs Reference SQL | -43.8% | -0.2% | +84.1% |
| Trilogy / Reference SQL | 0.56x | 1.00x | 1.84x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 23 | 0.621s | 0.202s | +0.418s |
| 05 | 0.362s | 0.060s | +0.302s |
| 78 | 0.378s | 0.200s | +0.178s |
| 70 | 0.143s | 0.029s | +0.114s |
| 35 | 0.187s | 0.096s | +0.091s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,643 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,233 | -39 | +89 |
| PreQL vs Reference SQL | -48.0% | -3.4% | +5.7% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,670 | 1,507 | +163 |

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.171s vs 0.213s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.006s | +0.023s | +0.462s |
| Trilogy vs Reference SQL | +18.0% | +64.8% | +836.1% |
| Trilogy / Reference SQL | 1.18x | 1.65x | 9.36x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 0.515s | 0.051s | +0.463s |
| 97.1 | 0.522s | 0.062s | +0.459s |
| 30.alt | 0.059s | 0.036s | +0.023s |
| 2.2 | 0.039s | 0.032s | +0.008s |
| 2.1 | 0.036s | 0.032s | +0.004s |
