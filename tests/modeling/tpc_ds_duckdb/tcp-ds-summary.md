# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 126,153 chars vs 182,755 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -210 | +206 |
| PreQL vs Reference SQL | -60.0% | -21.8% | +14.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,098 | 1,459 | +639 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 64 | 4,382 | 3,783 | +599 |

Trilogy execution is faster than the reference SQL for 49/99 queries. Total Trilogy execution time is 13.624s vs 13.183s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.050s | +0.001s | +0.067s |
| Trilogy vs Reference SQL | -39.9% | +0.9% | +59.7% |
| Trilogy / Reference SQL | 0.60x | 1.01x | 1.60x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 1.116s | 0.121s | +0.995s |
| 78 | 0.733s | 0.464s | +0.269s |
| 64 | 0.213s | 0.056s | +0.157s |
| 29 | 0.284s | 0.166s | +0.117s |
| 75 | 0.189s | 0.098s | +0.091s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 2.576s vs 0.439s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.012s | +0.022s | +1.062s |
| Trilogy vs Reference SQL | +14.3% | +41.3% | +990.1% |
| Trilogy / Reference SQL | 1.14x | 1.41x | 10.90x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.244s | 0.114s | +1.130s |
| 97.2 | 1.057s | 0.097s | +0.960s |
| 30.alt | 0.075s | 0.053s | +0.022s |
| 2.1 | 0.084s | 0.068s | +0.015s |
| 2.2 | 0.116s | 0.106s | +0.009s |
