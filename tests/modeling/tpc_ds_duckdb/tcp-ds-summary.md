# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 126,006 chars vs 182,494 reference SQL chars.

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

Trilogy execution is faster than the reference SQL for 38/99 queries. Total Trilogy execution time is 5.193s vs 5.206s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.022s | +0.001s | +0.027s |
| Trilogy vs Reference SQL | -41.9% | +4.4% | +68.8% |
| Trilogy / Reference SQL | 0.58x | 1.04x | 1.69x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.274s | 0.042s | +0.232s |
| 78 | 0.284s | 0.142s | +0.143s |
| 23 | 0.242s | 0.152s | +0.090s |
| 83 | 0.117s | 0.029s | +0.088s |
| 64 | 0.089s | 0.030s | +0.059s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 0.891s vs 0.160s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.002s | +0.017s | +0.355s |
| Trilogy vs Reference SQL | +8.8% | +70.1% | +824.1% |
| Trilogy / Reference SQL | 1.09x | 1.70x | 9.24x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 0.401s | 0.044s | +0.357s |
| 97.2 | 0.394s | 0.042s | +0.352s |
| 30.alt | 0.042s | 0.025s | +0.017s |
| 2.1 | 0.027s | 0.024s | +0.003s |
| 2.2 | 0.027s | 0.025s | +0.001s |
