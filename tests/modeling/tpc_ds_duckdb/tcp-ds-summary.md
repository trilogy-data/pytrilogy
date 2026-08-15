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

Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 7.365s vs 7.115s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.036s | +0.001s | +0.029s |
| Trilogy vs Reference SQL | -40.5% | +2.5% | +83.0% |
| Trilogy / Reference SQL | 0.60x | 1.03x | 1.83x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 23 | 0.582s | 0.222s | +0.360s |
| 05 | 0.388s | 0.067s | +0.321s |
| 78 | 0.392s | 0.199s | +0.193s |
| 83 | 0.145s | 0.039s | +0.106s |
| 35 | 0.168s | 0.094s | +0.075s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.200s vs 0.222s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.007s | +0.025s | +0.471s |
| Trilogy vs Reference SQL | +19.0% | +84.2% | +782.3% |
| Trilogy / Reference SQL | 1.19x | 1.84x | 8.82x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 0.535s | 0.059s | +0.476s |
| 97.1 | 0.527s | 0.062s | +0.464s |
| 30.alt | 0.055s | 0.030s | +0.025s |
| 2.2 | 0.042s | 0.034s | +0.008s |
| 2.1 | 0.042s | 0.036s | +0.006s |
