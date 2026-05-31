# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 71/99 queries. Total PreQL length is 131,305 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,333 | -200 | +257 |
| PreQL vs Reference SQL | -59.5% | -18.4% | +20.0% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,793 | 2,056 | +737 |
| 89 | 1,659 | 965 | +694 |
| 76 | 2,347 | 1,708 | +639 |
| 64 | 4,403 | 3,783 | +620 |
| 29 | 1,626 | 1,089 | +537 |

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 14.950s vs 59.110s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.128s | +0.003s | +0.052s |
| Trilogy vs Reference SQL | -48.2% | +5.7% | +91.3% |
| Trilogy / Reference SQL | 0.52x | 1.06x | 1.91x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 67 | 1.652s | 1.049s | +0.603s |
| 76 | 0.283s | 0.050s | +0.232s |
| 78 | 0.473s | 0.251s | +0.222s |
| 25 | 0.182s | 0.048s | +0.134s |
| 28 | 0.179s | 0.048s | +0.130s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 3/5 queries. Total PreQL length is 7,602 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,202 | -9 | +493 |
| PreQL vs Reference SQL | -46.8% | -0.8% | +38.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |
| 97.2 | 1,642 | 1,159 | +483 |

Trilogy execution is faster than the reference SQL for 3/5 queries. Total Trilogy execution time is 2.154s vs 0.441s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.031s | -0.002s | +1.063s |
| Trilogy vs Reference SQL | -30.3% | -2.0% | +1105.0% |
| Trilogy / Reference SQL | 0.70x | 0.98x | 12.05x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.861s | 0.097s | +1.764s |
| 30.alt | 0.059s | 0.048s | +0.011s |
