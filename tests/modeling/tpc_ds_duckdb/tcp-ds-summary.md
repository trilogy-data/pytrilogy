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

Trilogy execution is faster than the reference SQL for 48/99 queries. Total Trilogy execution time is 14.360s vs 12.473s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.059s | +0.000s | +0.067s |
| Trilogy vs Reference SQL | -43.9% | +0.7% | +109.2% |
| Trilogy / Reference SQL | 0.56x | 1.01x | 2.09x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 75 | 1.190s | 0.101s | +1.089s |
| 78 | 1.280s | 0.334s | +0.946s |
| 05 | 0.625s | 0.179s | +0.446s |
| 23 | 0.785s | 0.439s | +0.345s |
| 70 | 0.321s | 0.056s | +0.265s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.795s vs 0.414s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.023s | +0.028s | +0.664s |
| Trilogy vs Reference SQL | +32.4% | +42.1% | +695.3% |
| Trilogy / Reference SQL | 1.32x | 1.42x | 7.95x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 0.795s | 0.087s | +0.708s |
| 97.1 | 0.714s | 0.117s | +0.598s |
| 30.alt | 0.093s | 0.066s | +0.028s |
| 2.1 | 0.097s | 0.071s | +0.025s |
| 2.2 | 0.096s | 0.073s | +0.022s |
