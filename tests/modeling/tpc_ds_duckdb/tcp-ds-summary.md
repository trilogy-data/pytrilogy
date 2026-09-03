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

Trilogy execution is faster than the reference SQL for 55/99 queries. Total Trilogy execution time is 8.617s vs 8.794s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.041s | -0.002s | +0.043s |
| Trilogy vs Reference SQL | -44.1% | -3.3% | +73.6% |
| Trilogy / Reference SQL | 0.56x | 0.97x | 1.74x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.453s | 0.100s | +0.353s |
| 78 | 0.414s | 0.244s | +0.170s |
| 83 | 0.179s | 0.053s | +0.125s |
| 35 | 0.222s | 0.126s | +0.096s |
| 29 | 0.165s | 0.079s | +0.087s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.357s vs 0.286s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.011s | +0.030s | +0.514s |
| Trilogy vs Reference SQL | +23.8% | +79.8% | +704.0% |
| Trilogy / Reference SQL | 1.24x | 1.80x | 8.04x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 0.605s | 0.068s | +0.537s |
| 97.1 | 0.564s | 0.084s | +0.480s |
| 30.alt | 0.068s | 0.038s | +0.030s |
| 2.2 | 0.060s | 0.046s | +0.014s |
| 2.1 | 0.060s | 0.050s | +0.010s |
