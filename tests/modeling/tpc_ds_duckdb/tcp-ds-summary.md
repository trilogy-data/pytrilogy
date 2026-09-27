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

Trilogy execution is faster than the reference SQL for 47/99 queries. Total Trilogy execution time is 9.492s vs 9.018s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.039s | +0.000s | +0.059s |
| Trilogy vs Reference SQL | -42.9% | +1.1% | +76.5% |
| Trilogy / Reference SQL | 0.57x | 1.01x | 1.77x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 0.779s | 0.362s | +0.417s |
| 05 | 0.370s | 0.067s | +0.302s |
| 67 | 1.853s | 1.553s | +0.299s |
| 64 | 0.169s | 0.047s | +0.122s |
| 69 | 0.263s | 0.162s | +0.101s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 2.990s vs 0.313s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.010s | +0.019s | +1.352s |
| Trilogy vs Reference SQL | +30.2% | +65.3% | +1273.1% |
| Trilogy / Reference SQL | 1.30x | 1.65x | 13.73x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.589s | 0.104s | +1.485s |
| 97.1 | 1.264s | 0.111s | +1.152s |
| 30.alt | 0.049s | 0.030s | +0.019s |
| 2.2 | 0.044s | 0.033s | +0.011s |
| 2.1 | 0.045s | 0.035s | +0.010s |
