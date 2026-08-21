# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 74/99 queries. Total PreQL length is 125,717 chars vs 182,755 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -210 | +152 |
| PreQL vs Reference SQL | -60.0% | -21.8% | +13.0% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,098 | 1,459 | +639 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 64 | 4,382 | 3,783 | +599 |

Trilogy execution is faster than the reference SQL for 51/99 queries. Total Trilogy execution time is 12.242s vs 10.431s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.053s | -0.001s | +0.061s |
| Trilogy vs Reference SQL | -42.6% | -2.3% | +83.7% |
| Trilogy / Reference SQL | 0.57x | 0.98x | 1.84x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 29 | 1.052s | 0.056s | +0.996s |
| 64 | 0.868s | 0.083s | +0.785s |
| 05 | 0.575s | 0.109s | +0.466s |
| 78 | 0.555s | 0.272s | +0.283s |
| 83 | 0.234s | 0.060s | +0.174s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.517s vs 0.322s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.012s | +0.038s | +0.566s |
| Trilogy vs Reference SQL | +19.4% | +74.3% | +776.9% |
| Trilogy / Reference SQL | 1.19x | 1.74x | 8.77x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 0.643s | 0.073s | +0.570s |
| 97.1 | 0.633s | 0.072s | +0.561s |
| 30.alt | 0.090s | 0.051s | +0.038s |
| 2.1 | 0.081s | 0.063s | +0.018s |
| 2.2 | 0.070s | 0.062s | +0.008s |
