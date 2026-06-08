# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 70/99 queries. Total PreQL length is 129,180 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,333 | -214 | +186 |
| PreQL vs Reference SQL | -60.7% | -18.9% | +19.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,793 | 2,056 | +737 |
| 89 | 1,659 | 965 | +694 |
| 76 | 2,347 | 1,708 | +639 |
| 29 | 1,620 | 1,089 | +531 |
| 81 | 1,976 | 1,459 | +517 |

Trilogy execution is faster than the reference SQL for 44/99 queries. Total Trilogy execution time is 17.903s vs 71.067s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.128s | +0.002s | +0.076s |
| Trilogy vs Reference SQL | -49.4% | +2.9% | +96.7% |
| Trilogy / Reference SQL | 0.51x | 1.03x | 1.97x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.706s | 0.105s | +0.601s |
| 78 | 0.596s | 0.344s | +0.252s |
| 76 | 0.313s | 0.062s | +0.251s |
| 67 | 1.549s | 1.380s | +0.169s |
| 25 | 0.218s | 0.054s | +0.164s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 3/5 queries. Total PreQL length is 7,440 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -17 | +477 |
| PreQL vs Reference SQL | -48.6% | -1.5% | +36.2% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |
| 97.2 | 1,602 | 1,159 | +443 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.423s vs 0.499s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.032s | +0.006s | +1.186s |
| Trilogy vs Reference SQL | -29.2% | +5.4% | +1049.1% |
| Trilogy / Reference SQL | 0.71x | 1.05x | 11.49x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.083s | 0.113s | +1.970s |
| 30.alt | 0.069s | 0.058s | +0.011s |
| 97.2 | 0.115s | 0.109s | +0.006s |
