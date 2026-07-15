# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 76/99 queries. Total PreQL length is 124,815 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,432 | -209 | +180 |
| PreQL vs Reference SQL | -60.9% | -21.0% | +12.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,351 | 1,745 | +606 |
| 89 | 1,566 | 965 | +601 |
| 29 | 1,526 | 1,089 | +437 |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 19.929s vs 68.395s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.153s | +0.004s | +0.148s |
| Trilogy vs Reference SQL | -50.6% | +7.3% | +173.4% |
| Trilogy / Reference SQL | 0.49x | 1.07x | 2.73x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.932s | 0.170s | +0.762s |
| 23 | 0.969s | 0.367s | +0.602s |
| 73 | 0.391s | 0.046s | +0.345s |
| 83 | 0.374s | 0.060s | +0.314s |
| 44 | 0.360s | 0.049s | +0.310s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,579 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -57 | +81 |
| PreQL vs Reference SQL | -48.6% | -4.9% | +5.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,670 | 1,507 | +163 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.422s vs 0.470s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.032s | +0.024s | +1.518s |
| Trilogy vs Reference SQL | -23.8% | +38.8% | +2188.7% |
| Trilogy / Reference SQL | 0.76x | 1.39x | 22.89x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.685s | 0.078s | +1.606s |
| 97.1 | 1.446s | 0.061s | +1.385s |
| 30.alt | 0.087s | 0.062s | +0.024s |
