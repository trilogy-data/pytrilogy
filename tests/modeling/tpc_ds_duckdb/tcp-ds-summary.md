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

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 19.930s vs 69.334s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.130s | +0.002s | +0.124s |
| Trilogy vs Reference SQL | -51.0% | +5.4% | +118.5% |
| Trilogy / Reference SQL | 0.49x | 1.05x | 2.19x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 85 | 2.406s | 1.461s | +0.945s |
| 78 | 0.826s | 0.443s | +0.383s |
| 76 | 0.400s | 0.059s | +0.341s |
| 65 | 0.491s | 0.176s | +0.316s |
| 66 | 0.565s | 0.260s | +0.305s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.299s vs 0.554s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.025s | +0.013s | +1.657s |
| Trilogy vs Reference SQL | -27.2% | +26.9% | +1141.6% |
| Trilogy / Reference SQL | 0.73x | 1.27x | 12.42x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.862s | 0.145s | +2.717s |
| 97.2 | 0.242s | 0.176s | +0.066s |
| 30.alt | 0.060s | 0.047s | +0.013s |
