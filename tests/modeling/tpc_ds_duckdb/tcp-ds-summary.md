# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 71/99 queries. Total PreQL length is 131,314 chars vs 182,502 reference SQL chars.

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

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 11.922s vs 52.329s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.096s | +0.002s | +0.043s |
| Trilogy vs Reference SQL | -47.5% | +4.6% | +106.9% |
| Trilogy / Reference SQL | 0.53x | 1.05x | 2.07x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 0.412s | 0.202s | +0.210s |
| 76 | 0.237s | 0.040s | +0.197s |
| 28 | 0.173s | 0.043s | +0.130s |
| 25 | 0.152s | 0.040s | +0.111s |
| 16 | 0.122s | 0.018s | +0.104s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 1.755s vs 0.350s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.024s | +0.001s | +0.870s |
| Trilogy vs Reference SQL | -30.7% | +0.7% | +1121.0% |
| Trilogy / Reference SQL | 0.69x | 1.01x | 12.21x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.525s | 0.078s | +1.448s |
| 30.alt | 0.040s | 0.036s | +0.004s |
| 97.2 | 0.079s | 0.079s | +0.001s |
