# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 71/99 queries. Total PreQL length is 131,312 chars vs 182,502 reference SQL chars.

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

Trilogy execution is faster than the reference SQL for 40/99 queries. Total Trilogy execution time is 14.002s vs 55.876s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.115s | +0.003s | +0.056s |
| Trilogy vs Reference SQL | -44.4% | +6.2% | +91.8% |
| Trilogy / Reference SQL | 0.56x | 1.06x | 1.92x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 0.498s | 0.256s | +0.242s |
| 76 | 0.271s | 0.049s | +0.222s |
| 25 | 0.181s | 0.047s | +0.135s |
| 28 | 0.176s | 0.045s | +0.131s |
| 16 | 0.134s | 0.020s | +0.114s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.140s vs 0.411s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.022s | +0.003s | +1.058s |
| Trilogy vs Reference SQL | -25.6% | +3.2% | +1044.8% |
| Trilogy / Reference SQL | 0.74x | 1.03x | 11.45x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.857s | 0.102s | +1.755s |
| 30.alt | 0.059s | 0.046s | +0.013s |
| 97.2 | 0.099s | 0.096s | +0.003s |
