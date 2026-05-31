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

Trilogy execution is faster than the reference SQL for 43/99 queries. Total Trilogy execution time is 20.335s vs 82.404s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.161s | +0.004s | +0.136s |
| Trilogy vs Reference SQL | -46.5% | +5.3% | +113.8% |
| Trilogy / Reference SQL | 0.54x | 1.05x | 2.14x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 65 | 0.488s | 0.154s | +0.333s |
| 22 | 2.646s | 2.328s | +0.318s |
| 78 | 0.545s | 0.275s | +0.271s |
| 76 | 0.275s | 0.049s | +0.226s |
| 16 | 0.250s | 0.034s | +0.216s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.047s vs 0.461s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.047s | +0.000s | +1.003s |
| Trilogy vs Reference SQL | -38.5% | +0.1% | +1159.2% |
| Trilogy / Reference SQL | 0.62x | 1.00x | 12.59x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.753s | 0.087s | +1.666s |
| 30.alt | 0.059s | 0.051s | +0.008s |
| 97.2 | 0.091s | 0.091s | +0.000s |
