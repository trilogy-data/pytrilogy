# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 79/99 queries. Total PreQL length is 139,663 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -261 | +181 |
| PreQL vs Reference SQL | -58.0% | -20.4% | +13.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 08 | 20,358 | 19,155 | +1,203 |
| 28 | 2,667 | 2,056 | +611 |
| 89 | 1,547 | 965 | +582 |
| 76 | 2,275 | 1,708 | +567 |
| 29 | 1,532 | 1,089 | +443 |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 38.025s vs 61.016s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.076s | +0.003s | +0.085s |
| Trilogy vs Reference SQL | -41.4% | +6.0% | +116.0% |
| Trilogy / Reference SQL | 0.59x | 1.06x | 2.16x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 64 | 24.651s | 23.585s | +1.065s |
| 22 | 1.226s | 0.802s | +0.424s |
| 84 | 0.291s | 0.051s | +0.240s |
| 67 | 1.226s | 1.002s | +0.225s |
| 69 | 0.430s | 0.206s | +0.224s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 3/5 queries. Total PreQL length is 6,578 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,285 | -255 | +201 |
| PreQL vs Reference SQL | -50.1% | -22.0% | +14.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,773 | 1,507 | +266 |
| 97.2 | 1,262 | 1,159 | +103 |

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 1.610s vs 0.322s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.001s | +0.004s | +0.768s |
| Trilogy vs Reference SQL | +1.1% | +5.2% | +1103.4% |
| Trilogy / Reference SQL | 1.01x | 1.05x | 12.03x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.344s | 0.070s | +1.274s |
| 30.alt | 0.039s | 0.031s | +0.008s |
| 2.1 | 0.077s | 0.073s | +0.004s |
| 97.2 | 0.077s | 0.074s | +0.003s |
