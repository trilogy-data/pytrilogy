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

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 45.337s vs 64.270s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.077s | +0.003s | +0.098s |
| Trilogy vs Reference SQL | -44.6% | +7.7% | +155.8% |
| Trilogy / Reference SQL | 0.55x | 1.08x | 2.56x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 64 | 29.741s | 26.402s | +3.339s |
| 51 | 1.212s | 0.391s | +0.821s |
| 47 | 1.108s | 0.347s | +0.761s |
| 50 | 0.947s | 0.188s | +0.759s |
| 65 | 0.475s | 0.164s | +0.311s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 1.835s vs 0.355s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.002s | +0.885s |
| Trilogy vs Reference SQL | -0.6% | +2.9% | +1149.2% |
| Trilogy / Reference SQL | 0.99x | 1.03x | 12.49x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.545s | 0.077s | +1.468s |
| 2.1 | 0.091s | 0.082s | +0.009s |
| 2.2 | 0.087s | 0.084s | +0.002s |
| 97.2 | 0.078s | 0.077s | +0.001s |
