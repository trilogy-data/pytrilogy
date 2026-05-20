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

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 34.534s vs 61.934s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.131s | +0.003s | +0.114s |
| Trilogy vs Reference SQL | -43.6% | +9.6% | +129.3% |
| Trilogy / Reference SQL | 0.56x | 1.10x | 2.29x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 67 | 1.607s | 0.997s | +0.610s |
| 78 | 0.637s | 0.307s | +0.330s |
| 66 | 0.459s | 0.175s | +0.285s |
| 84 | 0.321s | 0.072s | +0.249s |
| 24 | 0.541s | 0.294s | +0.247s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 1.981s vs 0.374s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.004s | +0.961s |
| Trilogy vs Reference SQL | -2.1% | +5.0% | +1158.4% |
| Trilogy / Reference SQL | 0.98x | 1.05x | 12.58x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.680s | 0.083s | +1.597s |
| 2.1 | 0.094s | 0.087s | +0.007s |
| 97.2 | 0.083s | 0.079s | +0.004s |
