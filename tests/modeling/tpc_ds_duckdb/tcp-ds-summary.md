# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 80/99 queries. Total PreQL length is 138,883 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -261 | +149 |
| PreQL vs Reference SQL | -58.0% | -20.4% | +11.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 08 | 20,358 | 19,155 | +1,203 |
| 89 | 1,547 | 965 | +582 |
| 76 | 2,275 | 1,708 | +567 |
| 35 | 2,164 | 1,745 | +419 |
| 86 | 1,200 | 847 | +353 |

Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 12.558s vs 54.395s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.111s | +0.001s | +0.065s |
| Trilogy vs Reference SQL | -52.7% | +2.3% | +127.9% |
| Trilogy / Reference SQL | 0.47x | 1.02x | 2.28x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 0.425s | 0.211s | +0.213s |
| 66 | 0.342s | 0.171s | +0.171s |
| 28 | 0.200s | 0.043s | +0.157s |
| 50 | 0.308s | 0.176s | +0.133s |
| 67 | 1.012s | 0.880s | +0.132s |

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

Trilogy execution is faster than the reference SQL for 3/5 queries. Total Trilogy execution time is 1.814s vs 0.364s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.006s | -0.002s | +0.877s |
| Trilogy vs Reference SQL | -7.3% | -2.4% | +1077.8% |
| Trilogy / Reference SQL | 0.93x | 0.98x | 11.78x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.539s | 0.082s | +1.457s |
| 30.alt | 0.049s | 0.043s | +0.006s |
