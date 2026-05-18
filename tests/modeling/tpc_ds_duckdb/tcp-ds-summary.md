# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 138,530 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -269 | +149 |
| PreQL vs Reference SQL | -58.0% | -20.4% | +11.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 08 | 20,358 | 19,155 | +1,203 |
| 89 | 1,547 | 965 | +582 |
| 76 | 2,275 | 1,708 | +567 |
| 35 | 2,119 | 1,745 | +374 |
| 86 | 1,200 | 847 | +353 |

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 14.637s vs 62.225s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.109s | +0.003s | +0.088s |
| Trilogy vs Reference SQL | -53.0% | +6.6% | +124.5% |
| Trilogy / Reference SQL | 0.47x | 1.07x | 2.25x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 22 | 1.944s | 1.162s | +0.782s |
| 69 | 0.546s | 0.240s | +0.306s |
| 66 | 0.413s | 0.151s | +0.262s |
| 78 | 0.488s | 0.231s | +0.257s |
| 57 | 0.511s | 0.283s | +0.228s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.733s vs 0.328s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.003s | +0.004s | +0.836s |
| Trilogy vs Reference SQL | +3.9% | +6.7% | +1156.0% |
| Trilogy / Reference SQL | 1.04x | 1.07x | 12.56x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.463s | 0.072s | +1.390s |
| 2.1 | 0.084s | 0.078s | +0.005s |
| 2.2 | 0.080s | 0.076s | +0.004s |
| 30.alt | 0.034s | 0.030s | +0.004s |
| 97.2 | 0.073s | 0.071s | +0.002s |
