# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 79/99 queries. Total PreQL length is 139,346 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -261 | +181 |
| PreQL vs Reference SQL | -58.0% | -20.4% | +12.5% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 08 | 20,358 | 19,155 | +1,203 |
| 28 | 2,667 | 2,056 | +611 |
| 89 | 1,547 | 965 | +582 |
| 76 | 2,275 | 1,708 | +567 |
| 29 | 1,532 | 1,089 | +443 |
| 35 | 2,164 | 1,745 | +419 |

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 36.877s vs 64.365s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.139s | +0.003s | +0.162s |
| Trilogy vs Reference SQL | -44.3% | +7.6% | +130.3% |
| Trilogy / Reference SQL | 0.56x | 1.08x | 2.30x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 67 | 1.607s | 0.997s | +0.610s |
| 37 | 1.289s | 0.797s | +0.492s |
| 78 | 0.560s | 0.252s | +0.307s |
| 66 | 0.459s | 0.175s | +0.285s |
| 84 | 0.317s | 0.065s | +0.252s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 1.970s vs 0.366s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.000s | +0.003s | +0.960s |
| Trilogy vs Reference SQL | -0.4% | +5.0% | +1159.1% |
| Trilogy / Reference SQL | 1.00x | 1.05x | 12.59x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.680s | 0.083s | +1.597s |
| 97.2 | 0.083s | 0.079s | +0.004s |
| 30.alt | 0.037s | 0.034s | +0.003s |
| 2.2 | 0.085s | 0.084s | +0.000s |
