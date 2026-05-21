# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 79/99 queries. Total PreQL length is 140,860 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -247 | +251 |
| PreQL vs Reference SQL | -58.0% | -20.1% | +14.0% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 08 | 20,358 | 19,155 | +1,203 |
| 28 | 2,667 | 2,056 | +611 |
| 89 | 1,547 | 965 | +582 |
| 76 | 2,275 | 1,708 | +567 |
| 64 | 4,282 | 3,783 | +499 |

Trilogy execution is faster than the reference SQL for 40/99 queries. Total Trilogy execution time is 13.674s vs 57.944s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.091s | +0.003s | +0.072s |
| Trilogy vs Reference SQL | -43.8% | +6.1% | +122.7% |
| Trilogy / Reference SQL | 0.56x | 1.06x | 2.23x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 0.528s | 0.212s | +0.316s |
| 84 | 0.327s | 0.056s | +0.271s |
| 69 | 0.411s | 0.209s | +0.202s |
| 67 | 1.257s | 1.082s | +0.175s |
| 66 | 0.338s | 0.174s | +0.164s |

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

Trilogy execution is faster than the reference SQL for 3/5 queries. Total Trilogy execution time is 1.953s vs 0.348s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.005s | -0.000s | +0.968s |
| Trilogy vs Reference SQL | -5.9% | -0.0% | +1243.3% |
| Trilogy / Reference SQL | 0.94x | 1.00x | 13.43x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.689s | 0.078s | +1.611s |
| 30.alt | 0.035s | 0.031s | +0.004s |
