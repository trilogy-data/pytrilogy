# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 79/99 queries. Total PreQL length is 140,893 chars vs 184,304 reference SQL chars.

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

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 19.594s vs 67.851s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.111s | +0.003s | +0.111s |
| Trilogy vs Reference SQL | -43.2% | +7.3% | +155.9% |
| Trilogy / Reference SQL | 0.57x | 1.07x | 2.56x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 89 | 1.030s | 0.225s | +0.805s |
| 67 | 1.624s | 1.170s | +0.454s |
| 84 | 0.490s | 0.089s | +0.401s |
| 78 | 0.943s | 0.579s | +0.364s |
| 83 | 0.320s | 0.038s | +0.282s |

## Alternative Queries

Queries: 12

PreQL is shorter than the reference SQL for 10/12 queries. Total PreQL length is 15,245 chars vs 26,603 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -2,110 | -876 | +87 |
| PreQL vs Reference SQL | -56.9% | -42.6% | +7.0% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,773 | 1,507 | +266 |
| 97.2 | 1,262 | 1,159 | +103 |

Trilogy execution is faster than the reference SQL for 4/12 queries. Total Trilogy execution time is 4.521s vs 1.534s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.039s | +0.004s | +0.484s |
| Trilogy vs Reference SQL | -12.8% | +9.5% | +742.8% |
| Trilogy / Reference SQL | 0.87x | 1.09x | 8.43x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.394s | 0.121s | +2.273s |
| 14.then_where | 0.911s | 0.406s | +0.505s |
| 83.then_where | 0.330s | 0.036s | +0.294s |
| 58.then_where | 0.098s | 0.056s | +0.042s |
| 97.2 | 0.107s | 0.101s | +0.006s |
