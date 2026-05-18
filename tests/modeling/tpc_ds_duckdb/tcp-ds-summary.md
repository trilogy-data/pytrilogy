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

Trilogy execution is faster than the reference SQL for 44/99 queries. Total Trilogy execution time is 14.232s vs 56.523s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.097s | +0.002s | +0.090s |
| Trilogy vs Reference SQL | -48.1% | +4.9% | +128.7% |
| Trilogy / Reference SQL | 0.52x | 1.05x | 2.29x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 0.550s | 0.290s | +0.260s |
| 66 | 0.486s | 0.265s | +0.221s |
| 65 | 0.424s | 0.211s | +0.213s |
| 28 | 0.241s | 0.048s | +0.193s |
| 69 | 0.442s | 0.250s | +0.193s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 1.907s vs 0.349s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.001s | +0.002s | +0.934s |
| Trilogy vs Reference SQL | -1.7% | +2.6% | +1290.4% |
| Trilogy / Reference SQL | 0.98x | 1.03x | 13.90x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.624s | 0.072s | +1.552s |
| 30.alt | 0.049s | 0.042s | +0.007s |
| 97.2 | 0.073s | 0.071s | +0.002s |
| 2.2 | 0.080s | 0.080s | +0.000s |
