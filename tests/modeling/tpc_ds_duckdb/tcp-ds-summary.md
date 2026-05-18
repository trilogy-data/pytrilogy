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

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 18.095s vs 64.056s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.104s | +0.004s | +0.124s |
| Trilogy vs Reference SQL | -44.7% | +8.9% | +116.3% |
| Trilogy / Reference SQL | 0.55x | 1.09x | 2.16x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 22 | 1.944s | 1.162s | +0.782s |
| 85 | 1.597s | 1.081s | +0.516s |
| 65 | 0.458s | 0.184s | +0.275s |
| 78 | 0.688s | 0.416s | +0.272s |
| 66 | 0.462s | 0.222s | +0.240s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 4.146s vs 0.789s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.002s | +0.004s | +2.007s |
| Trilogy vs Reference SQL | +3.3% | +6.7% | +398.2% |
| Trilogy / Reference SQL | 1.03x | 1.07x | 4.98x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 3.852s | 0.510s | +3.342s |
| 2.1 | 0.084s | 0.078s | +0.005s |
| 2.2 | 0.080s | 0.076s | +0.004s |
| 30.alt | 0.034s | 0.030s | +0.004s |
| 97.2 | 0.097s | 0.095s | +0.002s |
