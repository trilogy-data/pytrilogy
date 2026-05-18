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

Trilogy execution is faster than the reference SQL for 43/99 queries. Total Trilogy execution time is 13.107s vs 57.673s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.138s | +0.002s | +0.076s |
| Trilogy vs Reference SQL | -62.3% | +5.5% | +100.5% |
| Trilogy / Reference SQL | 0.38x | 1.06x | 2.00x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 0.431s | 0.248s | +0.183s |
| 28 | 0.229s | 0.049s | +0.180s |
| 50 | 0.328s | 0.179s | +0.149s |
| 16 | 0.131s | 0.017s | +0.113s |
| 29 | 0.208s | 0.095s | +0.113s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 1.794s vs 0.447s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.028s | +0.005s | +0.820s |
| Trilogy vs Reference SQL | -22.5% | +14.6% | +1019.1% |
| Trilogy / Reference SQL | 0.77x | 1.15x | 11.19x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.398s | 0.080s | +1.318s |
| 97.2 | 0.167s | 0.093s | +0.073s |
| 30.alt | 0.039s | 0.034s | +0.005s |
