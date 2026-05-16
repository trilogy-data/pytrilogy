# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 138,617 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -274 | +149 |
| PreQL vs Reference SQL | -57.6% | -20.9% | +11.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 08 | 20,358 | 19,155 | +1,203 |
| 89 | 1,547 | 965 | +582 |
| 76 | 2,275 | 1,708 | +567 |
| 35 | 2,119 | 1,745 | +374 |
| 86 | 1,200 | 847 | +353 |

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 17.422s vs 96.899s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.148s | +0.005s | +0.103s |
| Trilogy vs Reference SQL | -65.0% | +7.4% | +122.8% |
| Trilogy / Reference SQL | 0.35x | 1.07x | 2.23x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 0.380s | 0.114s | +0.266s |
| 50 | 0.537s | 0.335s | +0.201s |
| 81 | 0.234s | 0.045s | +0.189s |
| 78 | 0.566s | 0.380s | +0.187s |
| 68 | 0.303s | 0.130s | +0.173s |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,323 | -656 | -4 |
| PreQL vs Reference SQL | -51.5% | -31.6% | -0.4% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1,262 | 1,159 | +103 |

Trilogy execution is faster than the reference SQL for 0/4 queries. Total Trilogy execution time is 1.576s vs 0.308s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.002s | +0.036s | +0.857s |
| Trilogy vs Reference SQL | +2.6% | +47.4% | +1124.5% |
| Trilogy / Reference SQL | 1.03x | 1.47x | 12.25x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.271s | 0.076s | +1.194s |
| 97.2 | 0.144s | 0.075s | +0.069s |
| 2.2 | 0.079s | 0.077s | +0.002s |
| 2.1 | 0.083s | 0.081s | +0.002s |
