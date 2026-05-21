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
| 89 | 1,547 | 965 | +582 |
| 76 | 2,275 | 1,708 | +567 |
| 29 | 1,532 | 1,089 | +443 |
| 35 | 2,164 | 1,745 | +419 |

Trilogy execution is faster than the reference SQL for 40/99 queries. Total Trilogy execution time is 29.906s vs 55.159s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.104s | +0.002s | +0.060s |
| Trilogy vs Reference SQL | -40.8% | +2.0% | +127.7% |
| Trilogy / Reference SQL | 0.59x | 1.02x | 2.28x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 84 | 0.303s | 0.056s | +0.247s |
| 78 | 0.399s | 0.230s | +0.170s |
| 28 | 0.207s | 0.047s | +0.160s |
| 16 | 0.148s | 0.021s | +0.127s |
| 25 | 0.145s | 0.047s | +0.098s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 1.983s vs 0.397s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.004s | +0.002s | +0.953s |
| Trilogy vs Reference SQL | -4.5% | +1.7% | +1010.2% |
| Trilogy / Reference SQL | 0.96x | 1.02x | 11.10x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.676s | 0.095s | +1.581s |
| 30.alt | 0.053s | 0.042s | +0.011s |
| 97.2 | 0.095s | 0.094s | +0.002s |
