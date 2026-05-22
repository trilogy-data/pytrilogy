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

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 14.660s vs 65.730s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.141s | +0.002s | +0.054s |
| Trilogy vs Reference SQL | -50.4% | +5.9% | +108.2% |
| Trilogy / Reference SQL | 0.50x | 1.06x | 2.08x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 84 | 0.369s | 0.059s | +0.309s |
| 78 | 0.448s | 0.249s | +0.199s |
| 28 | 0.217s | 0.051s | +0.166s |
| 67 | 1.314s | 1.154s | +0.160s |
| 16 | 0.125s | 0.018s | +0.107s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.135s vs 0.433s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.018s | +0.003s | +1.037s |
| Trilogy vs Reference SQL | -15.3% | +3.1% | +1223.7% |
| Trilogy / Reference SQL | 0.85x | 1.03x | 13.24x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.811s | 0.085s | +1.726s |
| 30.alt | 0.040s | 0.036s | +0.004s |
| 97.2 | 0.087s | 0.084s | +0.003s |
