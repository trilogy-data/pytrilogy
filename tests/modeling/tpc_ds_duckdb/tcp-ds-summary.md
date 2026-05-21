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

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 13.163s vs 58.488s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.110s | +0.001s | +0.058s |
| Trilogy vs Reference SQL | -47.9% | +1.3% | +103.5% |
| Trilogy / Reference SQL | 0.52x | 1.01x | 2.03x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 0.536s | 0.255s | +0.280s |
| 84 | 0.315s | 0.061s | +0.254s |
| 28 | 0.173s | 0.041s | +0.132s |
| 50 | 0.313s | 0.190s | +0.122s |
| 16 | 0.117s | 0.018s | +0.100s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 2.331s vs 0.406s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.000s | +0.005s | +1.150s |
| Trilogy vs Reference SQL | +0.7% | +6.9% | +1108.6% |
| Trilogy / Reference SQL | 1.01x | 1.07x | 12.09x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.015s | 0.104s | +1.911s |
| 97.2 | 0.108s | 0.101s | +0.007s |
| 30.alt | 0.035s | 0.030s | +0.005s |
| 2.1 | 0.085s | 0.081s | +0.004s |
