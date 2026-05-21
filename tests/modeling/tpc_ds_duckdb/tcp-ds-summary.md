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

Trilogy execution is faster than the reference SQL for 38/99 queries. Total Trilogy execution time is 15.056s vs 59.338s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.122s | +0.003s | +0.080s |
| Trilogy vs Reference SQL | -48.9% | +5.5% | +117.2% |
| Trilogy / Reference SQL | 0.51x | 1.06x | 2.17x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 84 | 0.301s | 0.057s | +0.244s |
| 78 | 0.492s | 0.262s | +0.230s |
| 50 | 0.370s | 0.206s | +0.164s |
| 28 | 0.209s | 0.055s | +0.153s |
| 67 | 1.190s | 1.039s | +0.151s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.205s vs 0.430s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.004s | +0.002s | +1.066s |
| Trilogy vs Reference SQL | -4.5% | +2.1% | +1074.3% |
| Trilogy / Reference SQL | 0.95x | 1.02x | 11.74x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.869s | 0.100s | +1.770s |
| 30.alt | 0.060s | 0.050s | +0.010s |
| 97.2 | 0.101s | 0.099s | +0.002s |
