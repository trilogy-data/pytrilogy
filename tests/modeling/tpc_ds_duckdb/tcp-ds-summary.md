# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 138,935 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -269 | +149 |
| PreQL vs Reference SQL | -57.6% | -20.4% | +11.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 08 | 20,358 | 19,155 | +1,203 |
| 89 | 1,547 | 965 | +582 |
| 76 | 2,275 | 1,708 | +567 |
| 30 | 1,888 | 1,507 | +381 |
| 35 | 2,119 | 1,745 | +374 |

Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 12.252s vs 58.540s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.140s | +0.001s | +0.066s |
| Trilogy vs Reference SQL | -63.0% | +1.6% | +93.4% |
| Trilogy / Reference SQL | 0.37x | 1.02x | 1.93x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 0.218s | 0.048s | +0.170s |
| 17 | 0.174s | 0.055s | +0.119s |
| 29 | 0.200s | 0.082s | +0.118s |
| 50 | 0.339s | 0.228s | +0.110s |
| 16 | 0.107s | 0.017s | +0.090s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.826s vs 0.380s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.003s | +0.006s | +0.847s |
| Trilogy vs Reference SQL | +6.6% | +7.3% | +879.3% |
| Trilogy / Reference SQL | 1.07x | 1.07x | 9.79x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.465s | 0.096s | +1.369s |
| 97.2 | 0.157s | 0.094s | +0.064s |
| 2.1 | 0.084s | 0.078s | +0.006s |
| 2.2 | 0.084s | 0.079s | +0.005s |
| 30.alt | 0.035s | 0.033s | +0.002s |
