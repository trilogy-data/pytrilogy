# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 75/99 queries. Total PreQL length is 131,131 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,269 | -207 | +251 |
| PreQL vs Reference SQL | -53.5% | -17.0% | +14.2% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,821 | 2,056 | +765 |
| 76 | 2,344 | 1,708 | +636 |
| 89 | 1,566 | 965 | +601 |
| 64 | 4,256 | 3,783 | +473 |
| 29 | 1,537 | 1,089 | +448 |

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 17.902s vs 69.077s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.084s | +0.003s | +0.100s |
| Trilogy vs Reference SQL | -41.2% | +6.7% | +149.8% |
| Trilogy / Reference SQL | 0.59x | 1.07x | 2.50x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 65 | 0.705s | 0.230s | +0.475s |
| 66 | 0.702s | 0.262s | +0.440s |
| 84 | 0.358s | 0.058s | +0.301s |
| 78 | 0.569s | 0.308s | +0.260s |
| 69 | 0.469s | 0.258s | +0.211s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 3/5 queries. Total PreQL length is 7,272 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,202 | -44 | +356 |
| PreQL vs Reference SQL | -46.8% | -3.8% | +28.5% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1,563 | 1,159 | +404 |
| 30.alt | 1,791 | 1,507 | +284 |

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 2.331s vs 0.445s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.000s | +0.002s | +1.128s |
| Trilogy vs Reference SQL | +0.3% | +2.7% | +1046.0% |
| Trilogy / Reference SQL | 1.00x | 1.03x | 11.46x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.984s | 0.108s | +1.876s |
| 30.alt | 0.055s | 0.047s | +0.007s |
| 2.1 | 0.092s | 0.090s | +0.002s |
| 2.2 | 0.094s | 0.092s | +0.002s |
