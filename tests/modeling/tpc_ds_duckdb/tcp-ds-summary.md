# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 78/99 queries. Total PreQL length is 124,642 chars vs 182,326 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,352 | -216 | +114 |
| PreQL vs Reference SQL | -61.1% | -21.1% | +11.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,976 | 1,459 | +517 |
| 30 | 2,007 | 1,507 | +500 |
| 29 | 1,545 | 1,089 | +456 |

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 12.385s vs 62.366s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.118s | +0.002s | +0.042s |
| Trilogy vs Reference SQL | -53.4% | +4.0% | +79.8% |
| Trilogy / Reference SQL | 0.47x | 1.04x | 1.80x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.562s | 0.085s | +0.477s |
| 28 | 0.239s | 0.041s | +0.197s |
| 76 | 0.219s | 0.045s | +0.173s |
| 78 | 0.394s | 0.225s | +0.169s |
| 67 | 0.947s | 0.854s | +0.092s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,934 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -48 | +287 |
| PreQL vs Reference SQL | -48.6% | -4.1% | +18.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.701s vs 0.326s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.015s | +0.008s | +1.202s |
| Trilogy vs Reference SQL | -17.6% | +17.1% | +2186.9% |
| Trilogy / Reference SQL | 0.82x | 1.17x | 22.87x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.268s | 0.054s | +1.214s |
| 97.2 | 1.240s | 0.056s | +1.184s |
| 30.alt | 0.052s | 0.044s | +0.008s |
