# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 71/99 queries. Total PreQL length is 127,662 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,333 | -216 | +147 |
| PreQL vs Reference SQL | -60.8% | -18.9% | +17.9% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,793 | 2,056 | +737 |
| 89 | 1,659 | 965 | +694 |
| 29 | 1,620 | 1,089 | +531 |
| 81 | 1,976 | 1,459 | +517 |
| 30 | 2,007 | 1,507 | +500 |

Trilogy execution is faster than the reference SQL for 44/99 queries. Total Trilogy execution time is 14.316s vs 58.490s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.120s | +0.001s | +0.062s |
| Trilogy vs Reference SQL | -50.3% | +2.5% | +106.1% |
| Trilogy / Reference SQL | 0.50x | 1.03x | 2.06x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.534s | 0.092s | +0.441s |
| 28 | 0.222s | 0.045s | +0.176s |
| 78 | 0.422s | 0.253s | +0.169s |
| 16 | 0.186s | 0.017s | +0.168s |
| 76 | 0.216s | 0.049s | +0.167s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,976 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -21 | +293 |
| PreQL vs Reference SQL | -48.6% | -1.8% | +19.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.031s vs 0.433s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.020s | +0.011s | +1.314s |
| Trilogy vs Reference SQL | -21.4% | +22.8% | +1314.5% |
| Trilogy / Reference SQL | 0.79x | 1.23x | 14.15x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.418s | 0.101s | +1.317s |
| 97.1 | 1.408s | 0.099s | +1.309s |
| 30.alt | 0.060s | 0.049s | +0.011s |
