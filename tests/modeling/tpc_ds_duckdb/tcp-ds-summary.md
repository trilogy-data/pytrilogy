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

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 15.824s vs 63.096s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.137s | +0.002s | +0.070s |
| Trilogy vs Reference SQL | -48.0% | +1.8% | +109.4% |
| Trilogy / Reference SQL | 0.52x | 1.02x | 2.09x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.581s | 0.108s | +0.473s |
| 16 | 0.233s | 0.022s | +0.210s |
| 76 | 0.239s | 0.052s | +0.187s |
| 28 | 0.235s | 0.049s | +0.185s |
| 78 | 0.466s | 0.285s | +0.181s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.569s vs 0.484s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.023s | +0.015s | +1.559s |
| Trilogy vs Reference SQL | -23.0% | +27.4% | +1343.5% |
| Trilogy / Reference SQL | 0.77x | 1.27x | 14.44x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.680s | 0.117s | +1.562s |
| 97.2 | 1.669s | 0.115s | +1.554s |
| 30.alt | 0.068s | 0.054s | +0.015s |
