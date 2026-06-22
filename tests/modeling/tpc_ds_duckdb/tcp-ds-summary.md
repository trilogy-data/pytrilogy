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

Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 16.315s vs 66.078s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.133s | +0.000s | +0.060s |
| Trilogy vs Reference SQL | -48.5% | +1.1% | +112.3% |
| Trilogy / Reference SQL | 0.51x | 1.01x | 2.12x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 78 | 1.489s | 0.268s | +1.221s |
| 05 | 0.554s | 0.103s | +0.451s |
| 16 | 0.211s | 0.021s | +0.190s |
| 28 | 0.228s | 0.047s | +0.181s |
| 76 | 0.225s | 0.048s | +0.176s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 3.578s vs 0.432s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.017s | +0.029s | +1.542s |
| Trilogy vs Reference SQL | +24.6% | +34.0% | +1493.7% |
| Trilogy / Reference SQL | 1.25x | 1.34x | 15.94x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.654s | 0.103s | +1.551s |
| 97.1 | 1.633s | 0.103s | +1.530s |
| 2.2 | 0.115s | 0.086s | +0.029s |
| 2.1 | 0.115s | 0.090s | +0.025s |
| 30.alt | 0.061s | 0.050s | +0.011s |
