# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 70/99 queries. Total PreQL length is 129,180 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,333 | -214 | +186 |
| PreQL vs Reference SQL | -60.7% | -18.9% | +19.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,793 | 2,056 | +737 |
| 89 | 1,659 | 965 | +694 |
| 76 | 2,347 | 1,708 | +639 |
| 29 | 1,620 | 1,089 | +531 |
| 81 | 1,976 | 1,459 | +517 |

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 21.560s vs 89.436s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.142s | +0.005s | +0.123s |
| Trilogy vs Reference SQL | -48.2% | +5.1% | +96.3% |
| Trilogy / Reference SQL | 0.52x | 1.05x | 1.96x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.920s | 0.179s | +0.741s |
| 78 | 0.731s | 0.379s | +0.351s |
| 76 | 0.396s | 0.075s | +0.320s |
| 28 | 0.316s | 0.069s | +0.247s |
| 25 | 0.264s | 0.073s | +0.191s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 3/5 queries. Total PreQL length is 7,440 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -17 | +477 |
| PreQL vs Reference SQL | -48.6% | -1.5% | +36.2% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |
| 97.2 | 1,602 | 1,159 | +443 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.817s vs 0.631s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.049s | +0.001s | +1.365s |
| Trilogy vs Reference SQL | -33.8% | +0.9% | +986.0% |
| Trilogy / Reference SQL | 0.66x | 1.01x | 10.86x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.407s | 0.139s | +2.269s |
| 30.alt | 0.077s | 0.066s | +0.011s |
| 97.2 | 0.142s | 0.141s | +0.001s |
