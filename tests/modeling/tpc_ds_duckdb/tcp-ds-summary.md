# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 76/99 queries. Total PreQL length is 124,815 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,432 | -209 | +180 |
| PreQL vs Reference SQL | -60.9% | -21.0% | +12.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,351 | 1,745 | +606 |
| 89 | 1,566 | 965 | +601 |
| 29 | 1,526 | 1,089 | +437 |

Trilogy execution is faster than the reference SQL for 40/99 queries. Total Trilogy execution time is 19.974s vs 74.140s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.167s | +0.004s | +0.136s |
| Trilogy vs Reference SQL | -55.8% | +6.8% | +173.4% |
| Trilogy / Reference SQL | 0.44x | 1.07x | 2.73x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.932s | 0.170s | +0.762s |
| 23 | 0.969s | 0.367s | +0.602s |
| 44 | 0.360s | 0.049s | +0.310s |
| 83 | 0.365s | 0.059s | +0.306s |
| 73 | 0.332s | 0.037s | +0.296s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,579 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -57 | +81 |
| PreQL vs Reference SQL | -48.6% | -4.9% | +5.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,670 | 1,507 | +163 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.937s vs 0.487s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.032s | +0.024s | +1.758s |
| Trilogy vs Reference SQL | -23.8% | +38.8% | +2229.0% |
| Trilogy / Reference SQL | 0.76x | 1.39x | 23.29x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.895s | 0.082s | +1.813s |
| 97.1 | 1.750s | 0.075s | +1.675s |
| 30.alt | 0.087s | 0.062s | +0.024s |
