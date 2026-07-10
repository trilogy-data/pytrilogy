# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 76/99 queries. Total PreQL length is 123,828 chars vs 182,368 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,435 | -220 | +114 |
| PreQL vs Reference SQL | -61.2% | -22.7% | +12.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,966 | 1,459 | +507 |
| 30 | 1,997 | 1,507 | +490 |
| 29 | 1,526 | 1,089 | +437 |

Trilogy execution is faster than the reference SQL for 47/99 queries. Total Trilogy execution time is 11.575s vs 57.576s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.119s | +0.001s | +0.055s |
| Trilogy vs Reference SQL | -64.6% | +0.7% | +127.6% |
| Trilogy / Reference SQL | 0.35x | 1.01x | 2.28x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.525s | 0.053s | +0.472s |
| 23 | 0.636s | 0.324s | +0.312s |
| 73 | 0.270s | 0.030s | +0.240s |
| 28 | 0.223s | 0.045s | +0.178s |
| 76 | 0.211s | 0.042s | +0.169s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,906 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -57 | +278 |
| PreQL vs Reference SQL | -48.6% | -4.9% | +18.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,997 | 1,507 | +490 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.475s vs 0.285s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.023s | +0.008s | +1.124s |
| Trilogy vs Reference SQL | -32.4% | +25.5% | +2082.6% |
| Trilogy / Reference SQL | 0.68x | 1.26x | 21.83x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.215s | 0.053s | +1.162s |
| 97.2 | 1.122s | 0.056s | +1.066s |
| 30.alt | 0.041s | 0.033s | +0.008s |
