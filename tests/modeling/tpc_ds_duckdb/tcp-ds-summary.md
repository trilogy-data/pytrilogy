# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 76/99 queries. Total PreQL length is 123,980 chars vs 182,368 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,435 | -219 | +114 |
| PreQL vs Reference SQL | -60.9% | -22.7% | +12.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,966 | 1,459 | +507 |
| 30 | 1,997 | 1,507 | +490 |
| 29 | 1,526 | 1,089 | +437 |

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 13.609s vs 71.635s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.133s | +0.002s | +0.068s |
| Trilogy vs Reference SQL | -56.6% | +3.0% | +136.7% |
| Trilogy / Reference SQL | 0.43x | 1.03x | 2.37x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.660s | 0.095s | +0.565s |
| 23 | 0.726s | 0.314s | +0.412s |
| 73 | 0.275s | 0.034s | +0.240s |
| 28 | 0.231s | 0.045s | +0.186s |
| 76 | 0.220s | 0.051s | +0.169s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.693s vs 0.353s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.015s | +0.006s | +1.185s |
| Trilogy vs Reference SQL | -15.2% | +12.1% | +2078.8% |
| Trilogy / Reference SQL | 0.85x | 1.12x | 21.79x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.257s | 0.057s | +1.200s |
| 97.1 | 1.219s | 0.057s | +1.162s |
| 30.alt | 0.054s | 0.048s | +0.006s |
