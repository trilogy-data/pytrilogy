# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 76/99 queries. Total PreQL length is 123,840 chars vs 182,368 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,435 | -219 | +114 |
| PreQL vs Reference SQL | -61.2% | -22.7% | +12.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,966 | 1,459 | +507 |
| 30 | 1,997 | 1,507 | +490 |
| 29 | 1,526 | 1,089 | +437 |

Trilogy execution is faster than the reference SQL for 49/99 queries. Total Trilogy execution time is 13.113s vs 56.265s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.128s | +0.001s | +0.072s |
| Trilogy vs Reference SQL | -58.3% | +0.4% | +142.0% |
| Trilogy / Reference SQL | 0.42x | 1.00x | 2.42x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.597s | 0.085s | +0.512s |
| 23 | 0.653s | 0.297s | +0.355s |
| 73 | 0.289s | 0.034s | +0.254s |
| 76 | 0.225s | 0.048s | +0.177s |
| 28 | 0.200s | 0.044s | +0.157s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.645s vs 0.331s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.012s | +0.005s | +1.170s |
| Trilogy vs Reference SQL | -13.6% | +12.1% | +2002.0% |
| Trilogy / Reference SQL | 0.86x | 1.12x | 21.02x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.245s | 0.058s | +1.187s |
| 97.2 | 1.203s | 0.059s | +1.144s |
| 30.alt | 0.050s | 0.045s | +0.005s |
