# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 123,500 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,435 | -219 | +121 |
| PreQL vs Reference SQL | -61.2% | -21.5% | +10.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,941 | 1,459 | +482 |
| 29 | 1,526 | 1,089 | +437 |
| 35 | 2,175 | 1,745 | +430 |

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 11.095s vs 50.908s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.111s | +0.002s | +0.058s |
| Trilogy vs Reference SQL | -61.4% | +2.4% | +207.9% |
| Trilogy / Reference SQL | 0.39x | 1.02x | 3.08x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.494s | 0.046s | +0.448s |
| 23 | 0.541s | 0.277s | +0.264s |
| 73 | 0.236s | 0.029s | +0.207s |
| 76 | 0.194s | 0.039s | +0.155s |
| 28 | 0.191s | 0.036s | +0.155s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,571 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -57 | +77 |
| PreQL vs Reference SQL | -48.6% | -4.9% | +4.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,662 | 1,507 | +155 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.117s vs 0.271s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.023s | +0.024s | +0.941s |
| Trilogy vs Reference SQL | -33.1% | +78.4% | +1853.9% |
| Trilogy / Reference SQL | 0.67x | 1.78x | 19.54x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.018s | 0.051s | +0.967s |
| 97.1 | 0.951s | 0.051s | +0.900s |
| 30.alt | 0.054s | 0.030s | +0.024s |
