# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 123,557 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,432 | -219 | +135 |
| PreQL vs Reference SQL | -61.2% | -21.5% | +10.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,941 | 1,459 | +482 |
| 29 | 1,526 | 1,089 | +437 |
| 35 | 2,175 | 1,745 | +430 |

Trilogy execution is faster than the reference SQL for 44/99 queries. Total Trilogy execution time is 17.417s vs 74.294s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.144s | +0.004s | +0.104s |
| Trilogy vs Reference SQL | -59.2% | +7.5% | +199.8% |
| Trilogy / Reference SQL | 0.41x | 1.08x | 3.00x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 35 | 1.174s | 0.392s | +0.781s |
| 05 | 0.690s | 0.107s | +0.582s |
| 23 | 0.777s | 0.347s | +0.429s |
| 73 | 0.310s | 0.035s | +0.274s |
| 76 | 0.260s | 0.056s | +0.204s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.232s vs 0.387s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.024s | +0.032s | +1.430s |
| Trilogy vs Reference SQL | -22.3% | +61.7% | +2300.7% |
| Trilogy / Reference SQL | 0.78x | 1.62x | 24.01x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.494s | 0.063s | +1.432s |
| 97.2 | 1.489s | 0.062s | +1.427s |
| 30.alt | 0.084s | 0.052s | +0.032s |
