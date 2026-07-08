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

Trilogy execution is faster than the reference SQL for 46/99 queries. Total Trilogy execution time is 15.697s vs 83.443s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.178s | +0.002s | +0.099s |
| Trilogy vs Reference SQL | -62.4% | +2.4% | +124.5% |
| Trilogy / Reference SQL | 0.38x | 1.02x | 2.24x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.751s | 0.100s | +0.652s |
| 73 | 0.440s | 0.038s | +0.402s |
| 23 | 0.740s | 0.414s | +0.327s |
| 76 | 0.293s | 0.059s | +0.234s |
| 28 | 0.265s | 0.052s | +0.213s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.880s vs 0.369s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.033s | +0.006s | +1.295s |
| Trilogy vs Reference SQL | -35.7% | +14.8% | +1844.0% |
| Trilogy / Reference SQL | 0.64x | 1.15x | 19.44x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.402s | 0.067s | +1.335s |
| 97.1 | 1.312s | 0.076s | +1.236s |
| 30.alt | 0.046s | 0.040s | +0.006s |
