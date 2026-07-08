# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 124,230 chars vs 182,368 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,419 | -216 | +114 |
| PreQL vs Reference SQL | -60.8% | -22.7% | +13.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,976 | 1,459 | +517 |
| 30 | 2,007 | 1,507 | +500 |
| 29 | 1,546 | 1,089 | +457 |

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 13.853s vs 82.329s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.163s | +0.003s | +0.088s |
| Trilogy vs Reference SQL | -62.2% | +4.9% | +138.5% |
| Trilogy / Reference SQL | 0.38x | 1.05x | 2.38x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.808s | 0.071s | +0.737s |
| 73 | 0.441s | 0.033s | +0.409s |
| 23 | 0.581s | 0.305s | +0.276s |
| 76 | 0.284s | 0.057s | +0.227s |
| 78 | 0.489s | 0.298s | +0.190s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,916 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -57 | +284 |
| PreQL vs Reference SQL | -48.6% | -4.9% | +18.5% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.146s vs 0.377s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.039s | +0.005s | +1.429s |
| Trilogy vs Reference SQL | -38.0% | +17.7% | +1982.8% |
| Trilogy / Reference SQL | 0.62x | 1.18x | 20.83x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.539s | 0.068s | +1.471s |
| 97.1 | 1.447s | 0.080s | +1.367s |
| 30.alt | 0.034s | 0.029s | +0.005s |
