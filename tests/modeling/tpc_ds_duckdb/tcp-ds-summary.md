# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 74/99 queries. Total PreQL length is 125,955 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,352 | -230 | +126 |
| PreQL vs Reference SQL | -60.8% | -21.1% | +13.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,976 | 1,459 | +517 |
| 30 | 2,007 | 1,507 | +500 |
| 29 | 1,545 | 1,089 | +456 |

Trilogy execution is faster than the reference SQL for 52/99 queries. Total Trilogy execution time is 15.075s vs 64.361s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.102s | -0.000s | +0.067s |
| Trilogy vs Reference SQL | -50.1% | -0.4% | +74.1% |
| Trilogy / Reference SQL | 0.50x | 1.00x | 1.74x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.635s | 0.098s | +0.537s |
| 76 | 0.315s | 0.062s | +0.252s |
| 16 | 0.193s | 0.018s | +0.174s |
| 28 | 0.216s | 0.048s | +0.168s |
| 78 | 0.495s | 0.346s | +0.149s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,934 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -48 | +287 |
| PreQL vs Reference SQL | -48.6% | -4.1% | +18.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 4.417s vs 0.471s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.016s | +0.008s | +1.988s |
| Trilogy vs Reference SQL | -17.7% | +18.1% | +1660.1% |
| Trilogy / Reference SQL | 0.82x | 1.18x | 17.60x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.116s | 0.117s | +1.999s |
| 97.2 | 2.095s | 0.124s | +1.971s |
| 30.alt | 0.055s | 0.047s | +0.008s |
