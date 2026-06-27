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

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 13.591s vs 57.330s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.087s | +0.003s | +0.069s |
| Trilogy vs Reference SQL | -50.5% | +6.8% | +164.8% |
| Trilogy / Reference SQL | 0.50x | 1.07x | 2.65x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 74 | 0.704s | 0.193s | +0.512s |
| 14 | 0.812s | 0.308s | +0.505s |
| 05 | 0.441s | 0.066s | +0.375s |
| 09 | 0.331s | 0.036s | +0.295s |
| 73 | 0.245s | 0.036s | +0.209s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.378s vs 0.352s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.030s | +0.045s | +0.461s |
| Trilogy vs Reference SQL | +53.9% | +59.1% | +594.0% |
| Trilogy / Reference SQL | 1.54x | 1.59x | 6.94x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 0.545s | 0.077s | +0.468s |
| 97.2 | 0.528s | 0.078s | +0.449s |
| 2.1 | 0.123s | 0.078s | +0.045s |
| 2.2 | 0.127s | 0.084s | +0.043s |
| 30.alt | 0.055s | 0.035s | +0.021s |
