# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 125,872 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -209 | +171 |
| PreQL vs Reference SQL | -60.0% | -21.0% | +13.0% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 29 | 1,545 | 1,089 | +456 |

Trilogy execution is faster than the reference SQL for 38/99 queries. Total Trilogy execution time is 15.147s vs 69.054s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.104s | +0.009s | +0.071s |
| Trilogy vs Reference SQL | -42.0% | +10.3% | +182.7% |
| Trilogy / Reference SQL | 0.58x | 1.10x | 2.83x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.736s | 0.061s | +0.675s |
| 23 | 0.653s | 0.329s | +0.324s |
| 73 | 0.343s | 0.035s | +0.308s |
| 83 | 0.270s | 0.040s | +0.229s |
| 28 | 0.261s | 0.044s | +0.217s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,643 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,233 | -39 | +89 |
| PreQL vs Reference SQL | -48.0% | -3.4% | +5.7% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,670 | 1,507 | +163 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 3.158s vs 0.378s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.038s | +0.022s | +1.422s |
| Trilogy vs Reference SQL | -36.4% | +59.6% | +2126.3% |
| Trilogy / Reference SQL | 0.64x | 1.60x | 22.26x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.514s | 0.068s | +1.447s |
| 97.1 | 1.452s | 0.066s | +1.386s |
| 30.alt | 0.058s | 0.036s | +0.022s |
