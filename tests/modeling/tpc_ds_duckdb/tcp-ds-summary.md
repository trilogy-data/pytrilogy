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

Trilogy execution is faster than the reference SQL for 41/99 queries. Total Trilogy execution time is 25.354s vs 77.805s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.188s | +0.009s | +0.186s |
| Trilogy vs Reference SQL | -48.8% | +9.2% | +238.9% |
| Trilogy / Reference SQL | 0.51x | 1.09x | 3.39x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 35 | 1.473s | 0.337s | +1.136s |
| 23 | 1.613s | 0.787s | +0.827s |
| 05 | 0.874s | 0.217s | +0.657s |
| 70 | 0.536s | 0.128s | +0.408s |
| 73 | 0.409s | 0.052s | +0.356s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 5.478s vs 0.573s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.027s | +0.041s | +2.465s |
| Trilogy vs Reference SQL | -23.0% | +56.1% | +1882.0% |
| Trilogy / Reference SQL | 0.77x | 1.56x | 19.82x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 2.620s | 0.131s | +2.490s |
| 97.1 | 2.559s | 0.131s | +2.428s |
| 30.alt | 0.114s | 0.073s | +0.041s |
