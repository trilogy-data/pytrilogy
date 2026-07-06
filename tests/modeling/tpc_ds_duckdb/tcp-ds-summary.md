# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 124,376 chars vs 182,360 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,352 | -216 | +114 |
| PreQL vs Reference SQL | -61.1% | -22.7% | +13.6% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,976 | 1,459 | +517 |
| 30 | 2,007 | 1,507 | +500 |
| 29 | 1,543 | 1,089 | +454 |

Trilogy execution is faster than the reference SQL for 51/99 queries. Total Trilogy execution time is 12.085s vs 60.635s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.124s | -0.001s | +0.045s |
| Trilogy vs Reference SQL | -59.8% | -0.7% | +157.3% |
| Trilogy / Reference SQL | 0.40x | 0.99x | 2.57x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.652s | 0.069s | +0.583s |
| 73 | 0.350s | 0.030s | +0.320s |
| 75 | 0.378s | 0.098s | +0.280s |
| 78 | 0.440s | 0.206s | +0.234s |
| 28 | 0.242s | 0.044s | +0.198s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.448s vs 0.294s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.014s | +0.006s | +1.096s |
| Trilogy vs Reference SQL | -19.0% | +21.6% | +1936.5% |
| Trilogy / Reference SQL | 0.81x | 1.22x | 20.36x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.185s | 0.057s | +1.128s |
| 97.2 | 1.104s | 0.057s | +1.048s |
| 30.alt | 0.035s | 0.029s | +0.006s |
