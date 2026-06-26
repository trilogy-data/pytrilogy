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

Trilogy execution is faster than the reference SQL for 38/99 queries. Total Trilogy execution time is 18.320s vs 65.213s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.138s | +0.010s | +0.164s |
| Trilogy vs Reference SQL | -51.7% | +11.9% | +311.1% |
| Trilogy / Reference SQL | 0.48x | 1.12x | 4.11x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 74 | 0.991s | 0.235s | +0.756s |
| 14 | 1.068s | 0.461s | +0.607s |
| 73 | 0.627s | 0.044s | +0.582s |
| 05 | 0.597s | 0.084s | +0.513s |
| 76 | 0.426s | 0.040s | +0.387s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 3.473s vs 0.451s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.003s | +0.038s | +1.493s |
| Trilogy vs Reference SQL | -4.1% | +98.5% | +1456.5% |
| Trilogy / Reference SQL | 0.96x | 1.98x | 15.56x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.613s | 0.103s | +1.511s |
| 97.2 | 1.568s | 0.102s | +1.465s |
| 2.1 | 0.147s | 0.109s | +0.038s |
| 30.alt | 0.076s | 0.038s | +0.038s |
