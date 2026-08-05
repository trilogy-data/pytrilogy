# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 126,332 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,420 | -209 | +206 |
| PreQL vs Reference SQL | -60.0% | -21.8% | +14.1% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 81 | 2,125 | 1,459 | +666 |
| 28 | 2,694 | 2,056 | +638 |
| 35 | 2,381 | 1,745 | +636 |
| 89 | 1,586 | 965 | +621 |
| 64 | 4,382 | 3,783 | +599 |

Trilogy execution is faster than the reference SQL for 52/99 queries. Total Trilogy execution time is 13.899s vs 13.845s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.067s | -0.001s | +0.074s |
| Trilogy vs Reference SQL | -43.5% | -1.8% | +81.0% |
| Trilogy / Reference SQL | 0.57x | 0.98x | 1.81x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.705s | 0.158s | +0.548s |
| 78 | 0.703s | 0.412s | +0.291s |
| 70 | 0.354s | 0.069s | +0.284s |
| 23 | 0.743s | 0.463s | +0.280s |
| 83 | 0.328s | 0.093s | +0.234s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 1.818s vs 0.388s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.029s | +0.043s | +0.669s |
| Trilogy vs Reference SQL | +39.6% | +74.7% | +739.6% |
| Trilogy / Reference SQL | 1.40x | 1.75x | 8.40x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 0.773s | 0.086s | +0.687s |
| 97.2 | 0.740s | 0.099s | +0.642s |
| 30.alt | 0.101s | 0.058s | +0.043s |
| 2.1 | 0.105s | 0.075s | +0.029s |
| 2.2 | 0.099s | 0.070s | +0.029s |
