# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 75/99 queries. Total PreQL length is 127,879 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,262 | -223 | +252 |
| PreQL vs Reference SQL | -53.5% | -17.9% | +17.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,827 | 2,056 | +771 |
| 64 | 4,434 | 3,783 | +651 |
| 76 | 2,314 | 1,708 | +606 |
| 89 | 1,570 | 965 | +605 |
| 29 | 1,537 | 1,089 | +448 |

Trilogy execution is faster than the reference SQL for 43/99 queries. Total Trilogy execution time is 14.772s vs 60.086s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.123s | +0.002s | +0.068s |
| Trilogy vs Reference SQL | -44.1% | +3.6% | +122.0% |
| Trilogy / Reference SQL | 0.56x | 1.04x | 2.22x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 84 | 0.324s | 0.059s | +0.265s |
| 78 | 0.482s | 0.262s | +0.220s |
| 28 | 0.206s | 0.051s | +0.155s |
| 16 | 0.165s | 0.023s | +0.142s |
| 25 | 0.158s | 0.050s | +0.108s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 3/5 queries. Total PreQL length is 7,124 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,222 | -80 | +313 |
| PreQL vs Reference SQL | -47.6% | -6.9% | +24.7% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1,491 | 1,159 | +332 |
| 30.alt | 1,791 | 1,507 | +284 |

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 2.205s vs 0.435s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.000s | +0.002s | +1.059s |
| Trilogy vs Reference SQL | -0.0% | +2.2% | +1077.7% |
| Trilogy / Reference SQL | 1.00x | 1.02x | 11.78x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.858s | 0.099s | +1.759s |
| 30.alt | 0.057s | 0.049s | +0.008s |
| 2.2 | 0.099s | 0.097s | +0.002s |
| 97.2 | 0.099s | 0.099s | +0.001s |
