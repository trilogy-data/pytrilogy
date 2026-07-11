# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 76/99 queries. Total PreQL length is 123,642 chars vs 182,494 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,435 | -219 | +121 |
| PreQL vs Reference SQL | -61.2% | -21.5% | +10.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,694 | 2,056 | +638 |
| 89 | 1,566 | 965 | +601 |
| 81 | 1,941 | 1,459 | +482 |
| 29 | 1,526 | 1,089 | +437 |
| 35 | 2,175 | 1,745 | +430 |

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 11.828s vs 52.623s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.126s | +0.002s | +0.064s |
| Trilogy vs Reference SQL | -59.5% | +6.0% | +200.5% |
| Trilogy / Reference SQL | 0.41x | 1.06x | 3.00x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.571s | 0.062s | +0.509s |
| 23 | 0.531s | 0.280s | +0.251s |
| 73 | 0.239s | 0.029s | +0.210s |
| 28 | 0.217s | 0.043s | +0.174s |
| 76 | 0.203s | 0.040s | +0.163s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 4/5 queries. Total PreQL length is 6,571 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,247 | -57 | +77 |
| PreQL vs Reference SQL | -48.6% | -4.9% | +4.8% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 1,662 | 1,507 | +155 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.206s vs 0.282s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.026s | +0.022s | +0.976s |
| Trilogy vs Reference SQL | -33.5% | +73.8% | +1942.1% |
| Trilogy / Reference SQL | 0.66x | 1.74x | 20.42x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.028s | 0.050s | +0.978s |
| 97.2 | 1.024s | 0.050s | +0.974s |
| 30.alt | 0.052s | 0.030s | +0.022s |
