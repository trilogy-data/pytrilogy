# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 77/99 queries. Total PreQL length is 123,496 chars vs 182,494 reference SQL chars.

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

Trilogy execution is faster than the reference SQL for 44/99 queries. Total Trilogy execution time is 24.368s vs 84.828s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.261s | +0.004s | +0.135s |
| Trilogy vs Reference SQL | -63.1% | +3.9% | +219.1% |
| Trilogy / Reference SQL | 0.37x | 1.04x | 3.19x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 1.028s | 0.214s | +0.814s |
| 23 | 1.660s | 0.937s | +0.722s |
| 73 | 0.447s | 0.056s | +0.390s |
| 28 | 0.341s | 0.069s | +0.272s |
| 16 | 0.263s | 0.038s | +0.225s |

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

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 5.648s vs 0.737s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.048s | +0.056s | +2.479s |
| Trilogy vs Reference SQL | -26.8% | +59.5% | +1855.8% |
| Trilogy / Reference SQL | 0.73x | 1.60x | 19.56x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 2.622s | 0.129s | +2.493s |
| 97.1 | 2.599s | 0.141s | +2.458s |
| 30.alt | 0.150s | 0.094s | +0.056s |
