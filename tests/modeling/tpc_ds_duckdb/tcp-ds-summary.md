# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 71/99 queries. Total PreQL length is 130,391 chars vs 182,502 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,333 | -200 | +186 |
| PreQL vs Reference SQL | -59.5% | -18.4% | +19.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 28 | 2,793 | 2,056 | +737 |
| 89 | 1,659 | 965 | +694 |
| 76 | 2,347 | 1,708 | +639 |
| 64 | 4,403 | 3,783 | +620 |
| 29 | 1,626 | 1,089 | +537 |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 30.541s vs 107.520s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.135s | +0.004s | +0.178s |
| Trilogy vs Reference SQL | -48.1% | +5.7% | +202.4% |
| Trilogy / Reference SQL | 0.52x | 1.06x | 3.02x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 69 | 2.916s | 0.350s | +2.565s |
| 66 | 2.081s | 0.341s | +1.740s |
| 65 | 1.846s | 0.298s | +1.548s |
| 70 | 1.219s | 0.227s | +0.991s |
| 71 | 1.014s | 0.153s | +0.861s |

## Alternative Queries

Queries: 5

PreQL is shorter than the reference SQL for 3/5 queries. Total PreQL length is 7,484 chars vs 8,957 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,249 | -9 | +493 |
| PreQL vs Reference SQL | -48.7% | -0.8% | +38.3% |

Top 5 queries where PreQL is longest vs reference SQL

| Query | PreQL chars | Reference SQL chars | PreQL - Reference SQL |
| --- | ---: | ---: | ---: |
| 30.alt | 2,007 | 1,507 | +500 |
| 97.2 | 1,642 | 1,159 | +483 |

Trilogy execution is faster than the reference SQL for 2/5 queries. Total Trilogy execution time is 2.561s vs 0.600s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.048s | +0.000s | +1.232s |
| Trilogy vs Reference SQL | -35.3% | +0.1% | +965.5% |
| Trilogy / Reference SQL | 0.65x | 1.00x | 10.66x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 2.175s | 0.128s | +2.047s |
| 30.alt | 0.068s | 0.057s | +0.011s |
| 97.2 | 0.141s | 0.140s | +0.000s |
