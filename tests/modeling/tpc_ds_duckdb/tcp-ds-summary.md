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

Trilogy execution is faster than the reference SQL for 34/99 queries. Total Trilogy execution time is 21.330s vs 60.137s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.126s | +0.008s | +0.203s |
| Trilogy vs Reference SQL | -54.6% | +10.4% | +334.0% |
| Trilogy / Reference SQL | 0.45x | 1.10x | 4.34x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.933s | 0.153s | +0.780s |
| 73 | 0.697s | 0.050s | +0.648s |
| 74 | 0.945s | 0.310s | +0.635s |
| 14 | 1.058s | 0.449s | +0.608s |
| 44 | 0.641s | 0.035s | +0.606s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 3.695s vs 0.588s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.010s | +0.094s | +1.493s |
| Trilogy vs Reference SQL | +5.1% | +195.9% | +1456.5% |
| Trilogy / Reference SQL | 1.05x | 2.96x | 15.56x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 1.613s | 0.103s | +1.511s |
| 97.2 | 1.568s | 0.102s | +1.465s |
| 30.alt | 0.141s | 0.048s | +0.094s |
| 2.1 | 0.235s | 0.173s | +0.062s |
