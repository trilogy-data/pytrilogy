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

Trilogy execution is faster than the reference SQL for 47/99 queries. Total Trilogy execution time is 13.998s vs 62.431s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.123s | +0.000s | +0.061s |
| Trilogy vs Reference SQL | -53.3% | +0.6% | +82.1% |
| Trilogy / Reference SQL | 0.47x | 1.01x | 1.82x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 05 | 0.601s | 0.100s | +0.501s |
| 76 | 0.239s | 0.050s | +0.188s |
| 16 | 0.206s | 0.018s | +0.187s |
| 28 | 0.222s | 0.051s | +0.172s |
| 78 | 0.442s | 0.274s | +0.168s |

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

Trilogy execution is faster than the reference SQL for 1/5 queries. Total Trilogy execution time is 4.169s vs 0.431s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.005s | +0.181s | +1.791s |
| Trilogy vs Reference SQL | +0.3% | +192.7% | +1758.6% |
| Trilogy / Reference SQL | 1.00x | 2.93x | 18.59x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.2 | 1.931s | 0.103s | +1.829s |
| 97.1 | 1.834s | 0.101s | +1.734s |
| 2.1 | 0.274s | 0.094s | +0.181s |
| 30.alt | 0.057s | 0.044s | +0.013s |
