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

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 5.595s vs 5.372s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.019s | +0.001s | +0.023s |
| Trilogy vs Reference SQL | -40.9% | +4.6% | +98.9% |
| Trilogy / Reference SQL | 0.59x | 1.05x | 1.99x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 23 | 0.467s | 0.184s | +0.283s |
| 05 | 0.319s | 0.048s | +0.271s |
| 78 | 0.281s | 0.143s | +0.138s |
| 83 | 0.103s | 0.024s | +0.079s |
| 70 | 0.094s | 0.021s | +0.073s |

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

Trilogy execution is faster than the reference SQL for 0/5 queries. Total Trilogy execution time is 0.845s vs 0.152s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.004s | +0.015s | +0.337s |
| Trilogy vs Reference SQL | +15.3% | +56.6% | +849.6% |
| Trilogy / Reference SQL | 1.15x | 1.57x | 9.50x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 97.1 | 0.381s | 0.040s | +0.341s |
| 97.2 | 0.369s | 0.039s | +0.330s |
| 30.alt | 0.042s | 0.027s | +0.015s |
| 2.1 | 0.025s | 0.021s | +0.004s |
| 2.2 | 0.028s | 0.025s | +0.003s |
