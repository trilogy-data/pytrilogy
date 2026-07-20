# TPC-H Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 22

PreQL is shorter than the reference SQL for 22/22 queries. Total PreQL length is 8,616 chars vs 13,732 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -195 | -80 |
| PreQL vs Reference SQL | -55.6% | -34.6% | -17.0% |

Top 5 queries where PreQL is longest vs reference SQL

None.

Trilogy execution is faster than the reference SQL for 6/22 queries. Total Trilogy execution time is 2.260s vs 1.422s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.017s | +0.010s | +0.116s |
| Trilogy vs Reference SQL | -26.6% | +20.6% | +158.9% |
| Trilogy / Reference SQL | 0.73x | 1.21x | 2.59x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 21 | 0.618s | 0.261s | +0.357s |
| 20 | 0.286s | 0.110s | +0.177s |
| 17 | 0.158s | 0.039s | +0.120s |
| 11 | 0.125s | 0.039s | +0.086s |
| 16 | 0.229s | 0.198s | +0.031s |
