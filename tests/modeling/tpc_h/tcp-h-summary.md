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

Trilogy execution is faster than the reference SQL for 2/22 queries. Total Trilogy execution time is 0.841s vs 0.618s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.000s | +0.005s | +0.028s |
| Trilogy vs Reference SQL | +0.9% | +23.6% | +80.6% |
| Trilogy / Reference SQL | 1.01x | 1.24x | 1.81x |

Top 5 queries where reference SQL is fastest vs Trilogy

| Query | Trilogy s | Reference SQL s | Trilogy - Reference SQL |
| --- | ---: | ---: | ---: |
| 20 | 0.088s | 0.023s | +0.065s |
| 17 | 0.067s | 0.011s | +0.056s |
| 16 | 0.076s | 0.047s | +0.029s |
| 05 | 0.052s | 0.035s | +0.017s |
| 21 | 0.079s | 0.062s | +0.016s |
