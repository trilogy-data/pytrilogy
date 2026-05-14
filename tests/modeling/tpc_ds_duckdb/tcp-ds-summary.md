# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 139,339 chars vs 184,304 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -454 | -261 | +131 |
| PreQL vs Reference SQL | -20.4% | -19.2% | +9.3% |

Trilogy execution is faster than the reference SQL for 42/99 queries. Total Trilogy execution time is 12.631s vs 59.011s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.468s | +0.003s | +0.083s |
| Trilogy vs Reference SQL | +40.1% | +9.1% | +153.7% |
| Trilogy / Reference SQL | 1.40x | 1.09x | 2.54x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -661 | -656 | -4 |
| PreQL vs Reference SQL | -27.6% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 2/4 queries. Total Trilogy execution time is 1.549s vs 0.312s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.309s | +0.035s | +0.842s |
| Trilogy vs Reference SQL | +351.4% | +49.2% | +950.4% |
| Trilogy / Reference SQL | 4.51x | 1.49x | 10.50x |
