# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 80/99 queries. Total PreQL length is 139,951 chars vs 184,304 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,257 | -211 | +149 |
| PreQL vs Reference SQL | -57.6% | -18.9% | +11.6% |

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 12.317s vs 56.620s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.111s | +0.002s | +0.083s |
| Trilogy vs Reference SQL | -64.4% | +6.0% | +156.4% |
| Trilogy / Reference SQL | 0.36x | 1.06x | 2.56x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -1,323 | -656 | -4 |
| PreQL vs Reference SQL | -51.5% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 0/4 queries. Total Trilogy execution time is 1.540s vs 0.294s reference SQL time.

| Performance metric | P10 | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.002s | +0.035s | +0.842s |
| Trilogy vs Reference SQL | +2.8% | +46.4% | +1214.2% |
| Trilogy / Reference SQL | 1.03x | 1.46x | 13.14x |
