# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 81/99 queries. Total PreQL length is 139,447 chars vs 184,304 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -453 | -211 | +131 |
| PreQL vs Reference SQL | -20.2% | -18.9% | +9.0% |

Trilogy execution is faster than the reference SQL for 45/99 queries. Total Trilogy execution time is 19.080s vs 72.243s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.537s | +0.005s | +0.146s |
| Trilogy vs Reference SQL | +46.1% | +5.6% | +184.6% |
| Trilogy / Reference SQL | 1.46x | 1.06x | 2.85x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -661 | -656 | -4 |
| PreQL vs Reference SQL | -27.6% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 0/4 queries. Total Trilogy execution time is 2.567s vs 0.427s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.535s | +0.060s | +1.446s |
| Trilogy vs Reference SQL | +425.3% | +48.0% | +1148.1% |
| Trilogy / Reference SQL | 5.25x | 1.48x | 12.48x |
