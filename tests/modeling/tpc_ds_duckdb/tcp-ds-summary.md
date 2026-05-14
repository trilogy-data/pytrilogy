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

Trilogy execution is faster than the reference SQL for 40/99 queries. Total Trilogy execution time is 15.692s vs 67.343s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.522s | +0.004s | +0.104s |
| Trilogy vs Reference SQL | +36.3% | +10.5% | +127.1% |
| Trilogy / Reference SQL | 1.36x | 1.11x | 2.27x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 4,805 chars vs 7,450 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -661 | -656 | -4 |
| PreQL vs Reference SQL | -27.6% | -31.6% | -0.4% |

Trilogy execution is faster than the reference SQL for 1/4 queries. Total Trilogy execution time is 1.935s vs 0.405s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.382s | +0.060s | +1.019s |
| Trilogy vs Reference SQL | +443.2% | +65.9% | +1182.9% |
| Trilogy / Reference SQL | 5.43x | 1.66x | 12.83x |
