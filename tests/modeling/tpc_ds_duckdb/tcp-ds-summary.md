# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 73/99 queries. Total PreQL length is 150,407 chars vs 184,707 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -346 | -168 | +302 |
| PreQL vs Reference SQL | -13.2% | -14.6% | +28.1% |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 22.950s vs 133.482s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -1.116s | +0.007s | +0.196s |
| Trilogy vs Reference SQL | +109.5% | +11.0% | +343.6% |
| Trilogy / Reference SQL | 2.10x | 1.11x | 4.44x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 5,372 chars vs 7,452 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -520 | -515 | +176 |
| PreQL vs Reference SQL | -17.8% | -21.9% | +15.2% |

Trilogy execution is faster than the reference SQL for 2/4 queries. Total Trilogy execution time is 2.774s vs 0.441s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.583s | +0.066s | +1.593s |
| Trilogy vs Reference SQL | +457.9% | +62.1% | +1244.1% |
| Trilogy / Reference SQL | 5.58x | 1.62x | 13.44x |
