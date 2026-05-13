# TPC-DS Result Summary

Timing fingerprint: `amd64-intel64_family_6_model_183_stepping_1_genuineintel-16`

Signed deltas are `PreQL - Reference SQL` for size and `Trilogy execution - Reference SQL execution` for performance. Negative values mean PreQL is shorter or Trilogy is faster.

## Suggested Queries

Queries: 99

PreQL is shorter than the reference SQL for 72/99 queries. Total PreQL length is 150,959 chars vs 184,707 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -341 | -166 | +302 |
| PreQL vs Reference SQL | -13.0% | -14.6% | +28.1% |

Trilogy execution is faster than the reference SQL for 39/99 queries. Total Trilogy execution time is 21.162s vs 82.326s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | -0.618s | +0.007s | +0.166s |
| Trilogy vs Reference SQL | +103.3% | +11.0% | +309.1% |
| Trilogy / Reference SQL | 2.03x | 1.11x | 4.09x |

## Alternative Queries

Queries: 4

PreQL is shorter than the reference SQL for 3/4 queries. Total PreQL length is 5,372 chars vs 7,452 reference SQL chars.

| Length metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| PreQL - Reference SQL chars | -520 | -515 | +176 |
| PreQL vs Reference SQL | -17.8% | -21.9% | +15.2% |

Trilogy execution is faster than the reference SQL for 1/4 queries. Total Trilogy execution time is 2.812s vs 0.447s reference SQL time.

| Performance metric | Avg | P50 | P90 |
| --- | ---: | ---: | ---: |
| Trilogy - Reference SQL seconds | +0.591s | +0.083s | +1.593s |
| Trilogy vs Reference SQL | +465.0% | +77.8% | +1244.1% |
| Trilogy / Reference SQL | 5.65x | 1.78x | 13.44x |
