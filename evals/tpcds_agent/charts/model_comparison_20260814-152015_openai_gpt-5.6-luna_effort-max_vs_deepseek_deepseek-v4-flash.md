# GPT-5.6 Luna max vs DeepSeek V4 Flash — TPC-DS q01–q20

All comparisons use the same first 20 TPC-DS questions. The Luna runs used
`openai/gpt-5.6-luna` with `reasoning.effort=max` and concurrency 2. SQL
DeepSeek values are the q01–q20 slice of the completed
`20260813-125008_sql_*` runs. The enriched DeepSeek values were reconstructed
from its logs because that run did not finalize a report.

| Category | DeepSeek pass | Luna pass | DeepSeek tokens | Luna tokens | Luna token delta |
|---|---:|---:|---:|---:|---:|
| enriched | 17/20 | 17/20 | 4,997,177 | 1,941,657 | -61.1% |
| sql_schema | 17/20 | 18/20 | 2,008,739 | 2,805,145 | +39.6% |
| sql_bare | 18/20 | 18/20 | 3,267,663 | 6,274,353 | +92.0% |
| **Combined** | **52/60** | **53/60** | **10,273,579** | **11,021,155** | **+7.3%** |

| Category | DeepSeek cache-adjusted | Luna cache-adjusted | Delta | DeepSeek agent time | Luna agent time | Delta |
|---|---:|---:|---:|---:|---:|---:|
| enriched | 1,182,099 | 557,858 | -52.8% | n/a | 877.7s | n/a |
| sql_schema | 527,843 | 679,801 | +28.8% | 1,509.1s | 1,954.2s | +29.5% |
| sql_bare | 724,162 | 1,359,836 | +87.8% | 1,995.7s | 3,218.8s | +61.3% |
| **Combined** | **2,434,104** | **2,597,495** | **+6.7%** | — | — | — |

The SQL agent-time comparison sums per-query durations so it is independent of
the different enclosing run schedules. Luna wall time at concurrency 2 was
1,006.8s for `sql_schema` and 1,657.7s for `sql_bare`.

Failure sets:

- enriched — DeepSeek: q04, q05 (timeout/missing), q20; Luna: q04, q11, q14
- sql_schema — DeepSeek: q02, q05, q20; Luna: q05, q14
- sql_bare — DeepSeek: q05, q20; Luna: q05, q14

The token advantage is therefore category-dependent. Luna max is substantially
more efficient when the enriched semantic model constrains discovery, but it
does more iterations and tool calls than DeepSeek on the two raw-SQL legs. A
Luna high-effort sample is the useful next experiment; lowering effort may keep
the enriched efficiency while cutting the long raw-SQL reasoning tails.
