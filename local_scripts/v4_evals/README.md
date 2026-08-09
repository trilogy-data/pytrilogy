# Discovery correctness evals

Correctness cases for the discovery planner. Each `cases/*.preql` is a
self-contained program (inline datasources / consts + a final SELECT); the
harness generates + executes it on duckdb. A crash, a hang, or a render error
is a regression.

These began life as v3-vs-v4 parity repros. With the legacy planner removed
there is no oracle to diff against, so what a case guards now is that the
program plans, renders and runs — the failure mode most of them originally
caught. A case whose *rows* need pinning should assert them in a normal test
under `tests/`.

`run_parity.py`'s cases are guarded in CI by
`tests/core/processing/test_v4_parity_cases.py` (marker `v4_parity`), which
parametrizes over the same cases via this harness — so adding a case here adds
a CI test for free. Run just those: `pytest -m v4_parity`.

## Harness

- **`run_parity.py`** — generic. Home for ad-hoc correctness repros lifted from
  failing suite tests.

```
python local_scripts/v4_evals/run_parity.py            # all cases
python local_scripts/v4_evals/run_parity.py filter_past_unnest
```
