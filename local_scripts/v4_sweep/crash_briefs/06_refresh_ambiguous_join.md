# P0 crash 06 — refresh of a multi-aggregate persist hits ambiguous join resolution

**Test:** `tests/scripts/test_refresh.py::test_refresh_multiple_aggregate_persists_with_shared_count`
**Exception:** `AmbiguousRelationshipResolutionException` wrapped in
`RefreshAssetError` (`trilogy/execution/state/state_store.py:587`).

```
Multiple possible concept additions (intermediate join keys) found to resolve
['local.data_through', '__preql_internal.all_rows', 'local.id', 'local.origin_code'],
have {'local.code'} or {'flight_date.year'}. Different paths are: [{'local.code'}, {'flight_date.year'}]
```

## Repro
```
TRILOGY_V4_DISCOVERY=1 .venv/Scripts/python.exe -m pytest \
  "tests/scripts/test_refresh.py::test_refresh_multiple_aggregate_persists_with_shared_count" \
  --runxfail --tb=short
```
v3 passes (drop the env var).

## Symptom
Refreshing a persisted datasource (`flight_count_by_origin`) that holds multiple
aggregates sharing a `count`. v4's root/merge search finds two candidate
intermediate join keys (`local.code` vs `flight_date.year`) to connect
`all_rows` + `id` + `origin_code` + `data_through` and refuses to pick — v3
resolves this deterministically.

## Hypothesis
This is the v4 root-merge join-key selection raising on a tie that v3 breaks. See
memory `[[project_join_resolution_pseudonyms]]` and the q04/q10 root-split notes;
likely in `v4_helper/source_planning.py` / `strategy_builder.py` root-merge
key reinjection (the `[COMMON] reinjecting common join key` path) or in how the
persist/refresh grain (`__preql_internal.all_rows` + the aggregate keys) demands
an intermediate that has two equal-cost paths. The fix is a deterministic
tie-break consistent with v3 (prefer the key already on the aggregate's grain over
an unrelated dimension like `flight_date.year`).

## Done when
the refresh test passes under both engines; entry removed from
`tests/v4_known_failing.py`; rerun `tests/scripts/test_refresh.py` under v4.
Note this is the same exception class seen during ad-hoc multi-source merges —
fixing the tie-break may also clear some `_MODELING`-bucket failures (gcat /
join_resolution), so re-sweep those after.
