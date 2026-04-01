## State Store & Watermarks

### BaseStateStore

Central class for watermark collection and staleness detection.

- `watermark_all_assets`: queries DB for watermarks. **Skips any `ds_id` already in `self.watermarks`** — callers can pre-seed with `initial_watermarks` to avoid redundant queries.
- `get_stale_assets`: runs full staleness pipeline: freshness probes → schema mismatch → missing files → watermark comparison.

### create_refresh_plan

Accepts `initial_watermarks: dict[str, DatasourceWatermark] | None` — pre-inject root watermarks collected externally (e.g. from a deduplication phase) so the state store skips re-querying them.

`skip_datasources`: ds_ids to ignore entirely (owned by another script in a multi-script run).

### Concept max watermarks and derived concepts

`concept_max_watermarks` is built from root datasource watermarks and represents the "expected" value each non-root should be at. Derived concepts (those with `lineage`) that don't appear directly on any root are resolved via `get_concept_max_watermark_abstract`.

### execute_refresh_plan

Hides not-yet-refreshed stale assets from the query planner during each refresh step (temporarily pops them from `executor.environment.datasources`). This prevents the planner routing through stale/missing sources.
