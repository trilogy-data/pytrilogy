---
name: project_q54_reference_store_fanout_fixed
description: FIXED 2026-07-01 — TPC-DS q54 reference had the official store fan-out (revenue x #stores in county/state); de-fanned .sql + .preql + test so we don't goal agents on the buggy metric
metadata:
  type: project
---

FIXED 2026-07-01. q54 was a BAD REFERENCE, not an agent/framework bug. The official DuckDB `PRAGMA tpcds(54)` (and the repo's `query54.sql` + canonical `query54.preql` which deliberately mirrored it) has the well-known store fan-out: `my_revenue` cross-joins `store` on `ca_county=s_county AND ca_state=s_state` WITHOUT `ss_store_sk=s_store_sk`, so each store sale is counted once PER store in the customer's county/state (Williamson County TN has 12 stores → ~12x). Reference said segment 10715 / total 535750; the business-correct value is segment 880 / total 43985 (one customer, 26788).

The business PROMPT was already correct ("store sales AT STORES whose county and state match the customer's current home address") — implies the sale's own store, i.e. `ss_store_sk=s_store_sk`. The agent computed the correct 880 and was wrongly failed against the fanned reference. Discovered while sweeping the q54 654k-token failure.

FIX (3 files, no prompt change):
1. `tests/modeling/tpc_ds_duckdb/query54.sql` — added `AND ss_store_sk = s_store_sk` to my_revenue → 880.
2. `tests/modeling/tpc_ds_duckdb/query54.preql` — rewrote: dropped the deliberate fan-out (`ss_revenue * stores_cs.scs_count` + stores_cs count rowset), now a single `my_revenue` filtering `ss.store.county = ss.customer.address.county and ss.store.state = ss.customer.address.state` (sale's store in the customer's current-addr county/state). `ss.store` and `ss.customer.address` (current home addr) both exist in the enriched store_sales model.
3. `test_queries.py::test_fifty_four` — changed to `run_query(engine, 54, sql_override=True)` so it validates against the corrected .sql, NOT `PRAGMA tpcds(54)` (which still has the fan-out). Added comment. Eval scorer already prefers custom_refs_dir/.sql (scoring.py:501) so the corrected .sql flows to the eval.

Verified: test_fifty_four + test_query_size green; agent's saved q54 candidate now scores `pass`.

FLAG (open): other TPC-DS references transcribed the same way may share a silent fan-out (a dimension joined for a FILTER but not linked to its fact by surrogate key). Worth a scan: for each queryNN.sql, check every filter-joined dimension also has its fact-key equality. See [[project_q17_empty_ref_model_customer_join_gap]] for a prior reference-side defect.
