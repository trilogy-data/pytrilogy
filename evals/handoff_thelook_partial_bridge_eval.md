# Handoff: thelook agent eval for the partial-bridge error

Build a new eval suite, `evals/thelook_agent`, that measures whether
`UnconstrainedPartialBridgeException` is HELPFUL: when an agent's natural first
query hits it, does the agent recover from the message alone (apply the
suggested pin or an equivalent fact filter) and land on correct rows? Runs
entirely on DuckDB.

## Why this model and not TPC-DS/TPC-H

The error fires only for a span whose EVERY bridge is a datasource with **two or
more required live `~` keys and no complete row anchor in the output**
(`trilogy/core/processing/partial_bridging.py`; semantics in
`docs/partial_bridge_pinning.md`). In TPC-DS every multi-`~` fact has a complete
same-grain sibling (store_sales anchors store_returns), and TPC-H's `~`
bindings (`lineitem.~part.id`, `order.~customer.id`) are one-per-fact, so both
batteries always find a safe bridge and the error never fires. The thelook
e-commerce shape — `order_items` binding BOTH `~product` and `~user` with no
complete sibling — is the shape that triggers it, and is the real-world model
(`sales_reporting`) this feature was built for.

## What already exists — read these first

- Harness: `evals/common/` (`spec.py::BenchmarkSpec`, `agent_runner.py`,
  `scoring.py` — order-independent row compare vs reference SQL,
  `categories.py`). Loop discipline: `evals/EVAL_LOOP_INSTRUCTIONS.md`.
- **Template for a custom-built DB**: `evals/dabstep_agent/` — no duckdb
  extension; `db_build.py` exposes `DB_FILENAME` + `build_database`, wired via
  `BenchmarkSpec(database_builder=...)`, with `references/` holding one
  canonical SQL per question. Copy this structure.
- The exact binding shapes and their row-level contracts, already pinned as
  tests: `tests/engine/test_duckdb_partial_key_assembly.py` (`_SIMPLE` /
  `_FORKED` are miniature thelook models — the eval model is these, scaled up)
  and `tests/modeling/tpc_ds_duckdb/test_partial_key_assembly_shapes.py`
  (`_PAIR_FACT_MODEL`).
- The error's agent-facing surface: message text + `.suggestion`
  (`where <k1> is not null and <k2> is not null`) built at the bottom of
  `partial_bridging.py::validate_partial_bridges`; exception class in
  `trilogy/core/exceptions.py`.

## Deliverables

`evals/thelook_agent/` containing:

1. **`db_build.py`** — deterministic synthetic thelook-ecommerce generator
   (seeded RNG, no network): `users` (id, state, age, traffic_source),
   `products` (id, brand, category, department, retail_price, cost),
   `orders` (order_id, user_id, status, created_at),
   `order_items` (id, order_id, user_id, product_id, sale_price, status).
   Sizing guidance: ~2k users, ~500 products, ~5k orders, ~15k order items.
   **Load-bearing data properties** (assert them after build):
   - some users never order (extension family #1 exists),
   - some products never sell (extension family #2 exists),
   - `order_items.user_id` matches its order's user (redundant FK, thelook
     style),
   - no NULL FKs inside fact rows (so the pin is a pure population statement).
2. **Curated model** (`enriched_model/`), the part that makes the error
   reachable — bindings must be exactly:
   - `users` / `products` complete at their own grain;
   - `orders` binds `user_id: ~user_id`;
   - `order_items` binds `order_id` complete, `product_id: ~product_id`,
     `user_id: ~user_id`, grain (id).
   Include a few derived concepts mirroring `_FORKED` (revenue = sum(sale_price),
   margin = sale_price - cost aggregates) so questions have metrics. Do NOT add
   any complete datasource that relates user×product (a summary table would
   anchor the span and the error stops firing).
3. **`query_prompts.json`** (~10-12 questions) + **`references/`** (one
   canonical DuckDB SQL each). Design the mix deliberately:
   - **Error-triggering (the point, ~5)**: spans with no fact anchor —
     "revenue by customer state and product brand", "which product categories
     do customers from each traffic source buy", "top (state, department)
     pairs by margin". Reference SQL = INNER-join semantics over order_items
     (the pinned population).
   - **Controls that must NOT error (~4)**: single-family extension ("all
     users and their lifetime revenue, including users who never bought" —
     expects the NULL-extension row), fact-anchored wide selects (include
     `order_items.id` or `order_id`), and a pre-pinned prompt where the user
     text itself says "only where a sale exists".
   - **Judgment (~2)**: a prompt whose correct answer is per-side ("list all
     states and separately all brands") — the error's "query each side
     separately" guidance is the right move, and pinning is wrong.
4. **`spec.py`** — `BenchmarkSpec` like dabstep's (`duckdb_extension=""`,
   `database_builder=db_build.build_database`,
   `default_enriched_dir=EVAL_DIR / "enriched_model"`), plus `run_eval.py`
   copied from `evals/dabstep_agent/run_eval.py`.
5. **Error-recovery metric**: beyond pass/fail scoring, report per question
   whether the transcript contains `UnconstrainedPartialBridgeException` and
   whether the run still converged to a correct answer — that ratio IS the
   "is the error helpful" number. Precedent for transcript scanning:
   `evals/tpcds_agent/error_scan.py`; trajectories via
   `evals/trajectory_viewer.py`. Compare `enriched` (curated model, sees the
   error) against `sql_bare`/`sql_schema` baselines as usual.

## Facts the eval-builder must not rediscover the hard way

- The pin heals: with `where user_id is not null and product_id is not null`
  the `~` marks are dropped per-statement and the plan is a plain INNER star —
  so the reference SQL for pinned questions is ordinary join SQL, no special
  semantics.
- A pin on a key NEVER heals that key itself; keys heal each other. The
  `.suggestion` always pins every involved key and is always sufficient.
- An equality/range filter on the OTHER side also heals (`where brand = 'X'`
  proves brand non-null → kills user-side extensions) — agents that "fix" the
  error by filtering instead of pinning may be correct; score by rows, not by
  whether they used the literal suggestion.
- `then where` (staged) statements skip heal+validation entirely — an agent
  that rewrites into a staged chain will get pre-error behavior; treat as a
  finding, not a scoring bug.
- Aggregates collapse their internals: `sum(x) by user` never creates a span
  obligation. Only row-level outputs and WHERE row-args count.
- Don't run eval iterations concurrently with pytest suites in this repo
  (phantom failures — see project memory/AGENTS.md).

## Success criteria

- Suite runs green end-to-end at `--category enriched` with a live key.
- Every "error-triggering" question demonstrably raises the exception on the
  naive un-pinned query (add a smoke test that asserts this against the
  enriched model directly, no LLM — cheap regression guard for the model
  bindings).
- Report includes the error-recovery ratio and, for failures, whether the
  agent misread the message (that's the actionable output: message wording
  lives in one place in `validate_partial_bridges` and can be tuned).
