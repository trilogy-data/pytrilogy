# Syntax / error / tooling follow-ups — handoff

> **Resolution pass (follow-up).** Status per item:
> - **P1 resolve-connections message — DONE.** Enriched in
>   `trilogy/core/processing/concept_strategies_v3.py` (`_disjoint_source_models`
>   helper + message). The error now names the disjoint source models and which
>   output concepts draw on each, and suggests `merge <a>.<key> into ~<b>.<key>;`.
>   Verified via CLI; unit test `tests/test_failure.py::test_disjoint_source_models_grouping`.
> - **P1 tool-arg JSON — NOT ACTIONED (out of repo).** Eval-harness concern, not
>   the `trilogy` package. Left for the harness owner.
> - **P2 A/B (GROUP BY, AS), P3 C (UDF), P3 E (count_distinct), F — NOT ACTIONED.**
>   Confirmed these are agent-prompt / docs items, not engine code; messages
>   already good. Belong in the eval agent system prompt / feature docs.
> - **P4 D (`by`-after-alias parse hint) — DEFERRED.** Adding a new parse-hint code
>   requires wiring both grammar backends (lark + pest/rust rebuild); not worth it
>   for a P4 given neighboring messages (103/201/211) are already strong.
> - **P4 H (CLI missing PATH) — NOT ACTIONED.** click auto-generates the message;
>   overriding is fiddly for marginal value.
> - **G, J — no action** (as originally triaged).


Source: run `20260601-025817` (q20–q29, deepseek, sf=1), from
`charts/trilogy_failures.md` (base, 24/205) and
`charts/trilogy_failures_enriched.md` (enriched, 10/132).

These are NOT framework crashes — those are filed separately as
`bug_aggregate_render_derived_in_by_grain.md` and
`bug_merge_in_subselect_missing_cte.md`. Everything here is a clean refusal or an
ergonomics gap. Each item below is scoped for an agent: **symptom → verdict →
where → action → acceptance**. All claims were verified against
`results/20260601-025817_base/workspace` on `trilogy 0.3.275`.

Ordered by priority.

---

## P1 — Tool-arg JSON rejected on large `--content` writes (eval-harness)

- **Symptom:** `Tool call 'trilogy' rejected: invalid tool arguments: Expecting ','
  delimiter: line 58 column 12 (char 2682). Re-issue the call with valid JSON
  arguments.` The agent tried to `trilogy file write queryNN.preql --content <large
  multi-line preql>`; the body broke the JSON encoding of the tool call. The whole
  turn is wasted and the agent often re-thrashes.
- **Verdict:** Harness/tool-schema issue, not Trilogy. High value: it silently
  burns turns on exactly the long, correct queries we most want to land.
- **Where:** the eval agent's `trilogy` tool definition / arg marshalling (eval
  tooling, not the `trilogy` package). Owner: eval-harness.
- **Action:** give large bodies a path that doesn't round-trip through a JSON
  string arg — e.g. a heredoc/stdin write mode, a base64 `--content-b64`, or a
  "write from a temp file" affordance. Alternatively make the marshaller robust to
  newlines/quotes in `--content`.
- **Acceptance:** a 2–3 KB multi-line preql body writes in one tool call without a
  JSON-delimiter rejection.

## P1 — `Could not resolve connections` is opaque (better message)

- **Symptom:** `Could not resolve connections for query with output
  ['local.channel<...>', 'local.last_name<...>', ...] from current model.` (q23
  draft: filtered `catalog_sales`/`web_sales` by `... in frequent_desc_prefix` /
  `... in best_cust_sk`, where those sets are derived from **store_sales**, an
  unconnected model — no `merge` linking them).
- **Verdict:** The refusal itself is plausibly correct (the models aren't
  connected), but the message names neither *what* failed to connect nor the fix.
  Priority is the message; the deeper "should `in` against a value-set from an
  unconnected model need a join path at all?" is a separate design question — note
  it, don't block on it.
- **Where:** `trilogy/core/processing/concept_strategies_v3.py:551-553`
  (`UnresolvableQueryException`, "Could not resolve connections..."). The
  `output_concepts` and their sources are in scope at the raise site.
- **Action:** enrich the message — list the disjoint source groups (which concepts
  came from which unconnected model namespace) and suggest `merge <a.key> into
  ~<b.key>` to bridge them. Keep it business-readable.
- **Acceptance:** the q23-draft query above produces an error naming the
  store_sales-vs-catalog/web split and suggesting a `merge`.

---

## P2 — Agent-prompt gaps (recurring; messages mostly already good)

These are model muscle-memory, not language holes. Best fixed with a short
"Trilogy ≠ SQL" reminder block in the **agent system prompt**, not grammar changes.

### A. SQL `GROUP BY` (recurring, ~4 hits)
- `Syntax [103]: ... Trilogy has no GROUP BY`. Message is already excellent.
- **Action:** add to agent prompt: "No `GROUP BY` — grouping is implicit by the
  non-aggregated select fields; aggregate at another grain with `agg(x) by k1,k2`."

### B. Missing `AS` on aggregates (recurring, ~3 hits)
- `Syntax [201]: Missing alias?` — message gives the exact fix. Same root cause as A.
- **Action:** fold into the same prompt reminder ("every select expression needs `AS`").

### C. UDFs — agent uses wrong call/placement (NOT a missing feature)
- **Correction to prior triage:** UDFs *exist*. Grammar:
  `def name(arg[: type][= default], ...) -> <expr>;` (top-level statement), called
  with an **`@` prefix**: `@name(...)`. Also `def table name(...) -> select ...`.
  Verified working:
  ```
  def double(x) -> x * 2;
  select ss.item.item_sk, @double(ss.sales_price) as dbl limit 3;   -- runs
  def avg_by_item(metric) -> avg(metric) by ss.item.item_id;
  select ss.item.item_id, @avg_by_item(ss.quantity) as ra limit 3;  -- runs (agg body OK)
  ```
- The agent's q27 failure (`def rollup_avg(metric) -> avg(metric) by rollup ...`,
  parse error / mis-alias) was two mistakes: (1) it embedded `def` **mid-query**
  (after a `select`/string had started — `def` is a top-level statement), and (2)
  it called the function without `@`. The `by rollup` body ties into the separate
  hard rollup cluster, not UDFs.
- **Verdict:** agent-prompt/docs. **Where:** grammar `trilogy/parsing/trilogy.lark`
  lines 194–196 (`raw_function`/`table_function`) and 626 (`custom_function`, the
  `@` call site).
- **Action:** document UDF define/`@`-call syntax + "defs are top-level" in agent
  guidance and any feature docs. No code change.

---

## P3 — Docs / idiom (no feature work recommended)

### E. `count_distinct(a, b)` — multi-arg distinct → use a derived key
- **Symptom:** q23 `count_distinct(store_sales.item.item_sk,
  store_sales.date_dim.date_sk)` → `Unexpected token COMMA ... Expected RPAR`.
- **Verdict:** Don't add the feature. Multi-arg `COUNT(DISTINCT a, b)` is
  essentially MySQL-only; Postgres / DuckDB / BigQuery / standard SQL don't support
  it. The portable idiom works today (verified):
  ```
  auto pair_key <- ss.item.item_sk::string || '|' || ss.date_dim.date_sk::string;
  select count(pair_key) as distinct_pairs;   -- count() de-dups
  ```
- **Action:** document "count distinct over a tuple → build a derived concat/struct
  key, then `count(key)`." Tie into the existing `count(key) is already distinct`
  guidance.

### F. Bare output-alias / business-id referenced as a concept → `Undefined concept`
- **Symptom:** `Undefined concept: last_name` (enriched q23 ×3), `Undefined concept:
  item_id` (base q23). Agent uses `item_id` expecting the business id (`i_item_id`
  → `item.text_id`) or treats a select alias as addressable.
- **Verdict:** overlaps the recurring **item code = `text_id`, not surrogate**
  model-description theme (AGENTS.md). Mostly a question/model nudge.
- **Action (optional):** make the `Undefined concept` error suggest near-matches
  (`did you mean item.text_id / item.id?`) when an unqualified token resembles a
  known concept suffix.

---

## P4 — Minor parse-hint / CLI ergonomics

### D. `by` after a select-item alias / on a bare constant
- q23 drafts: `... count_distinct(...) as y by ss.item.item_sk ?` and `auto pair <-
  1 by (...)`. `Unexpected token 'by'`. This is the inline-`by` placement confusion
  (AGENTS.md grain-inheritance note).
- **Action:** targeted parse-error hint when `by` follows a select alias or a
  non-aggregate constant → point to `agg(x) by k1,k2`. **Where:**
  `trilogy/parsing/v2/errors.py` (alongside the [103]/[201] hint handlers).

### G. Relative import path — already handled well
- q27: `Unable to import '.\physical_sales.preql' ... Did you mean:
  raw.physical_sales?` Message is good. **No action** beyond telling the agent
  imports are dotted module paths.

### H. `trilogy file write --content ...` with no PATH
- `Missing argument 'PATH'`. **Action:** make the error name the signature
  (`trilogy file write <PATH> --content <...>`). Minor CLI polish.

### J. HAVING references a measure not in SELECT — intended, leave as-is
- q24 (×3): the message already gives the `--col` fix and the WHERE alternative,
  and the agent complied. This is the designed HAVING-grain contract (AGENTS.md).
  **No action.**
