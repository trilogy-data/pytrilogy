# q95 token sink (1,213,259 tok, FAIL) — explore JSON hides imported-dimension FK; wrong result is filter-scope idiom error

Run: `evals/tpcds_agent/results/20260711-042547_enriched`
Query: q95 — web-sales ship-date / IL / 'pri' / multi-warehouse order / has-return.
Result: `status: fail`, `detail: result set differs from reference`; 34 iterations, 14 `explore` calls, 4 tool errors, prompt_tokens 1.2M.

Ground truth (reference `query95.sql`, run on workspace db): **68 / 100592.32 / -18202.90**.
Agent's `query95.preql`: **6 / 111266.45 / -30167.86**.

## TL;DR — three distinct things, only one is a codegen bug

1. **Token-sink driver = TOOLING defect (framework) — PRESENTATION, not data absence:** the imported
   dim is NOT dropped. Verified 2026-07-11: `warehouse.sk` + all 7 of its properties ARE present in the
   JSON, under an `imported.warehouse` block (`grain: warehouse.sk`). The real defect is that JSON v2
   hoists each imported dim OUT of the fact's line-grain `namespaces` block into its own per-grain block,
   and the fact's own block references warehouse nowhere — so the dim reads as a standalone entity, not
   a dimension web_sales joins to. The **table** (`--show concepts`) lists `warehouse.sk` as a flat
   `root`-grain row *in the same concept list* as the fact's fields, so adjacency implies reachability;
   the JSON's regroup-by-grain breaks that adjacency. Neither format renders the FK edge
   (`WS_WAREHOUSE_SK → warehouse.sk`) explicitly. The agent spent ~13 turns / 14 explore calls
   concluding "warehouse isn't a field on the fact," then guessed `ws.warehouse.sk` (worked). That
   thrash is the token sink.
2. **The scored FAILURE = agent idiom error (NOT a framework bug):** the agent put the base filters
   (date / IL / 'pri') *inside* the `qualifying_orders` rowset, scoping `count_distinct(warehouse.sk)`
   and `sum(is_returned)` to only the filtered lines → 6 orders. The reference counts warehouses/returns
   over the **whole order**. Engine executes both faithfully; matches the documented "WHERE doesn't
   cross-filter into inline-aggregate scope" semantics.
3. **Bonus latent codegen bug (agent never hit it) — FIXED 2026-07-11:** a `?` filtered rowset that
   mixes an aggregate-comparison with a row-level predicate emitted invalid GROUP-BY SQL → DuckDB
   BinderException. Fixed in `filter_node.py`; see closed handoff
   `handoffs/handoff_mixed_filter_aggregate_row_predicate_groupby_leak.md`.

## Proof — trigger matrix (all against `<run>/workspace`, `import raw.web_sales as ws`)

| # | Query | Rows |
|---|-------|------|
| REF | `query95.sql` via `execute_raw_sql` | `68 / 100592.32 / -18202.90` |
| T1 | canonical idiom: **global** `?`-rowsets + `in` membership | `68 / 100592.32 / -18202.90` ✅ matches ref |
| T2 | agent's final query (filters **inside** rowset) | `6 / 111266.45 / -30167.86` ✗ |
| M2 | **agent's exact CTE structure**, base filters moved to the **final** select | `68 / 100592.32 / -18202.90` ✅ matches ref |

M2 isolates the entire discrepancy to **filter placement**: the agent's `with qualifying_orders as …`
approach is valid and correct once the date/IL/'pri' predicates live in the final `where`, not in the
rowset. So the wrong answer is a pure semantic/idiom mistake — no mis-codegen on the agent's path.

### Secondary latent codegen bug (characterization)

`ws.order_number ? <aggregate-comparison> and <row-predicate>` breaks:

| probe | filter body | outcome |
|-------|-------------|---------|
| A | `count(ws.warehouse.sk) by ws.order_number > 1` | OK |
| B | `count(ws.warehouse.sk) by ws.order_number > 1 and ws.ship_address.state='IL'` | **BinderException: column "ws_order_number" must appear in the GROUP BY** |
| C | `ws.ship_address.state='IL'` | OK |
| D | `count(...) by ws.order_number > 1 and sum(ws.is_returned::int) by ws.order_number > 0` | OK |

Only the **mix** of an aggregate filter with a row-grain predicate in one `?` filter generates SQL
that selects a non-grouped, non-aggregated `ws_order_number`. Per the "Binder error from GENERATED SQL
is always a framework bug" rule, this is a real codegen defect — but it is NOT what q95's agent hit
(the agent used the `with … as` CTE form throughout). Flag for a follow-up; repro above is minimal.

## Root cause (file:line)

- **Tooling defect (drives the token sink) — presentation, not omission:** `trilogy/scripts/explore.py`
  - `build_concepts_payload` (line ~703) splits `concept_items` into `local_items`
    (`c.namespace == DEFAULT_NAMESPACE`) → `namespaces`, and everything else → `_imported_payload`.
  - `_imported_payload` (line ~640–700) groups imported concepts under their dimension key
    (`imported.warehouse` at grain `warehouse.sk`). The data is all there — but grouping the dim into
    its own per-grain block, with the fact's own block referencing it nowhere, presents the dim as a
    standalone entity rather than one web_sales joins to. The table renderer keeps `warehouse.sk` in
    the flat concept list (adjacency = reachability); the JSON regroup loses that cue. The per-line FK
    is `WS_WAREHOUSE_SK: ?warehouse.sk` in `raw/web_sales.preql:97`.
  - Fix direction (do NOT implement here): make imported-dim blocks signal reachability FROM the fact —
    render the access path (`ws.warehouse.*`) or an explicit FK/`via` edge on the fact's namespace —
    rather than adding a "missing" key (nothing is missing). Goal: an agent reading JSON sees that
    `ws.<dim>.sk` is queryable, matching what the table view implies.

- **Scored wrong result:** agent idiom error (filter placement); reference idiom (T1) and the agent's
  own structure with correct placement (M2) both produce 68. Not a framework bug. Guidance-fixable via
  model/task wording ("multi-warehouse / has-return evaluated over the whole order, not just filtered lines").

- **Secondary codegen bug:** mixed aggregate+row predicate in a `?` filtered rowset → group-by leak.
  Not root-caused to a line; minimal repro (probe B) provided for a follow-up owner.

## Classification

- Token sink: **FRAMEWORK / TOOLING defect** — explore JSON presents imported dims as standalone
  entities (adjacency/reachability cue lost vs the table view); data is present, not omitted. Open.
- Wrong answer: **agent idiom error** (proven by M2), reinforced by intentional WHERE-scope semantics.
- Extra: **latent framework codegen bug** in mixed `?` filters — **FIXED 2026-07-11** (`filter_node.py`).
