# Pre-merge framework fixes — prioritized index

Source: top-10-by-token-sink probes on the enriched TPC-DS leg (runs `20260628-042638`,
`20260628-194910`, `20260629-001912`). Operating principle that drove this list, validated
repeatedly: **any query that burns >500k tokens has a framework bug behind it** — the token
bar is often the *only* detector, because several of these are SILENT (wrong result or
timeout, no error). Every open item below has a minimal `generate_sql` repro in its handoff.

Legend: **SILENT** = no error/sentinel, ships a wrong answer or hangs (worst — only the token
bar catches it). **LOUD** = raises an exception the agent sees.

---

## A. OPEN — fix before merge (priority order: silent-wrong-result first)

| # | Query | Bug (one line) | Signal | Fix locus | Handoff |
|---|---|---|---|---|---|
| ~~1~~ | ~~q97~~ | **NOT a codegen bug** (claude, 2026-06-28) — scoped `full join a=b` is an *identity assertion*: `a.k`/`b.k` become one merged key, so per-side `is null` flags are tautologically empty (trilogy is self-consistent). The agent transliterated the reference SQL's distinct-column FULL OUTER JOIN, which doesn't translate. → **guidance** (see §D) | n/a | n/a | [bug_q97_churn_001912.md](bug_q97_churn_001912.md) |
| 2 | q78 | (a) chained `=` join keys **zip-pair adjacently** (`item=item=cust=cust` → garbage `item⇒cust`); (b) constant join key (`2000 as yr`) → **`on 1=1` cross product** → cartesian → 600s timeout | **SILENT** (timeout) | `parsing/v2/rules/select_statement_rules.py:205-224`; `dialect/common.py:253-318` | [bug_q78_residual_churn_001912.md](bug_q78_residual_churn_001912.md) |
| 3 | q14 | membership with an **expression RHS** (`x::string in (rowset.col::string)`) renders the rowset column inline against a CTE that's never joined/subselected → `Referenced table not found` BinderException | LOUD (binder, at execute) | `dialect/base.py:1540` (works for bare concept) vs `:1620` fallback (broken for expr RHS) | [bug_q14_resolution_churn_001912.md](bug_q14_resolution_churn_001912.md) — ✅ **FIXED claude** |
| 4 | q75 | nested-rowset output **both filtered and windowed** → lineage re-namespaced to `<outer>.<inner>.<col>` → `UndefinedConceptException: yearly.deduped.yr` | LOUD | `core/models/author.py:1194` (`with_namespace` double-prefix); the filter+window resolution path that requalifies an aliased output's lineage | [bug_q75_nested_rowset_filter_window_lineage_resolution.md](bug_q75_nested_rowset_filter_window_lineage_resolution.md) |
| 5 | q70 | rollup + **rank-within-parent**: `RecursionError building within_parent_rank` (CASE/window concept whose condition references `grouping()` + embeds an aggregate); plus a `GROUPING child must be a grouping column` binder variant | LOUD (uncaught recursion / binder) | `build.py:2824`/`3006` (no on-stack cycle break in `Factory._building`); grouping-in-CASE normalization | [bug_q70_recursion_within_parent_rank.md](bug_q70_recursion_within_parent_rank.md) |

| 8 | q64 | a **correlated equality inside a `?` filter used as a membership RHS** (`ss.ticket in (sr.ticket ? sr.ticket = ss.ticket)`) mints an unsourceable `_virt_filter` concept → `Could not resolve connections for query with output ['local._virt_filter_…']` (leaks internal repr) | LOUD | `discovery_utility.py:986` (`raise_if_filter_disconnected` no-ops because the two facts stay graph-connected) → generic raise at `concept_strategies_v3.py:672` / `query_processor.py:689` | [bug_q64_churn_013151.md](bug_q64_churn_013151.md) |



---

## B. Confirmed FIXED this session (verification record — re-ran the repros, sentinel gone)


---

## C. Already-landed harness/tooling fixes (this session)

---

## D. Non-blocking follow-ups (token/UX, not correctness)


- **`agent-info` is now the largest carried-context contributor** (post-explore-dedup). The full ~26–34KB reference index is re-sent every turn; it dominated q05's 1.28M (1.265M prompt / 12k completion). Worth a paging/relevance pass or per-topic fetch. (no handoff yet)
- **q64 cross-import disconnect messaging** — correct-by-design `cannot merge all concepts`, but the error should suggest the conformed-sibling/`all_sales` path. [bug_q64_self_pair_resolution_churn.md](bug_q64_self_pair_resolution_churn.md)
- **q97 guidance** (confirmed root cause, claude 2026-06-28) — scoped `full join a=b` is an *identity assertion* (a.k/b.k merge to one key), so the reference SQL's per-side `is null` presence flags are tautologically empty in trilogy. NOT a codegen bug. agent-info still steers cross-model set comparison toward this (untranslatable) full-join shape — add the canonical `all_sales` + `max(case when channel=…)` presence-flag idiom and a note that scoped `=` joins assert identity (per-side `is null` after a full join is always false). The agent's working fallback was `concat`-key + `count_distinct(k ? k in/not in other)`. (user updating constants.)
- **Population-wide `max(...) by *` null-key footgun** (q23) — agent-info note: filter nulls *before* the aggregate, not after. [bug_q23_residual_churn.md](bug_q23_residual_churn.md)
- **`count(<key>) by <key>` is always 1** (q23) — count is count-DISTINCT at the key's grain, so the natural "frequent items" form `count(item.id) by item.id > 4` silently returns empty. Documented in-model, but an agent-info gotcha for the "frequent X" pattern would help. [bug_q23_churn_013151.md](bug_q23_churn_013151.md)
- **Silent no-op runs** (q64) — a `.preql` containing only rowset/`with` definitions and no final `select` runs "Executing 0 statements" and reports `ok:true rows:0` with no warning; the agent can't tell broken from no-op. [bug_q64_churn_013151.md](bug_q64_churn_013151.md)

### Investigated this round, NOT framework bugs (record)
- **q38 (1.69M)** — NOT a bug (claude/user, 2026-06-29): **null is a valid group/intersect member**, and Trilogy's LEFT-join-with-null-group behavior is self-consistent. The 107-vs-155 gap is only because the *reference SQL* INNER-joins `customer` (dropping nulls) — a question-spec choice, not an engine error. The canonical `query38.preql` reaches 107 with null handling, so the answer is reachable. **Resolved by prompt update** (specifies filtering null). Reclassified out of §A. [bug_q38_churn_013151.md](bug_q38_churn_013151.md) (treat as superseded — keep only the "nullable FK → LEFT" note as reference). Lingering guidance value: expressing a null-guarded cross-channel intersect naturally is non-obvious.
- **q97 (709k)** — scoped `full join a=b` asserts a.k≡b.k (one merged key), so the reference SQL's per-side `is null` presence flags are tautologically empty. Trilogy is self-consistent; the agent transliterated SQL's distinct-column FULL OUTER JOIN, which doesn't translate. Root-cause confirmed by projecting both sides → both render `coalesce(a.k,b.k)`. Fix = guidance (§D), not codegen. [bug_q97_churn_001912.md](bug_q97_churn_001912.md)
- **q23 (1.10M)** — the `count(key)==1` footgun above + agent's frequent-items rule diverging from canonical. Both prior q23 framework bugs stay fixed. [bug_q23_churn_013151.md](bug_q23_churn_013151.md)
- **q05 (1.28M)** — the filed `_level asc` ordering defect did NOT recur (agent used correct totals-first order). Failure was an agent measure-selection error ("gross sales" → used `net_profit` instead of `ext_sales_price`); churn was agent-info prompt-replay + re-deliberating ambiguous "gross sales" wording. No new file.
