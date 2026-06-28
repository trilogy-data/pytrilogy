# q23 residual churn — verdict: old framework bugs FIXED, remaining churn is PURE AGENT THRASH (no handoff-worthy bug)

Run: `evals/tpcds_agent/results/20260628-194910`, `agent_log.q23.jsonl` (q23 ~856k tokens, FAILED on
result mismatch — ref 19 rows vs candidate 4 rows; no crash).

## TL;DR

- **Both prior q23 framework bugs are FIXED** on the current engine (re-verified by re-running their
  minimal repros — both now `generate_sql` clean AND execute clean against the workspace DB).
- **The 856k churn is agent-only.** The whole run hit exactly **one** framework error: a single clean
  `Syntax [211]: Expression in 'by' clause must be wrapped in parens` (agent fixed it on the very next
  write). Every `run` afterward returned `exit_code: 0`. The rest of the iterations were the agent
  debugging **its own empty/wrong results** — a semantic modeling mistake, not a framework defect.
- **No residual framework issue. No new handoff-worthy bug.** q23 ultimately fails on result accuracy
  (agent logic), not on any Trilogy crash or invalid SQL.

## Old framework bugs — re-verification (current engine, eval workspace `raw.store_sales`/`raw.catalog_sales`)

Both minimal repros from the prior reports were re-run via `generate_sql` and executed against
`evals/tpcds_agent/results/20260628-194910/workspace/tpcds.duckdb`:

| prior report | minimal repro | before | now |
|---|---|---|---|
| `bug_q23_groupby_contains_aggregate_union_channel.md` (was Status CLOSED/FIXED 2026-06-28) | derived-threshold membership `catalog.fk in best_customers.cust_id`, HAVING `customer_total > best_customer_threshold` | DuckDB BinderException "GROUP BY clause cannot contain aggregates" at execute | **FIXED** — clean SQL (GROUP BY holds only the key, no CASE-over-aggregate); **executes OK, 1346 rows** |
| `bug_q23_invalid_reference_best_customers_membership.md` (was Status OPEN) | hidden `--sum(...) as lifetime_total` rowset output referenced downstream | `generate_sql` raised `ValueError: Invalid reference string` (`INVALID_REFERENCE_BUG * INVALID_REFERENCE_BUG`) | **FIXED** — clean SQL (no sentinel); **executes OK, 5 rows** |

So the `INVALID_REFERENCE_BUG` sentinel (hidden `--` aggregate dropped from its grouping CTE and
re-derived from lineage) and the GROUP-BY-contains-aggregate BinderException are both gone. The OPEN
report `bug_q23_invalid_reference_best_customers_membership.md` can be marked **FIXED** (verified
2026-06-28); recommend leaving the report as a closed record.

## What the 856k actually churned on (from `agent_log.q23.jsonl`)

Reconstructed turn-by-turn:

1. **Log line 25** — `file write` rejected with `Syntax [211]: Expression in 'by' clause must be
   wrapped in parens` (the agent wrote `... by substring(store_sales.item.desc, 1, 30)` without
   parens). Clean, actionable error; the agent fixed it immediately (next write, line 26 → run line 29
   succeeded). **This is the only framework-surfaced error in the entire run.**
2. **Lines 29-89** — every `trilogy run query23.preql` returned `exit_code: 0`. The agent looped
   purely on **wrong/empty data**:
   - Query returned 0 rows; agent probed the data (store sales exist for 2000-2003, max total
     236266.51, frequent description prefixes exist, membership `in` works in isolation).
   - Root cause it eventually found (line 80): its `best_customers` threshold
     `max(all_customer_totals) by *` included the **`customer.id IS NULL` aggregate bucket**
     (lifetime total 18,359,188.84), which dominated the max so **no real customer cleared 50%** →
     empty result. It added `customer.id is not null` and finally got rows.
3. Final state: runs cleanly, returns results, but the result set still differs from the reference
   (4 vs 19 rows) — an accuracy/logic gap (likely the frequent-items / channel / date-window modeling),
   not a framework failure.

## Verdict

- **Old bugs:** both FIXED and verified (execute clean).
- **Residual framework issue:** none. One clean `[211]` syntax error (immediately self-corrected) is
  the only framework touchpoint; it is already a good, actionable message.
- **Nature of the churn:** pure agent thrash — debugging an empty result that stemmed from a NULL-key
  aggregate inflating a `max(... ) by *` threshold (a modeling responsibility), then an unresolved
  result-accuracy gap. **No handoff-worthy framework bug.**

### Possible non-bug follow-ups (optional, low priority)

- An agent-info / gotcha note: `max(<agg>) by *` over a key with NULLs includes the NULL-key bucket;
  filter `key is not null` before deriving a population-wide threshold. This is the single trap that
  consumed most of q23's iterations and is a recurring TPC-DS "best customers" pattern.
- Canonical `tests/modeling/tpc_ds_duckdb/query23.preql` sidesteps the whole thing with a single
  conformed `all_sales` source and visible `by`-grain aggregates (no hidden rowset measure, no
  cross-source membership-as-CASE), which is why it neither churns nor trips either old bug.
</content>
