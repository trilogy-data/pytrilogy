# q59 token sink (954k) — NOT a Trilogy codegen bug; the FAIL is a bad `PRAGMA tpcds(59)` reference

**Target:** q59 in `evals/tpcds_agent/results/20260706-222300/` — 954k tokens,
scored `fail | ref 100 | cand 100 | result set differs from reference`.
(Old run `20260706-135542_enriched` ~1.03M; barely moved.)

**Verdict:** The agent's **final query is essentially correct** — its generated SQL
runs clean and matches the curated `tests/modeling/tpc_ds_duckdb/query59.sql` on
**99 of 100 rows** (the 1 diff is a LIMIT-100 tie-break at a multi-SCD store, not a
wrong value). The scored FAIL is a **reference/scoring defect** (q54 family):
`PRAGMA tpcds(59)` fan-out-duplicates every result row **49×**, so the multiset
scorer can never match a correctly de-duplicated candidate. The "cannot merge"
error in the log is a **correct disconnect** the agent caused itself and fixed two
turns later. There is **no silent framework wrong-result** here.

---

## 1. The "cannot merge" error signature — CORRECT disconnect, self-corrected (not a bug)

New-log error (`agent_log.q59.conversation.txt` msg 33, line 1999):

```
Resolution error in query59.preql: Discovery error: cannot merge all concepts into
one connected query (statement at line 16). ... split into 2 disconnected subgraphs:
{...day prices, _weekly_store_code, _weekly_store_name}; {_weekly_week_seq, _weekly_year}.
```

Root: that intermediate query (msg 30) used a **standalone** `import raw.date as d`
and selected `d.week_seq` / `d.year`, while the fact is `import raw.store_sales as ss`
whose model binds `SS_SOLD_DATE_SK: ?date.id` — i.e. the FK-joined date path is
`ss.date.*`, a **different** concept than top-level `d.*`. The standalone `d`
columns have no join path to store_sales, so they correctly split off. The
aggregate `sum(ss.sales_price ? d.day_name=w)` resolved via `ss` (with store dims);
only the free `d.week_seq/d.year` disconnected. The error message correctly says
"missing a join or merge."

Minimal repro (current engine, workspace `raw/` model):

```
# A (msg-33 shape): standalone date import  ->  DisconnectedConceptsException
import raw.store_sales as ss; import raw.date as d;
select ss.store.name as sn, d.week_seq as wk, d.year as yr,
       sum(ss.sales_price ? d.day_name='Sunday') as sun;

# B: through ss.date  ->  OK (resolves)
import raw.store_sales as ss;
select ss.store.name as sn, ss.date.week_seq as wk, ss.date.year as yr,
       sum(ss.sales_price ? ss.date.day_name='Sunday') as sun;
```

A raises the exact disconnect; B resolves. The agent switched to `ss.date.*` /
`ss.store.*` at msg 34 and the query resolved. **Agent modeling error, correctly
reported, self-corrected in ~2 turns. Not a framework false-disconnect.**

## 2. Why it scores FAIL — the reference is fan-out-duplicated (root cause)

Scoring uses `PRAGMA tpcds(59)` (via `evals/common/scoring.py::_load_reference`,
L578–591 — falls back to `PRAGMA {extension}({idx})` because no
`custom_refs_dir/query59.sql` exists for the tpcds_agent eval).

The built-in q59 (`tpcds_queries()` #59) builds `wss` at (d_week_seq, ss_store_sk)
grain, then in **each** of subqueries `y` and `x` joins:

```
FROM wss, store, date_dim d
WHERE d.d_week_seq = wss.d_week_seq  AND  ss_store_sk = s_store_sk  AND d_month_seq BETWEEN ...
```

`d.d_week_seq` is **non-unique** (7 calendar days share one week_seq → 7 date_dim
rows), and the join has **no date surrogate key** — so `y` fans **7×** and `x` fans
**7×**. The final cross join on `s_store_id1=s_store_id2` then yields **7×7 = 49
IDENTICAL copies** of each correct (store, week) row. Measured on the run DB:

| source | rows | distinct | max dup |
|---|---|---|---|
| `PRAGMA tpcds(59)` (scorer's reference) | 100 | **3** | **49** |
| curated `tests/.../query59.sql` (DISTINCT-week CTEs) | 100 | 100 | 1 |
| agent candidate (`workspace/query59.preql`) | 100 | 100 | 1 |

The curated `query59.sql` dodges the fan-out with `weeks_y/weeks_x AS (SELECT
DISTINCT d_week_seq ...)`. Candidate **== curated query59.sql on 99/100 rows**
(round-6 multiset); the single diff is the 100th row at the LIMIT boundary
(candidate collapses a store_id that has 2 `s_store_sk`, so it reaches one extra
`able` week where the SQL keeps SCD versions separate — a defensible grain choice,
not a bug). Candidate values are byte-identical to the reference values (the 49
dup copies all equal the candidate's single row).

**This is the same class as the memory q54 finding (store fan-out reference).**
Fix is eval-side: give tpcds_agent a `custom_refs_dir/query59.sql` (the curated
non-fanned SQL), or de-duplicate the PRAGMA reference. Not a Trilogy change.

## 3. What actually consumed 954k tokens

Cumulative context across **58 turns** (final prompt ~86k; the total is the sum of
growing per-turn prompts), NOT one catastrophic output and NOT an engine crash loop.
Drivers, in order:
- **Genuinely hard shape:** 7-way day-of-week pivot + cross-year self-join on
  (store, week=week−52). Lots of schema exploration up front (msg 3 alone ~845 lines).
- **The self-corrected modeling error** (§1) — a couple of turns.
- **Verification / approach flip-flopping (the biggest chunk):** the agent had a
  WORKING subset-join query returning 318/100 rows by msg 36, then spent msgs 38–57
  (the largest-context turns) hand-verifying rows, second-guessing year-alignment,
  and rewriting toward a window-`lead(...,52)` shape (msg 46) before reverting to the
  subset-join version that is the final file. It never sees the score, so nothing
  forced this churn except low confidence — a guidance/verifiability gap, not a bug.

## 4. Union/subset-join-between-rowsets collapse (memory FIXED 2026-07-04) — confirmed NOT regressed

The final query uses three composite `subset join` clauses
(`this_year.week_seq = next_year.week_seq - 52` + store_name + store_code). Generated
SQL resolves as a **true composite AND** LEFT-join with coalesced keys (the derived
`week_seq-52` key is materialized as `_virt_func_subtract_*`; both sides expose their
own key members). No side-collapse, no co-key drop, no fan-out. The earlier
`20260704-035023` collapse regression is fixed and stays fixed on this engine.

## Classification

- **Primary defect (drives the FAIL): eval reference bug — `PRAGMA tpcds(59)` is
  49× fan-out-duplicated.** `evals/common/scoring.py::_load_reference` L578–591 uses
  the built-in PRAGMA; add a curated `query59.sql` override (q54 pattern). NOT Trilogy.
- **"cannot merge" signature: correct disconnect (agent used standalone `raw.date`),
  self-corrected.** Optional guidance nicety: nudge toward `ss.date.*` FK path when a
  standalone dimension import disconnects.
- **Token sink: complex query + verification churn, no framework wrong-result.** The
  engine produced correct SQL; the agent lacked a way to confirm it and thrashed on
  manual checks. Guidance/verifiability gap, not a codegen bug.
- **Regression watch:** `test_fifty_nine` only asserts SQL length < 12000, and strict
  row-scoring of even the curated preql fails 100-vs-100 against the fanned PRAGMA —
  so this reference defect is invisible to the suite. Worth a curated-reference cell.

## Files
- Candidate: `evals/tpcds_agent/results/20260706-222300/workspace/query59.preql`
- Log: `.../agent_log.q59.conversation.txt` (msg 33 = the disconnect; msgs 38–57 = churn)
- Curated clean reference: `tests/modeling/tpc_ds_duckdb/query59.sql` (DISTINCT-week CTEs)
- Scorer reference selection: `evals/common/scoring.py::_load_reference` L578–591
