# Trace audit — run `20260911-001859` (deepseek-v4-flash, 44 traces)

Every final query's rows were checked by hand: no leg produced Trilogy-generated
SQL that DuckDB rejected, and no passing answer had wrong rows. The cost and the
three failures come from the items below. Items marked **verified** were
reproduced outside the traces; the rest cite the trace line.

## Framework

1. **Ingest infers no foreign keys on `*_identifier` columns** (verified).
   `FK_SUFFIXES = ("_sk", "_id", "_key", "_fk")` in
   `trilogy/scripts/ingest_helpers/fk_inference.py:37`; `_fk_stem("policy_identifier")`
   is `None`, so `ingest --all` wrote 13 island models with `imports: 0` and
   every join in the DDL was rediscovered per query with scoped joins. This is
   the whole 1,574k-vs-649k token gap between `ingest` and `enriched`. Fix:
   accept `_identifier` (and read `duckdb_constraints()` when the DB has FKs).
2. **Single-column marker tables are dead ends for ingest.** `Premium`,
   `Loss_Payment`, `Loss_Reserve`, `Expense_Payment`, `Expense_Reserve` have
   one column that is both PK and FK; `generate_candidates` skips a table's own
   identity, so even with (1) they stay unlinked. Agents spent 2-6 calls each
   learning "one concept, no imports". The curated model folds them into
   `amount_kind` / a filtered `premium` source; ingest could emit
   `merge x.k into ~parent.k` plus an `is_<table>` flag on the parent.
3. **A subset join whose narrow side contributes only its key is elided**
   (verified). `select sum(pa.policy_amount) subset join prem.policy_amount_identifier = pa.policy_amount_identifier`
   compiles to a scan of `Policy_Amount` alone (5,698,000; Premium never
   referenced), and projecting `prem.policy_amount_identifier` renders the
   coalesced `pa` key, so the probe shows ids that are not in Premium. The
   ingest q03 agent concluded "scoped joins don't filter" and abandoned joins
   (`agent_log.q03.conversation.txt` ~:1890-1991). `where prem.k is not null`
   gives 98,000. Suggest a `warnings` entry when a joined model is elided, or
   render an explicitly projected narrow-side key as its own NULL-extended
   column.
4. **Clause-order parse error is a raw pest expectation list.**
   `select … where … subset join` fails with
   `expected limit, order_by, THEN_LA, having, …` (ingest q07:1061). The GROUP BY
   case has a Syntax [103] hint; this one deserves the same ("`where` precedes
   `select`; `subset join` follows the select list").
5. **`explore --ns <unknown>` silently returns the root namespace** (verified).
   Enriched q03 asked `--ns policy` on the wrong file and read the answer as
   authoritative. Emit a warning naming the unmatched namespace.
6. **`explore` labels pinned-aggregate arithmetic as properties.** `loss_amount`
   and `total_loss` (`sum(...) by claim.id` inputs) show under `properties`, so
   the enriched q05 agent wrapped them in another `sum()`. Right answer here,
   fan-out risk elsewhere.
7. **Parse-error `Location:` snippets carry `\r`** (`...n_holders\r ???`,
   enriched q03/q04/q07; ingest q07/q08). `errors.py` only strips `\n`;
   the Windows text-mode stdin write adds CR. Cosmetic.

## Tool / harness

8. **`agent-info cli` advertises commands the eval disables** (verified:
   `agent_info_docs/cli.py:11,19-20` vs the gate in `agent_tools.py:485-503`).
   In 7 of the 9 ingest queries with errors the agent read `cli` and then
   issued `file read` / `database describe`; 36 of ingest's 43 errors are this.
   Thread the disabled flags into the drilldown.
9. **The refusal text points at a substitute that cannot answer.** "Use
   `explore` … it chains in imported dimensions" — the ingest model has no
   imports. Point at the scoped-join / `in` idiom instead.
10. **`run_query` runs only the last `;`-separated statement** (verified,
    `agent_sql_tools.py:89`) and reports `statements: 1`. Five SQL agents
    batched DESCRIBEs and lost results (sql_bare q04/q09, sql_schema
    q02/q06/q08). Execute each or refuse.
11. **Row cap truncation is misread and uncounted** (verified). The SQL toolset
    middle-truncates at 25 rows as `<redacted N rows>`; sql_bare q05 read it as
    sandbox redaction and re-queried per table (part of its 315k tokens).
    `scoring.py` counts only the Trilogy toolset's `...[truncated` marker, so
    the report says `truncated: 0`. Count both; reword the marker.
12. **`trilogy.toml` ships the whole `[agent]` section into the workspace**
    (verified). SQL agents read it looking for schema hints and found the
    reviewer A/B note and a `schema.md` mention that did not apply. Write
    `[engine]` only.
13. **Two candidate filenames.** The SQL tool prompt says `query<NN>.sql`, the
    task says `answer_<opaque>.sql`. Harmless (the harness stages it), but
    sql_schema q01 noticed the conflict.
14. **`explore` refuses a directory** (ingest q04). A directory mode would have
    answered "which model has an agent id".

## Prompt

15. **No scale hint.** "Returns at least one row" plus a 2-policy fixture made
    agents distrust correct answers: 18 "seems small" deliberations across the
    SQL legs, 10 of 11 enriched queries ran a "really only 2 rows?" probe, and
    enriched q06's four-call spiral. One sentence stating the fixture size
    removes ~15 tool calls per leg.
16. **"Position and values matter" without a key convention.** sql_bare q05
    spent ~19k reasoning tokens over 13 iterations deciding between
    `Claim_Identifier` and `Company_Claim_Number`. Say "prefer the business
    key", or make the scorer key-tolerant.

## Question wording

17. **q03 fails on every unmodeled leg.** "total amount of premiums that a
    policy holder has paid" never says "by holder"; every agent returned the
    scalar `(98000,)` against gold `(1, 98000)`. The curated model's comment on
    `total_premium` is what carried the enriched leg. Reword to "by policy
    holder id" (parallel with q06) or accept the scalar.
18. **Never-drop joins vs inner-join gold** (q02, q08). Trilogy keeps the
    claim-less policy; agents on both Trilogy legs pinned it out, but ingest q08
    spent ~20k reasoning tokens deciding to. The prompt could state "group over
    the facts; do not pad dimension members".
19. **Fixture inconsistency worth knowing**: claim 2's insurable object belongs
    to policy 2 but its `Claim_Coverage` row points at policy 1's coverage. The
    gold follows the coverage path; a question about insured objects would
    disagree.

## Curated model

20. `claim.preql`'s comment says `where claim.id is not null`; `query02.preql`
    uses `where claim.close_date is not null`. Both are right for different
    asks; the comment should name both.
21. `total_premium`, `policy_count`, `claim_count` exist but 7 of 11 enriched
    agents re-derived them by hand (identical results). Question-phrase
    comments ("'total premiums paid' questions select this") steer to them.
22. `loss_amount` / `total_loss` comments should say "already per claim; do
    not `sum()` next to `claim.claim_number`" (see 6).

## Agent (correct, clear error; nothing to change)

`by * as all_rows;` / trailing `by *;` (enriched q04/q07, Syntax [213] message
is good), missing `;` (q03), `file show` invented three times (ingest q03),
alias typo (sql_schema q03), q09 over-projection on ingest.

## Priority

1. Ingest: `_identifier` FK suffix (1) and marker-table folding (2) — removes
   the discovery loop behind every heavy ingest trace.
2. `agent-info cli` gated by the disabled-tool flags (8, 9).
3. `run_query` multi-statement and the row-cap marker/count (10, 11).
4. Subset-join elision warning and the clause-order hint (3, 4).
5. Scale sentence and key convention in the task prompt (15, 16); q03 wording (17).

## Fix pass (commit "Agent tooling fixes from the ACME trace audit")

Landed: 1 (`_identifier` FK suffix; ingest now links 12 of the 13 ACME tables),
4 (Syntax [231]), 5 (`explore --ns` warning), 7 (`\r` strip), 8 and 9
(disabled commands named in the prompt, the `cli` drilldown, and the
refusal), 10 (`run_query` runs every statement), 11 (`<hidden N rows>` with a
note; the scorer counts it).

Not landed, with what blocked them:

- **2, marker-table folding.** Letting a one-column table's key be an FK
  candidate yields `root datasource Premium (Policy_Amount_Identifier:
  ~Policy_Amount.policy_amount_identifier) grain (...)` with no concept of
  its own, which is worse for an agent than the island model (the `in`
  idiom stops working). Giving it a flag column exposed two `raw()`
  limitations: a `raw('''true''')` binding on a partial source is hoisted
  as a constant, so `select pa.id, is_premium` reports `True` for every
  parent row without touching `Premium`; and a `raw('''col IS NOT NULL''')`
  binding renders unqualified in the consuming query, so it is ambiguous
  under any join. The sound shape is the curated model's: a query-backed
  parent datasource that LEFT JOINs the markers into an `amount_kind` /
  `is_<marker>` column. That is an ingest design change, not a patch.
- **3, subset-join elision.** Exact repro on the ingest models
  (`Premium` = key-only island, `Policy_Amount` = the parent):

  ```
  select sum(pa.policy_amount) as total
  subset join prem.policy_amount_identifier = pa.policy_amount_identifier;
  -- SELECT sum("pa_Policy_Amount"."Policy_Amount") FROM "Policy_Amount"   -> 5,698,000

  select prem.policy_amount_identifier as prem_id, pa.policy_amount_identifier as pa_id
  subset join prem.policy_amount_identifier = pa.policy_amount_identifier;
  -- both columns render from "Policy_Amount"; 12 rows, ids 1,3,5,... are not in Premium

  where prem.policy_amount_identifier is not null
  select sum(pa.policy_amount) as total
  subset join prem.policy_amount_identifier = pa.policy_amount_identifier;
  -- INNER JOIN on a presence CTE                                          -> 98,000
  ```

  Consistent with "joins never drop a row" and "the projected key is the
  coalesced group axis", but the second query is the one an agent writes to
  check membership, and it shows the narrow side's key carrying values the
  narrow side does not have.
- **6**, explore's property/metric label for pinned-aggregate arithmetic:
  the payload already prints the full derivation with its grain, so left
  as is.
- **13**, the two candidate filenames in the SQL toolset: harmless, left.
- **15, 16, 17** are prompt/question wording; unchanged so the run stays
  comparable to the blog.
