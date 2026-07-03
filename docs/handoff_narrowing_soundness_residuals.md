# Handoff: join-narrowing soundness residuals (pre-drop hole + evidence audits)

Written 2026-07-03, end of the session that landed rule B + the carve-out
deletion (docs/domain_graph_design.md "Landed (rule B + carve-out deletion,
2026-07-03)" is the full record; docs/handoff_rule_b_and_carveouts.md is the
executed predecessor). These are the CORRECTNESS-flavored residuals that
session mapped but did not build. None has a known failing test today — the
tasks below start by trying to construct one. If a repro can't be built
because the planner never produces the shape, document WHY (which invariant
prevents it) and close the item; a guard against an unreachable state is
noise.

**Owner bar (unchanged)**: correctness + performance, not byte-stability.
Battery row-assertions are the oracle; narrowing log diffs are wins. Owner
2026-07-03: "optimize for what *should* be globally optimal; don't chase
duckdb bugs."

## Task 1: the PRE-DROP hole (chain-context completeness)

### The gap

`_pair_side_fully_matches` (value_set_join_upgrade.py) proves the superset
side complete via `_complete_values(sup_concept, sup_cte, graph)` — a
judgment about `sup_cte` IN ISOLATION. But the join being narrowed lives in a
multi-join FROM chain, and an EARLIER join in that chain may have DROPPED
some of `sup_cte`'s rows before the target join reads its values:

    FROM a
      INNER JOIN b ON a.x = b.x          -- drops b rows unmatched by a
      FULL  JOIN c ON b.k = c.k          -- narrowing candidate

If `c.k ⊑ b.k` is proven (declared/structural/`~`-stamp) and `b` is complete
standalone, `_narrow_directionally` narrows the FULL to LEFT (preserving the
chain) — but c rows whose k lives only in the PRE-DROPPED b rows lose their
partner and get dropped. The FULL was load-bearing for them.

This is the exact mirror of the NULL-EXTENSION hole fixed 2026-07-03
(`_null_extended_before`, caught by `test_join_grain` returning wrong rows):
that guard covers the SUB side being null-extended by an earlier outer join;
nothing covers the SUP side being row-dropped by an earlier INNER (or by an
earlier LEFT where sup sat on the optional side). The hole PREDATES rule B —
the old directional path had it too — but rule B and the genuine-stamp path
widened how often directional narrowing fires, so the surface grew.

### Where the earlier INNER comes from

Three producers, in decreasing safety:
1. A sound equivalence upgrade (key sets equal) — drops nothing; narrowing
   after it is fine. A guard must NOT block this or q64-style chains regress.
2. Cross-CTE null-rejection (`UpgradeJoinOnGuards`) — the INNER reflects a
   user-authored WHERE; rows ARE dropped, legitimately, before the target
   join. This is the reachable-repro candidate.
3. Planner-emitted INNER (both sides complete-by-declaration).

### Concrete steps

1. Try to build a failing repro: `tests/join_matrix/` style, three sources,
   `b` complete for k, `c` authored `~` on k (genuine-stamp path), a WHERE
   that null-rejects `a`-side columns so the a–b join renders INNER and
   drops a b row whose k value some c row needs, then a query whose FULL
   b–c join narrows. Assert exact rows against the DuckDB oracle.
2. If reachable: extend the chain guard — a `_rows_dropped_before(cte,
   target, member)` sibling of `_null_extended_before` that walks
   `cte.joins` up to the target and reports whether `member`'s rows can have
   been dropped (member on the non-preserved side of an earlier INNER /
   LEFT-optional / RIGHT-base). Consume it in `_narrow_directionally` for
   the SUP side (both directions) and in `_upgrade_to_inner`. The subtlety:
   distinguishing producer 1 (row-identical INNER, safe) from producer 2
   (filtering INNER) at optimize time — join objects don't record WHY they
   are INNER. Options: (a) conservative — treat any earlier INNER on the sup
   side as dropping, accept the narrowing losses, measure them via the
   battery census; (b) thread a `row_identical` stamp onto joins the
   upgrades themselves produced (they KNOW they were row-identical) so only
   unstamped INNERs block. (b) is the right shape; (a) is the fallback.
3. Guard sweeps: `tests/join_matrix/`, `tests/test_scoped_join.py`,
   `tests/optimization/test_value_set_join_upgrade.py`,
   `tests/test_scoped_derived_rowset_join_matrix.py`, gcat, battery
   (`pytest tests/modeling/tpc_ds_duckdb/test_queries.py -q`, 106/106).
   Battery FULL census at handoff: 24 (`grep -c "FULL JOIN"
   tests/modeling/tpc_ds_duckdb/zquery*.log`) — a conservative guard that
   regresses q64's three readback LEFTs is NOT acceptable; those chains have
   an earlier `INNER ... on 1=1` (cross join, drops nothing) and an earlier
   LEFT that must not trip the sup-side check.

## Task 2: audit the structural-EQUAL mint (unfiltered-rowset edges)

### The gap

`structural_domain_edge` (domain_graph.py) mints `output ≡ content` for an
unfiltered rowset body. Rule B now CONSUMES these edges for narrowing, so
their soundness is load-bearing where it used to be advisory. Two gates
already exist: a filtered body (where/having) mints a conditioned ⊑ instead,
and a body carrying scoped joins mints NOTHING (the collapse makes the
output's domain the join-group union — caught 2026-07-03 by
`test_rowset_key_readback_full_k_aw` returning wrong rows).

Remaining exposure: a MULTI-SOURCE body with no scoped joins and no where —
`with rs as select s.period, t.x` — where the planner's sourcing of the
body could drop s rows (a non-preserving join between s and t) without any
condition appearing on the select. Then `rs.period ≡ s.period` is a lie in
the ⊒ direction (rs is a proper subset), and a rule-B proof through it
narrows a FULL that was preserving real rows... note this direction of lie
(subset pretending equal) makes the SUP side overclaim — same failure mode
as task 1.

### Concrete steps

1. Determine whether the planner can drop base rows in an unfiltered
   multi-source rowset body. The phase-2 always-preserving flip suggests NO
   for enrichment joins (that is its whole point), but membership semijoins
   and same-grain zips are worth checking. Build matrix cells:
   `with rs as select a.k, b.v` over (complete a, partial b), (partial a,
   complete b), disjoint-key sources — read each back through a FULL join
   and assert rows.
2. If a dropping shape exists: gate the mint on body source-count == 1
   (conservative), or mint ⊑ (output ⊑ content) instead of ≡ for
   multi-source bodies — containment in that direction stays true.
3. Battery + rowset guardrails: `tests/test_rowset_generation_matrix.py`,
   `tests/test_rowset_cross_datasource_outer_read.py`.

## Task 3 (small, quality): sup-side partial stamps at pseudonym addresses

`_own_coverage_partial` blocks a completeness claim only on an EXACT-address
stamp. A sup side carrying a genuine `~` stamp at a PSEUDONYM address of the
key (an aliased concept bound `~` by the same scan) slips through and the
side overclaims completeness. Pre-existing asymmetry (the sub side's
`_genuine_partial_stamp` closes over `_key_addresses`; the sup side check
does not), never observed failing. Try to repro via a model that binds a
merged/aliased concept `~` on the dim side; if reachable, the fix is to
close the sup check over `_key_addresses(p)` for GENUINE stamps only
(relation stamps must stay exact-address — that closure being wrong is
documented in `_own_coverage_partial`'s docstring and is what the carve-out
deletion preserved).

## Explicitly NOT in scope

- q04 `is not distinct from` reduction (`SimplifyNullSafeJoins`) — pure
  perf, no correctness angle. Separate effort.
- The q64 customer↔customer_address enrichment FULL — narrowing it needs an
  AUTHORED fact (`~` on the FK binding or an FK-inference feature), not an
  optimizer change; trusting undeclared fact-FK schema completeness was
  deliberately rejected (see `_authoritative_scan`'s docstring).

## Environment / process notes

- venv `.venv/Scripts/python.exe`; after repo-wide changes: `ruff check .
  --fix`, `mypy trilogy`, `black .`.
- Full suite: `pytest tests/ -q -m "not adventureworks_execution"` (~12
  min). Expected failures at handoff: ONLY the 2 OpenAI-key tests
  (tests/ai/test_providers_basic.py, tests/modeling/faa/test_llm.py).
- ⛔ Never `git stash`/`checkout --`/`reset` in the shared tree — parallel
  agents work here concurrently (this session: filter_node.py / group_node.py
  changed under us mid-run, and a full-suite result was invalidated by it).
  A/B against a commit via a temporary `git worktree` + copying ONLY your
  changed files in.
- Battery logs (zquery*.log) regenerate on every battery run — treat diffs
  as reviewable evidence; before attributing a log flip to your change,
  re-run the battery on an ISOLATED worktree (this session burned an hour
  chasing a flip caused by a parallel agent's in-flight edit).
