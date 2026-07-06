# Token-Sink Rebaseline — enriched run `20260706-222300`

Fan-out over every query >500k tokens in the rebaseline of the 44-query target set
(38 fails ∪ 16 over-500k from prior run `20260706-135542_enriched`). One read-only
subagent per sink. This index collects the verdicts; each row links its bug report.

## Rebaseline scorecard (44 target queries, enriched leg)
- **16 FIXED** (fail→pass): q6 q11 q16 q17 q25 q41 q49 q60 q62 q70 q73 q74 q76 q88 q90 q94
- **3 "regressed"** (q5, q83, q84) — ALL confirmed **NOT code regressions** from commit
  `4e69c5547`; each is agent-strategy variance exposing a *pre-existing* framework issue.
- Headline report.md says 69/99 but is polluted (spliced 55 rows from the `sql_schema`
  leg). Ignore it; use the per-query enriched comparison.

## The 14 sinks — verdicts

| q | new tok | status | class | one-line root cause | fix locus |
|---|---|---|---|---|---|
| q23 | 1.08M | fail | **ENGINE (silent)** | `sum(x) by k ? off-grain-cond` sums LIFETIME + post-agg CASE gate → wrong threshold | `filter_node.py` pushdown L114-146 + FilterItem/agg lowering in `build.py` |
| q54 | 1.39M | pass* | **ENGINE (silent)** | `subset join a=b` w/ ROWSET superset anchor renders FULL+coalesce not LEFT → non-members leak | `build.py` `_rowset_outer_pair` ~L2400-2414 |
| q87 | 918k | fail | **ENGINE (silent)** | `HideUnusedConcepts` prunes UnionCTE outputs ignoring `set_operator` → EXCEPT/INTERSECT compares 1 col | `optimizations/hide_unused_concept.py` L87-128 |
| q84 | 749k | fail | **ENGINE (silent)** | `union join` onto rowset drops rowset grain cols from GROUP BY → silent dedup (16→15) | group/rowset grain resolution (`group_node`/`rowset_node`) |
| q83 | 865k | timeout | **ENGINE (perf)** | RIGHT-JOINs huge `*_sales` facts that yield no output col (item via returns FK) → 4.6min runaway | `concept_strategies_v4.py` source selection |
| q30 | 790k | fail | **ENGINE (DX→silent)** | `_find_similar_concepts` ranks suggestions by dict order, 6-cap → omits correct key, agent uses wrong customer | `models/environment.py:390` (`path_matches` L416-425, cap L459-463) |
| q59 | 954k | fail | agent near-miss | ~~eval ref bug~~ MISDIAGNOSED: `query59.sql` override already exists+used; real = 99/100, cand (`able`,wk5323) bumps (`ation`,wk5318) past LIMIT 100 | none (not an eval bug) |
| q11 | 624k | pass | **DX / error msg** | disconnect error doesn't name curated `all_sales` for shared-dim union → 400k thrash | `discovery_utility.py:762` `connected_equivalent_suggestions` |
| q05 | 635k | fail | agent+guidance | `cast(0 as float)` union placeholder drift (no 8-byte DOUBLE); untyped `0` is exact | type-system gap `dialect/base.py:343,377-381` + guidance |
| q02 | 1.41M | pass | agent variance | window `lead(…,53)` under narrow WHERE → all-NULL (correct); agent misread. All prior q02 bugs fixed | guidance note: window sees post-WHERE rows |
| q14 | 631k | fail | agent+guidance | sums `ext_list_price` vs ref `quantity*list_price`; UPPER channel; HAVING at rollup grain | guidance: `all_sales.preql:44` + channel enum |
| q64 | 865k | fail | agent+guidance | two INDEPENDENT semijoins vs correlated `(item,tkt) in (...)` → 7.2× loose; +ext vs per-unit | guidance: composite-tuple membership |
| q75 | 605k | fail | agent+guidance | coalesces the SALE side of net measure to 0 (ref leaves bare) | guidance: coalesce only the missing side |
| q80 | 521k | fail | agent+guidance | `sum(a)-sum(b)` two-aggregate vs per-row `sum(a-b)` w/ NULL operand → Δ1285 | guidance: `constants.py:177`, `syntax_examples.py:653` |

\* q54 passes via the `in` idiom but the `subset join` idiom silently returns a superset;
the agent burned ~600k unable to tell which was right.

## Cross-cutting meta-finding — guidance steers agents INTO pre-existing bugs
Commit `4e69c5547` expanded guidance toward `all_sales`, `except/intersect`, and
`union/subset join`. Several sinks are agents *following that guidance* straight into
latent framework defects: q87 (except optimizer), q54 (subset-join rowset anchor),
q83/q84 (all_sales + union join), q11 (all_sales disconnect message). The engine fixes
below matter more now precisely because the guidance points agents at these paths.

## Six real engine bugs — priority order (silent > perf > DX)
1. **q23** filtered-aggregate off-grain `where` gates instead of filtering input (silent, wrong analytics).
2. **q54** subset-join rowset-anchor → FULL leak (silent, subset/union-join family correctness).
3. **q87** EXCEPT/INTERSECT output pruning (silent; NEW-feature latent bug, guidance-amplified).
4. **q84** union-join-onto-rowset grain collapse (silent dedup).
5. **q83** unnecessary huge-fact join (perf; can time out full queries).
6. **q30** suggestion ranking (DX defect that becomes silent-wrong via agent trust).

## Non-engine follow-ups
- **q59:** NO action — `query59.sql` override already exists and is used; failure is a
  99/100 agent near-miss at the LIMIT boundary, not an eval defect.
- **DX:** name `all_sales` in the disconnect suggestion (q11); rank suggestions by fewest
  extra segments (q30 shares this helper family).
- **Guidance:** window-after-WHERE (q02), coalesce-only-missing-side (q75/q80),
  `ext_list_price ≠ qty×list_price` + channel enum case (q14), composite-tuple
  membership for same-pair matching (q64), `cast(0 as float)` drift (q05).
