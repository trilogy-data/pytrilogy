# Scope diagnostics evaluation set

## Purpose

Measure whether factual aggregate/window input-scope diagnostics help agents
correct filter-placement and computation-boundary mistakes. This set excludes
role selection, null-preserving joins, key choice, and counting-grain failures
unless a query also has a distinct, historically observed scope failure.

The evaluation should compare the same agent and prompts with scope diagnostics
disabled versus enabled. The diagnostic contains no warnings and should not be
mentioned in the business question.

## Primary set

| Query | Scope family | Historical agent mistake | What the diagnostic must make visible |
|---|---|---|---|
| **Q30** | benchmark population vs output subset | Restricted the per-return-state average to customers whose current home state is GA | Customer totals see 2002 returns; the state average sees all customer totals; `home_state = GA` is applied only after the average |
| **Q81** | benchmark population vs output subset | Same failure family as Q30 for catalog returns; previously also exposed illegal predicate pushdown | The returning-state average is computed from the complete circa-2000/non-null-state customer-total rowset, before the final GA restriction |
| **Q47** | window boundary population | Computed `lag`/`lead` after filtering to qualifying 1999 rows, causing windows to skip excluded neighboring months | The windows see December 1998, all 1999 months, and January 2000; year/deviation qualification occurs after the windows |
| **Q57** | window boundary population | Catalog-channel twin of Q47; agents repeatedly omitted or filtered out boundary months before `lag`/`lead` | Same boundary-versus-output distinction as Q47, with the call-center partition |
| **Q14** | leaf filter before rollup | Applied the above-average test as HAVING on rollup output, so subtotal rows included leaves that should have been removed | The overall 1999–2001 average has its own population; November 2001 leaf aggregates are filtered against it; rollup consumes only surviving leaves |
| **Q92** | aggregate-local filter vs report filter | Allowed manufacturer or per-line threshold filters to contaminate the per-item date-window average, or left the date restriction outside the average | The per-item average sees all web-sale lines for that item in the date window and no manufacturer/threshold restriction; the final sum sees the qualifying manufacturer-350 lines |
| **Q94** | order eligibility vs report population | Applied ship-date/state/site filters while determining different-warehouse and no-return eligibility | Eligibility aggregates see every line/return for an order; report aggregates see only eligible order lines satisfying the report filters |
| **Q95** | order eligibility vs report population | Put ship-date/state/site filters inside `qualifying_orders`, reducing the answer from 68 orders to 6 | Multi-warehouse and has-return computations see the whole order; report totals apply date/state/site restrictions afterward |

These eight queries are the cleanest success metric. Each has a documented
stage-order error that should be legible in an aggregate/window scope report.

## Secondary stress set

| Query | Scope family | Why include it | Caveat |
|---|---|---|---|
| **Q05** | independent event-time populations | Sales are filtered by sale date while returns are independently filtered by return date before channel/entity rollup | The query also stresses role-specific entity keys and full-preserving combination, so a failure is not automatically a scope failure |
| **Q23** | several independent temporal populations | Frequent items use 2000–2003; customer totals are all-time; the comparison maximum is 2000–2003; final catalog/web sales are February 2000 | Very complex and historically affected by subselect/tooling issues; use for qualitative agent traces more than a clean pass-rate attribution |
| **Q44** | explicitly filtered benchmark aggregate | The comparison average must be restricted to store 1 and missing sale-address key, while ranked item averages use store 1 generally | Rank direction and pairing can fail independently |
| **Q67** | window over rollup output | Rank must consume every rollup row and partition by resulting category value, including one shared null-category partition | Historical framework regressions around rollup identity make this a planner-regression test as well as an agent-scope test |
| **Q78** | channel-local anti-return scopes | Each channel must determine never-returned lines using its own order/ticket and item match before channel aggregation and combination | Most historical misses were key/measure mapping rather than filter placement |

Do not combine primary and secondary results into one headline lift number.
Secondary failures need trace review to establish whether the scope diagnostic
was relevant.

## Development/holdout split

Several primary queries are semantic twins. Use that deliberately:

| Family | Development query | Holdout query |
|---|---|---|
| Population benchmark followed by output filtering | Q30 | Q81 |
| Boundary rows required by `lag`/`lead` | Q47 | Q57 |
| Whole-order eligibility followed by line filtering | Q94 | Q95 |

Develop formatting and agent guidance against the development member only.
Do not inspect or tune against the holdout trajectory until the implementation
and guidance are frozen. Improvement on the holdout is the strongest evidence
of systemic lift.

Q14 and Q92 are singleton families and should remain in the final evaluation,
not in the formatting-development loop.

## Expected scope assertions

The feature itself should have deterministic tests independent of agent score.
At minimum, assert these semantic boundaries:

### Q30/Q81

- Inner total input: relevant return year and non-null return-location state.
- Benchmark input grain: returning customer plus return-location state.
- Benchmark grouping: return-location state.
- Current-home-state GA filter: output-side of the benchmark.

### Q47/Q57

- Monthly aggregate input includes required prior/following boundary months.
- `lag` and `lead` partition by the full business grouping.
- Window order is chronological year then month.
- `year = 1999` and relative-deviation qualification are output-side of the
  navigation windows.

### Q14

- Overall average input is all three channels in 1999–2001.
- Leaf aggregate input is qualifying tuples in November 2001.
- Above-average qualification occurs before rollup.
- Rollup consumes the qualified leaf rowset, not raw November sales.

### Q92

- Per-item average input contains only the date-window restriction.
- Manufacturer and `discount > 1.3 * average` conditions restrict final
  contributing lines, not the average population.

### Q94/Q95

- Eligibility inputs have no ship-date, state, or site-company restriction.
- Eligibility grain is order number.
- Final count/sums consume only eligible orders and then apply report filters.

## Experiment design

Run at least three conditions:

1. **Baseline:** current agent tools, no scope block.
2. **Scope only:** scope block returned automatically, no new explanatory
   guidance beyond its factual labels.
3. **Scope + minimal guidance:** tell the agent to compare each aggregate/window
   input scope with the question before accepting a clean run.

Use fresh trajectories and the same model, prompt, model version, token budget,
and tool-output limits. Run multiple seeds if supported.

Track:

- exact-result pass rate on the eight-query primary set;
- holdout pass rate on Q81, Q57, and Q95;
- number of query rewrites after the first clean execution;
- whether the agent explicitly notices a scope mismatch in its reasoning;
- token and wall-clock change;
- false interventions, where a correct scope is rewritten into an incorrect
  one after reading the diagnostic.

For failed runs, classify the final miss as:

- scope still wrong and visible in the diagnostic;
- scope wrong but not represented accurately by the diagnostic;
- scope correct, different query defect;
- framework generated behavior disagrees with the reported scope.

The last category is especially important: it turns the feature into a planner
correctness oracle rather than only an agent aid.

## Recommended first smoke test

Before running the full set, use four queries spanning distinct boundaries:

1. Q30 — nested benchmark plus outer dimension filter.
2. Q47 — navigation window plus boundary months.
3. Q14 — filter leaves before rollup.
4. Q95 — whole-entity eligibility before report-line filtering.

Then freeze the format and run Q81/Q57/Q94 as the first generalization check.
