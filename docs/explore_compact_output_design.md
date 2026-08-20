# Explore output: compact-by-default modes

Status: IMPLEMENTED (2026-08-20) through change 2 below, with one deviation
from the original plan; change 3 (cross-file schema referencing) remains open.
Evidence from TPC-DS agent eval run `20260820-031800` (99q x
enriched/ingest/sql_schema, deepseek-v4-flash).

What landed (json v3, `trilogy/scripts/explore.py`):

- `namespaced` entries outline by default: roles/description/join/keys survive,
  member declarations collapse to `members_elided`, a one-time `outline_note`
  carries the drill-down instruction. Local namespaces always render full.
- `--ns <alias>` (repeatable) expands one entry; `--expand-imports`,
  `--expand-roles`, and `--regex` render full detail. `TRILOGY_EXPLORE_COMPACT=0`
  pins v2 (the eval A/B kill-switch). The AI one-shot prompt embed pins v2.
- Deviation from change 0: no per-group event split was needed. The outline
  payload fits under the broad cap, so the existing single-event stream and
  entry-level dedup work as-is once `_TRILOGY_EXPLORE_BROAD_CAP` rose to
  12,288 (it doubles as the dedup record limit, and the fattest curated
  fact's outline is ~9KB).
- Agent guidance updated in `trilogy/scripts/agent.py` and
  `agent_info_docs/cli.py` (outline-first, then `--ns`).

Measured on the enriched store_sales model: first explore 15,948 -> 8,677
chars (-46%); same-file repeat 15,948 -> 2,312 (-86%, dedup now fires);
cross-file catalog_store_returns 10,415 -> 3,757 (-64%, outline entries
dedup across files where the full schemas never could). The 99q A/B
(validation step 4) has not run yet.

## Problem

`explore` output is the single largest token class in the Trilogy agent legs:
40.6% of all rebilled prompt tokens on enriched, 44.6% on ingest (language docs
are second at ~31-33%). Median payload is 15,975 chars on enriched (p90 21k),
at 2.4 calls per query on enriched and 3.2 on ingest. Every char lands in the
conversation and is rebilled on each later turn.

Composition of a representative payload (`explore raw/store_sales.preql`,
enriched, 321 concepts, 15,948 chars):

| section | chars | share |
|---|---|---|
| `namespaced` (imported dim schemas + role maps) | 9,781 | 61% |
| root namespace (the fact's own concepts) | 1,929 | 12% |
| declaration text within lines | 4,065 | - |
| description comments within lines | 2,313 | - |

The ingest leg shows the same shape (catalog_store_returns: 6,846 of 10,415
chars in `namespaced`, 66%).

Three observations from the run:

1. **The dominant cost is retransmission of shared dimensions.** Every fact
   explore re-renders customer/item/date/store/demographics in full. The
   conformed-role collapse (json v2) dedupes roles *within* one payload, but
   across files the same dimension renders under different aliases and role
   maps, so it is never byte-identical and session dedup cannot fire.
2. **The existing defenses defeat each other.** The agent wrapper caps broad
   explores at 8,192 bytes (`_TRILOGY_EXPLORE_BROAD_CAP`), but explore emits
   its payload as ONE JSON event and `truncate_json_events` always keeps the
   first event whole even when it alone exceeds the cap
   (`agent_tools.py:90`), so the cap is a no-op for explore's main payload:
   16-21k objects sail through. The same cap rides down as
   `TRILOGY_EXPLORE_RECORD_LIMIT`, and `explore_seen` refuses to record any
   payload above it, so the oversized payloads are also never marked seen and
   session dedup cannot fire on the calls that matter. Observed: 8 of 234
   enriched payloads contained an `already_shown` stub; two exact
   same-command repeats on ingest re-emitted 13k chars in full.
3. **Agents take the firehose**: 93% of enriched explore calls are bare, 7%
   use `--regex`. The first explore cannot be filtered (the agent does not
   know names yet), so the default render is what matters, not the flags.

The SQL leg is the control: it discovers through 824-char probes and one 13.5k
schema.md, and spends 2.4x fewer adjusted tokens per query overall.

## Proposal

Three changes, ordered by leverage. All ride the existing payload-version
machinery (`_LATEST_RENDER_VERSION`); the new shape is json v3 with a
`version` field consumers can branch on, and an env kill-switch
(`TRILOGY_EXPLORE_COMPACT=0`) mirrors the effective-nulls rollout so an eval
A/B can flip it per leg.

### 0. Emit the payload as per-group events (prerequisite)

Split the single concepts object into one JSON event per namespace group
(root first, then one per conformed group), plus a small header event carrying
`type`/`version`/`count`. This alone re-arms both existing mechanisms with no
rendering change: `truncate_json_events` can enforce the broad cap at group
boundaries (dims get dropped before the root namespace, with the
`output_truncated` narrow-the-call note), and `explore_seen` can record and
dedup at group granularity, which is exactly the granularity at which content
repeats across files. Changes 1-3 then operate on the same per-group unit.

### 1. Outline the `namespaced` section by default (json v3)

Root namespace stays exactly as today: it is the query's subject, only ~12% of
the payload, and its descriptions (metric guidance, derivation warnings) are
the highest-value enrichment content.

Each conformed `namespaced` group collapses to a header-only entry: combined
alias list, key concept + type, join nullability, per-role descriptions, a
property/metric count, and the drill-down hint. Everything that describes the
*relationship* survives; only the member schema is deferred:

```json
"customer, return_customer": {
  "key": "customer.sk int?",
  "join": "nullable",
  "roles": {"customer": {}, "return_customer": {"description": "may differ from the purchasing customer"}},
  "members": 24,
  "drilldown": "explore raw/store_sales.preql --ns customer"
}
```

Estimated payload: root (~2k) + outline (~1.5k) + wrapper = **~4-5k chars vs
16k today (-70%)**. The store_sales fact has 8-10 conformed groups; a typical
question needs member detail for 1-3 of them.

### 2. `--ns <alias>` drill-down

Renders the full schema for one (or more, repeatable) namespaced group, keyed
by the alias shown in the outline. `--regex` already covers address-shaped
filtering; `--ns` exists so the drill-down is copy-pasteable from the outline
without the agent inventing a regex. A drill-down payload is ~1-2k chars.

`--show full` (or the v2 behavior under the kill-switch) remains for humans
and model authoring.

### 3. Cross-file schema referencing via the session store

Extend `explore_seen` from byte-identity to schema identity: hash the rendered
member schema *body* separately from its role/prefix bindings. The first fact
that renders `customer` emits it in full; later facts (or later `--ns` calls)
emit the role map plus `"schema": {"same_as": "customer (shown exploring
raw/store_sales.preql)"}`. Role maps stay per-fact because they genuinely
differ; only the member list is shared. `--reshow` reprints, as today.

This is the structural fix for the retransmission problem and also covers
same-file re-explores. It is safe by the same construction as today's dedup:
any model change renders differently, hashes differently, prints in full.

### Guidance change (with 1)

The agent-info query guide's discovery paragraph changes from "explore the
fact file" to the two-step protocol: outline first, then `--ns` only the
groups the question touches. One sentence plus one example; the outline's
embedded `drilldown` field does most of the teaching in-band.

## Expected effect

Per enriched query today: ~29k chars of explore output arriving, ~86k raw
prompt tokens after rebilling. Outline default plus 2 drill-downs is ~9k chars
arriving; with the same turn structure that is roughly **-25k raw / -3-4k
cache-adjusted per query**, and more on ingest (3.2 explores/q, 66%
namespaced share). Secondary effect: smaller contexts make every excess turn
cheaper, which compounds on the churn-heavy queries.

## Risks

- **Pass-rate regression from hidden descriptions.** The enrichment value
  lives in descriptions and nullability rendering (the effective-nulls A/B
  was a win on exactly this channel). Mitigations: root descriptions never
  collapse; join nullability and role descriptions stay in the outline; the
  outline names the drill-down explicitly. The A/B below is the real gate.
- **Extra turns.** A drill-down the old payload avoided is a new turn. The
  arithmetic still wins (a turn costs ~2-4k adj at these context sizes vs 12k
  chars saved per avoided full render), but lazy agents that skip the
  drill-down and guess member names would show up as `Undefined concept`
  retries. Watch that error class in the A/B.
- **Consumer breakage.** Anything parsing explore JSON pins v2 semantics; the
  version bump plus `render_version_override` covers tests and pinned
  callers.

## Validation plan

1. Unit: new render version tests beside the existing v1/v2 shape tests;
   dedup-store tests for schema-identity referencing.
2. Land change 0 first and re-measure a single leg: per-group events plus the
   re-armed cap and dedup may already claim a third of the win, which resizes
   how much changes 1-3 need to carry.
3. `repeat_query.py --repeats 10` on a deliberation-heavy sink (q49 ingest:
   3 same-file explores, 5 syntax lookups) and a clean floor query (q42) in
   both legs, compact on vs off: pass_rate guard plus token delta.
4. One 2-leg 99q A/B (enriched+ingest, `--concurrency 2`) with per-leg
   `--output-dir` arms. Primary metric: cache-adjusted tokens per query and
   explore chars per query; guard: pass rate and the `Undefined concept`
   error count. The expected ~30% token effect is well above the run-to-run
   token noise on these legs; pass rate at +-13% noise is a guard only.
