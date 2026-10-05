# Explore output: compact-by-default modes

Status: changes 0-2 shipped (2be34102b, json v3 in `trilogy/scripts/explore.py`):
`namespaced` entries outline by default with a `--ns <alias>` drill-down, and
`TRILOGY_EXPLORE_COMPACT=0` pins v2. Change 3 below remains open.

Why it matters: `explore` output was the largest token class in the Trilogy
agent legs (40-45% of rebilled prompt tokens), dominated by retransmission of
shared dimensions — every fact explore re-rendered customer/item/date/store in
full. The outline shrank a first explore ~46% and lets entry-level dedup fire
across files, but a dimension drilled down under one fact still renders in full
when drilled down again under another, because its alias and role map differ
so the payload is never byte-identical.

## 3. Cross-file schema referencing via the session store

Extend `explore_seen` from byte-identity to schema identity: hash the rendered
member schema *body* separately from its role/prefix bindings. The first fact
that renders `customer` emits it in full; later facts (or later `--ns` calls)
emit the role map plus `"schema": {"same_as": "customer (shown exploring
raw/store_sales.preql)"}`. Role maps stay per-fact because they genuinely
differ; only the member list is shared. `--reshow` reprints, as today.

This is the structural fix for the retransmission problem and also covers
same-file re-explores. It is safe by the same construction as today's dedup:
any model change renders differently, hashes differently, prints in full.

Validation: dedup-store tests for schema-identity referencing beside the
existing ones, then a 2-leg 99q A/B (enriched + ingest) with compact on in
both arms and change 3 on vs off. Primary metric: cache-adjusted tokens and
explore chars per query; guard: pass rate and the `Undefined concept` error
count (agents guessing member names they were not shown).
