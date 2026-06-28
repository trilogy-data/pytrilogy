# Roadmap: accept `as` as a connector for `rowset` definitions (`rowset name as select …`)

**Status:** OPEN — proposed grammar enhancement (support it), not just a better error.
**Surfaced by:** TPC-DS q05 enriched eval — the agent wrote `rowset base as select …` and got a
dead-end parse error.
**Severity:** Medium (agent-efficiency). This conflation is extremely common because the sibling
`with … as` form *does* use `as`.

## Symptom

```trilogy
rowset base as
select all.channel, sum(all.ext_sales_price) as v;
```
→
```
Parse error: --> 4:1
4 | rowset base as
  | ^---
  = expected EOI, block, or show_statement
```

The agent's own narration said "use a `with` rowset for the base data," then typed `rowset` + `as`
(mixing the two rowset-definition forms). Today the only valid forms are:

- `rowset base <- select …`  (rowset keyword → `<-`)
- `with base as select …`    (with keyword → `as`)

## Proposed direction — support `as` for `rowset` too

Rather than only improve the error, **accept `as` as a synonym for `<-` in `rowset`
definitions**, so `rowset <name> as <select>` parses identically to `rowset <name> <- <select>`.
Rationale:

- `with <name> as …` already establishes `as` as a rowset-binding connector, so agents (and humans)
  naturally write `rowset <name> as …`. Supporting it removes a whole class of slip instead of
  teaching agents to avoid it.
- It's purely additive — `<-` keeps working; this just adds an accepted alternative.

Grammar change touches **both backends** (`trilogy.lark` + `trilogy.pest`) — add an optional `as`
alternative to the `<-` in the rowset-definition rule — and any round-trip/serializer that emits the
connector. Keep the two grammars in sync (see `reference_parser_backends`); pest is the default and
needs a `maturin develop --release` rebuild.

## Fallback (if not supported)

If `as` is deliberately *not* accepted on `rowset`, then at minimum emit a targeted error in
`trilogy/parsing/v2/errors.py`: *"a `rowset` is defined with `<-`: `rowset <name> <- select …` (or
use `with <name> as select …`)."* But the user's call here is to support `as` — make it Just Work.

## Repro

```python
import sys; sys.path.insert(0, 'evals')
from common import scoring
from pathlib import Path
ws = Path('evals/tpcds_agent/results/20260626-125555/workspace')
eng = scoring.make_scoring_engine(ws / 'tpcds.duckdb', ws, 'tpcds')
# currently a parse error; should parse identically to `rowset base <- …`
eng.generate_sql('import raw.all_sales as s;\n'
                 'rowset base as select s.channel as ch, sum(s.ext_sales_price) as v;\n'
                 'select base.ch, base.v limit 3;')
```

Note: the separate `def`-without-param / definition-connector error-quality items are tracked in
`definition_connector_mixup_cryptic_parse_error.md` (an agent is already on those); this file is
scoped to the *support `rowset … as`* enhancement only.
