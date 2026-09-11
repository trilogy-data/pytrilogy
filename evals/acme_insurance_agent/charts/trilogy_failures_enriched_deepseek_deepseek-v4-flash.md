# Trilogy failure analysis — 20260911-001859

- Run `20260911-001859_enriched_deepseek_deepseek-v4-flash` | `deepseek/deepseek-v4-flash` | sf=1
- `trilogy` calls: 89 | failed: 5 (6%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `syntax-parse` | 3 | 60% |
| `disabled-tool` | 2 | 40% |

## Detail

### `syntax-parse`

- `trilogy file write probe_a.preql --run-and-delete`

  ```text
  refused to write 'probe_a.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [202]: Missing closing semicolon? Statements must be terminated with a semicolon `;`.
  Location:
  ...licy_holder_id) as n_holders
   ???
  ```
- `trilogy file write probe_agents.preql --run-and-delete`

  ```text
  refused to write 'probe_agents.preql': not syntactically valid Trilogy.

  Parse error:
   --> 7:1
    |
  7 | by * as all_rows;
    | ^---
    |
    = expected metadata, limit, order_by, where, having, select_grouping, or JOIN_TYPE
  Location:
  ...policy.agent_id) as n_agents
   ??? by * as all_rows;
  ```
- `trilogy file write probe_4213852742.preql --run-and-delete`

  ```text
  refused to write 'probe_4213852742.preql': not syntactically valid Trilogy.

  Parse error:
  Syntax [213]: A `by <grain>` clause must follow an aggregate, but the expression before it has none. If the `by` sits inside an aggregate's parentheses (`max(x by *)`), move it outside the call: `max(x) by *`. To take each distinct value once per grain, wrap it in `group(...)` - e.g. `group(item.current_price) by item.id, item.category`. For a reduction, use an aggregate: `sum(x) by ...`, `avg(x) by ...`, `max(x) by ...`.
  Location:
  ...tal_premium as total_premium
   ??? by *;
  ```

### `disabled-tool`

- `trilogy file read raw/policy.preql`

  ```text
  trilogy file read is disabled for this task. Use `explore <file.preql>` to inspect a model's queryable concepts (it chains in imported dimensions too) instead of reading raw file contents. `file list` and `file write` are still available.
  ```
- `trilogy database list`

  ```text
  trilogy database introspection is disabled for this task. The semantic model is already built under root/ — use `explore <file.preql>` to see queryable concepts (it chains in imported dimensions too). Do not list raw database tables.
  ```
