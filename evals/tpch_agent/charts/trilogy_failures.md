# Trilogy failure analysis — 20260526-015341

- Run `20260526-015341` | `openrouter/deepseek/deepseek-v4-flash` | sf=0.01
- `trilogy` calls: 58 | failed: 18 (31%)

## Categories

| Category | Count | Share |
|---|---:|---:|
| `other` | 18 | 100% |

## Detail

### `other`

- `trilogy run --import raw/lineitem:lineitem where lineitem.shipdate <= '1998-09-02'::date select lineitem.returnflag, lineitem…`
  - name 'g' is not defined
- `trilogy run query01.preql`
  - name 'g' is not defined
- `trilogy run query02.preql`
  - name 'g' is not defined
- `trilogy --debug run query02.preql`
  - name 'g' is not defined
- `trilogy run raw/part.preql`
  - name 'g' is not defined
- `trilogy explore raw/part.preql`
  - name 'g' is not defined
- `trilogy run raw/orders.preql`
  - name 'g' is not defined
- `trilogy run raw/orders.preql duckdb`
  - name 'g' is not defined
- `trilogy database list`
  - name 'g' is not defined
- `trilogy run query04.preql`
  - name 'g' is not defined
- `trilogy run query04.preql duck_db`
  - name 'g' is not defined
- `trilogy --debug run query04.preql`
  - name 'g' is not defined
- `trilogy run --config trilogy.toml query04.preql`
  - name 'g' is not defined
- `trilogy agent-info`
  - name 'g' is not defined
- `trilogy run query04.preql duck_db db_location=tpch.duckdb`
  - name 'g' is not defined
- `trilogy run query04.preql duck_db --config trilogy.toml`
  - name 'g' is not defined
- `trilogy run query04.preql`
  - name 'g' is not defined
- `trilogy unit query04.preql`
  - name 'g' is not defined
