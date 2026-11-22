# Trilogy CLI vs dbt

Draft positioning doc. Trilogy claims come from the CLI and this repo; dbt claims
are as of mid-2026 and move fast (Fusion, MetricFlow licensing, microbatch) —
re-verify against dbt's docs before publishing anything derived from this.

## The core difference

- **dbt** is a build tool for SQL: Jinja-templated `SELECT` files become tables
  and views. You write every join and own every grain; dbt handles ordering,
  materialization, tests, and docs.
- **Trilogy** is a modeling language with a build tool on top: you declare
  concepts (keys, properties, metrics) and which datasources hold them.
  Transforms and queries are written against concepts, not tables, so the
  physical layer can be split, merged, renamed, or pre-aggregated without
  touching anything that consumes it — the planner picks the cheapest
  datasources that answer each query, including pre-aggregates. The CLI builds
  and maintains the datasources the model declares.

In dbt, every consumer names a model (`ref('fct_orders')`), so the table layout
*is* the interface and restructuring it means editing everything downstream.

## Feature by feature

| Area | dbt | Trilogy CLI | Edge |
|---|---|---|---|
| Authoring | SQL + Jinja; joins and grain by hand; very flexible | Typed concepts, no joins, reusable `def` functions, declared grain per datasource | Trilogy on reuse and refactor safety; dbt on familiarity and raw flexibility |
| Learning curve | Any SQL user; huge hiring pool | New (SQL-like) language; small community | dbt |
| Semantic layer | MetricFlow, a separate YAML layer beside the models | The model *is* the semantic layer; no YAML | Trilogy |
| Ad-hoc querying | `dbt show` / compile; not built for exploration | `run "select ..."`, `explore`, inline charts, `render` (markdown reports to HTML/PNG), `serve` (Studio UI) | Trilogy |
| Build orchestration | `ref()` DAG, selectors, `state:modified+`, `--defer`, `source_status:fresher+` | `refresh` rebuilds only watermark-stale assets; directory runs ordered by import/persist; `--dry-run`, `-i` approval, `[dependencies]` with `when = failed` | Even: dbt selectors more mature; Trilogy staleness-driven by default |
| Incremental / partitions | Mature: merge, append, insert_overwrite, microbatch; adapter-specific | `incremental by`, `partition by`, `refresh --partition` (one slice by concept), per-partition state + `state-merge` for parallel workers, BigQuery native partition swap | dbt on maturity and breadth; Trilogy on concept-addressed slice refresh |
| Snapshots (SCD2) | Built in | Not supported | dbt |
| Seeds / ingestion | `dbt seed` from CSV | `ingest` from CSV/Parquet/URLs/warehouse tables, generating models with FK inference | Different jobs; `ingest` does more |
| Data tests | Generic/singular tests run *after* the build; large package ecosystem | `integration` type/grain validation; datasource `assert` with `prior()` runs *before* publish, so a bad build never replaces the old asset | Trilogy on pre-publish protection; dbt on breadth of off-the-shelf tests |
| Unit tests | Hand-written YAML fixture rows (1.8+) | `unit` synthesizes mock data from the declared model and runs without a warehouse; `--scale` | Trilogy on effort; dbt fixtures give exact control |
| Source freshness | `dbt source freshness` | `freshness by`; `state` probes watermarks read-only, emits JSON | Even |
| Environments / CI | Targets, schema-per-env, Cloud CI with deferral; blue/green by hand or package | `env create/diff/fingerprint/publish`: namespaced builds with rename cutover | Trilogy for built-in blue/green; dbt slim CI more mature |
| Docs / lineage | `dbt docs` site, Catalog/Explorer, column-level lineage (Cloud/Fusion) | `explore`, `plan --json` dependency graph, Studio via `serve`; no static docs site | dbt |
| Contracts / versioning | Contracts, model versions, access modifiers, Mesh cross-project refs | Parse-time typing; model fingerprints and `env diff`; no access modifiers | dbt for multi-team governance; Trilogy for type safety |
| Python | Python models on Snowflake, Databricks, BigQuery | Script datasources: any Arrow-returning program (Python or Rust) with filter/column pushdown, on any engine; full SDK | Different shapes; Trilogy's is engine-agnostic |
| AI / agents | dbt MCP server, Copilot (Cloud) | `serve --mcp`, `agent`, `agent-info`, `--format json` / `--agent`, LLM `validate ... matches` tests | Trilogy on depth; dbt on installed base |
| Warehouses | Dozens of adapters, many vendor-maintained | Core: BigQuery, DuckDB, Snowflake, SQLite; also Postgres, MySQL, ClickHouse; SQL Server and Presto experimental; no Databricks/Spark or Redshift | dbt, clearly |
| Hosted platform | dbt Cloud: IDE, scheduler, Explorer, semantic layer APIs | `trilogy cloud`: jobs, schedules, secrets, workspaces, sync; earlier stage | dbt |
| Ecosystem | Thousands of packages, Coalesce, large community and job market | Public model registry (`public fetch`), Discord | dbt, by a wide margin |
| Time to first result | `pip install`, a profile, a project | `public fetch` → `run` → `refresh` → `serve`, embedded DuckDB | Trilogy, slightly |

## Where Trilogy sits

**Ahead:**

1. Consumption is decoupled from tables: refactor, split, or pre-aggregate the
   physical layer and queries and downstream transforms don't change; aggregate
   resolution routes queries to the best datasource automatically. This is the
   structural difference. Semantic layers (MetricFlow, Cube, LookML) decouple
   *BI* from tables; Trilogy decouples the pipeline itself.
2. Data protection before publish: `assert ... prior()`, mock-data `unit` tests,
   rename-based `env publish`.
3. One tool from exploration to production: ad-hoc queries, charts, reports,
   Studio, MCP. dbt deliberately stops at the build.
4. Built for agents: JSON/agent modes, MCP, and a language that gives an LLM
   fewer ways to be wrong.
5. Staleness-driven refresh is the default, not a selector recipe.

**Behind (don't contest these):**

1. Ecosystem, community, hiring pool, vendor adapters — dbt's moat.
2. Warehouse coverage: no Databricks or Redshift blocks a large slice of the market.
3. Governance at scale: Mesh, contracts, versions, access, column lineage, docs site.
4. SCD2 snapshots and mature incremental strategies.
5. Production track record.

## Positioning

Not a dbt replacement — dbt wins most of the checklist. Trilogy is what dbt
doesn't do: transforms written against concepts rather than tables, so the
table layer can evolve without rewriting what consumes it, and builds that
refuse to publish bad data. Automatic joins are table stakes for a semantic
layer, not the pitch. Best fit: DuckDB/BigQuery/Snowflake
teams, small teams without dbt habits, and agent/LLM analytics. For existing
dbt shops, "Trilogy on top of your dbt tables" (`ingest` over dbt outputs) is an
easier adoption path than migration.

### Would SCD2 change this?

Parity snapshots alone would not: they remove an objection from "can this
replace dbt?" evaluations, but the gaps that decide those evaluations are
ecosystem, adapters, and governance.

What would matter is SCD2 as a *semantic* feature. dbt snapshots only produce
the history table; every point-in-time query still hand-writes
`fact.ts >= dim.valid_from and fact.ts < dim.valid_to`. If a Trilogy datasource
could declare its validity window and the planner resolved the as-of join
itself — "the customer's region when the order was placed" by default — that
would be something dbt (and MetricFlow) cannot do, and it would fit the
existing "consumption doesn't know the table layout" story.
