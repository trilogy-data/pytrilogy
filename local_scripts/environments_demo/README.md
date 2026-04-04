# Deployment environments demo (local DuckDB)

End-to-end walkthrough of `trilogy env`: build changed code into a namespaced
environment next to production, verify it, and promote it with an atomic
rename cutover. All against a local on-disk DuckDB (`warehouse.duckdb`).

Every command below runs from this directory.

## 1. Build production

```bash
trilogy refresh .
```

`orders_enriched` is built from the root `raw_orders`. Check it:

```bash
trilogy run "import model; select product, sum(amount) -> total;"
```

## 2. Build a change into an environment

Edit `model.preql` — for example, add a derived column and carry it on the
managed table:

```
auto amount_with_tax <- amount * 2;
```

(and add `amount_with_tax: amount_with_tax,` to `orders_enriched`'s columns).
Then:

```bash
trilogy refresh . --environment dev
```

Every managed (non-root) table is prefixed: the build writes
`dev_orders_enriched`, production's `orders_enriched` is untouched, and both
read the same root `raw_orders`. Staleness is evaluated against the
environment's own tables, so the first refresh backpopulates everything.

You can also set the environment once instead of flagging each command:

```bash
trilogy env activate dev
trilogy refresh .        # builds into dev
trilogy env deactivate
```

Inspect what exists:

```bash
trilogy env list         # environments + tracked assets
trilogy state . --environment dev   # staleness of the dev build
```

## 3. Verify, then cut over

```bash
trilogy env publish dev --dry-run   # show the rename plan
trilogy env publish dev
```

Publish is a two-phase rename: production tables move to `*__pub_backup`,
environment tables move into their place, backups are dropped on success.
Any failure rolls the whole cutover back. Use `--keep-backups` to retain the
previous production tables.

## 4. Clean up

```bash
trilogy env delete dev   # drops any remaining dev_* tables, deregisters
```

## Notes

- The environment registry lives under `[environments] home` in
  `trilogy.toml` (here `.trilogy_envs/`; defaults to `~/.trilogy`).
- `--environment <label>` auto-registers unknown labels, so a cloud
  orchestrator can spin up `deploy_1234` without a create step.
- Root datasources are never rewritten — an environment shares its sources
  of truth with production. Tables and local file outputs (parquet/csv) both
  cut over; remote (gs://, s3://) outputs are rejected at publish time for
  now — object stores have no atomic rename.
- Environment names must be identifier-safe (letters, digits, underscores):
  they become physical table-name prefixes.
