# Handoff: `recurse_edge` planner bugs (2026-10-15)

Found while modelling a process tree for the osquery public model
(trilogy-public-models `duckdb/osquery/process_tree.preql`): each process rolls
up to the top of its launch tree with `recurse_edge`, and a second import of
the same model is merged onto the root to read its attributes. Two query
shapes fail to plan; a third returns inconsistent rows. The common shape the
model's example uses (`select app.name, total_resident_mb`) works.

## Repro model

Two files in one directory. A single datasource carries the key, the edge and
a measure, which is what the osquery process table looks like.

`proc.preql`:

```trilogy
key pid int;
property pid.app_parent int?;
property pid.name string;
property pid.mem int;
auto app_pid <- recurse_edge(pid, app_parent);

datasource procs (pid: pid, app_parent: app_parent, name: name, mem: mem)
grain (pid)
query '''
select * from (values (1, null, 'code', 10), (2, 1, 'node', 5), (3, 2, 'py', 1),
    (4, null, 'fx', 7), (5, 4, 'fxc', 3)) as t(pid, app_parent, name, mem)
''';
```

`tree.preql`:

```trilogy
import proc as process;
import proc as app;
merge process.app_pid into app.pid;
auto total_mem <- sum(process.mem);
```

Expected rollup: app 1 (`code`) = 16 over 3 processes, app 4 (`fx`) = 10 over 2.

| Query | Result |
|---|---|
| `import proc; select app_pid, count(pid) -> n;` | OK: `(1, 3), (4, 2)` |
| `import proc; select app_pid, sum(mem) -> m;` | **Bug A** |
| `import tree; select app.pid, total_mem;` | OK: `(1, 16), (4, 10)` |
| `import tree; select app.name, total_mem;` | Rows for all 5 names (**C**) |
| `import tree; select app.pid, app.name, total_mem;` | **Bug B** |

## Open: A. Aggregating a measure by the recursive concept fails to plan

`select app_pid, sum(mem)` raises
`SyntaxError: Missing source map entry for local.app_pid with pseudonyms set(), have map: {'local.pid': {BuildDatasource(name='procs', ...`.
`count(pid)` by the same concept plans, so the failure is specific to an
aggregate input other than the recursion's own key: the node that sources
`mem` is planned without the recursive CTE's `app_pid` in its source map. The
agent-facing form of this is "memory per `application_pid`", which an agent
reaching for the concept directly (rather than `process_tree`) will write.

## Open: B. A second attribute of the merged root plus a metric loses `_terminal`

`select app.pid, app.name, total_mem` raises
`UndefinedConceptException: Concept '_terminal' not found in environment.`
Either attribute alone with the metric plans. `_terminal` is
`RECURSIVE_GATING_CONCEPT` (`trilogy/constants.py`): the recursive CTE's
internal stop flag, which a downstream node is resolving as an environment
concept. Same failure on the osquery model
(`select app.pid, app.name, total_resident_mb`).

## Open: C. Grouping by a merged root attribute returns non-root rows

`select app.name, total_mem` returns `code 16, fx 10` plus `node`, `py`,
`fxc` with a null total (`count` gives 0), while `select app.pid, total_mem`
returns only the two roots. The `merge ... into app.pid` is FULL, so every
`app` member is in the dimension; whether a property of the merged side should
widen the result past the key is the question. Agents rank with
`order by ... desc nulls last limit N`, so it is invisible in practice, but the
two spellings of "per application" disagree.

## Open: D. A model cannot import itself

The natural spelling of the parent lookup is, inside `process.preql`:

```trilogy
import process as parent;
datasource processes (..., parent_pid: ~?parent.pid, ...)
```

It parses, but the import-cycle guard stops loading the self-import after the
key: the `parent` namespace holds only `parent.pid`, so `parent.name` is
undefined. The osquery model works around it with a separate
`process_tree.preql` that imports `process` three times (base, `parent`,
`app`) and merges the edges. Supporting the self-import needs the cycle guard
to complete the namespace from the file's own (already parsed) concepts
rather than stop.
