# Handoff: parent lookups over a self-referencing key (2026-10-16)

Found modelling the osquery process tree (trilogy-public-models
`duckdb/osquery`). The `recurse_edge` rollups fixed in #743 now plan; a plain
one-level parent lookup still returns the wrong rows in both spellings.

## Open: a child's parent attribute adds or drops rows

Two processes, `code` (pid 1, no parent) and `node` (pid 2, parent 1). Expected
for "each process with its parent's name": `(1, code, NULL), (2, node, code)`.

Self-import (`selfp.preql`):

```trilogy
self import as parent;
key pid int;
property pid.name string;

datasource procs (pid: pid, parent_pid: ~?parent.pid, name: name)
grain (pid)
query '''
select * from (values (1, null, 'code'), (2, 1, 'node')) as t(pid, parent_pid, name)
''';
```

`import selfp; select pid, name, parent.name order by pid;` returns
`(1, code, NULL), (2, node, code), (NULL, NULL, node)`: the parent side is
FULL-joined, so a parent member with no child of its own (`node`) becomes a row
with no `pid`.

Merge across a second file (`p2.preql` with `property pid.parent_pid int?;`
bound directly, and `t2.preql`):

```trilogy
import p2 as process;
import p2 as parent;
merge process.parent_pid into parent.pid;
```

`import t2; select process.pid, process.name, parent.name order by process.pid;`
returns only `(2, node, code)`: the root, whose `parent_pid` is null, is dropped.

In both, the requested grain is `pid` and `parent.name` is a nullable lookup
through `parent_pid`, so the answer is one row per `pid` with a LEFT join to the
parent. The osquery model's `process_tree.preql` uses the merge spelling, so
"processes with their parent" there silently omits root processes.
