"""The FINAL assembly of argv queries on any model: the cover after each
contributor pass, every `plan_source` the assembly runs with its extent scope,
and each FINAL fold that changes the parents. `--caps` adds the group graph's
capability/output sets and the `[v4] built grp:` lines, `--sql` the SQL.

The model: `--src <file.py>:<ATTR>` (a module-level model string, repo-relative;
the file's directory is importable, so a test module's helpers resolve) or
`--matrix` (the nullability matrix's `matrix_addr_partial` fixture). `KS_REPO`
points it at another tree for a baseline: run with `PYTHONPATH=<worktree>`.

The eighth session's tracer: the scope of a domain's FINAL re-source
(`select customer.sk, product.sk, customer.current_address.state`) and the
cover moving `brand` onto the BASIC that reads the product domain."""

import importlib.util
import logging
import os
import sys
import tempfile
from pathlib import Path

REPO = Path(os.environ.get("KS_REPO", str(Path(__file__).resolve().parents[3])))
sys.path.insert(0, str(REPO))

from trilogy import Dialects, Environment
from trilogy.core.processing.v4_helper import group_graph as gg
from trilogy.core.processing.v4_helper import strategy_builder as sb


def _short(s: str) -> str:
    return s.replace("local.", "")


def _desc(p) -> str:
    outs = ",".join(sorted(_short(c.address) for c in p.output_concepts))
    spans = sorted(_short(s) for s in (p.region_spans or []))
    return f"{type(p).__name__}[{outs}] spans={spans}"


def _per_group(tag, per_group) -> None:
    print(f"  {tag}:")
    for gid, concepts in per_group.items():
        print(f"     {gid}: {sorted(_short(c.address) for c in concepts)}")


def _trace_per_group_pass(name: str) -> None:
    orig = getattr(sb, name)

    def traced(*a, **k):
        r = orig(*a, **k)
        per_group = next(
            (
                x
                for x in a
                if isinstance(x, dict) and all(isinstance(v, list) for v in x.values())
            ),
            None,
        )
        if per_group is not None:
            _per_group(name, per_group)
        return r

    setattr(sb, name, traced)


def _trace_fold(name: str) -> None:
    orig = getattr(sb, name)

    def traced(parents, *a, **k):
        before = [_desc(p) for p in parents]
        r = orig(parents, *a, **k)
        after = [_desc(p) for p in (r if r is not None else parents)]
        if before != after:
            print(f"  {name}:")
            for b in before:
                print("     -", b)
            for x in after:
                print("     +", x)
        return r

    setattr(sb, name, traced)


def install() -> None:
    for name in (
        "_cover_groups_for_mandatory",
        "_add_relation_axis_contributors",
        "_add_partial_completion_contributors",
        "_fold_descendant_contributors",
        "_promote_final_aliases_to_grouping_contributors",
        "_add_region_domain_contributors",
    ):
        _trace_per_group_pass(name)
    orig_plan = sb.plan_source

    def plan(request, *a, **k):
        scope = request.environment.span_scope
        r = orig_plan(request, *a, **k)
        carried = {
            _short(key): sorted(_short(s) for s in spans)
            for key, spans in scope.extent_free_carried.items()
        }
        print(
            f"  PLAN_SOURCE outs={sorted(_short(c.address) for c in request.outputs)}"
            f" extent_free={sorted(_short(s) for s in scope.extent_free)}"
            f" carried={carried} -> {_desc(r) if r is not None else None}"
        )
        return r

    sb.plan_source = plan
    for name in (
        "_fold_constant_parents",
        "_satisfy_parent_projection_contract",
        "_fold_passthrough_parents",
        "_fold_covered_contributors",
        "_bridge_unpaired_parents",
    ):
        _trace_fold(name)


class _BuiltFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "built grp" in record.getMessage()


def install_caps() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(_BuiltFilter())
    handler.setFormatter(logging.Formatter("%(message)s"))
    log = logging.getLogger("trilogy")
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    orig_compute = gg._compute_concept_sets
    orig_for = gg.GroupIOPlan.for_groups
    plans: list = []

    def for_groups(*a, **k):
        plan = orig_for(*a, **k)
        plans.append(plan)
        return plan

    gg.GroupIOPlan.for_groups = for_groups

    def compute(*a, **k):
        result = orig_compute(*a, **k)
        plan = plans[-1]
        for gid in sorted(plan.capability):
            print(f"  CAP {gid}: {sorted(_short(x) for x in plan.capability[gid])}")
            print(
                f"  OUT {gid}: {sorted(_short(x) for x in plan.outputs.get(gid, ()))}"
            )
        return result

    gg._compute_concept_sets = compute


def _module(file: str):
    spec = importlib.util.spec_from_file_location("m", REPO / file)
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str((REPO / file).parent))
    spec.loader.exec_module(module)
    return module


def _executor():
    if "--matrix" in sys.argv:
        m = _module("tests/engine/test_duckdb_nullability_matrix.py")
        return m._build(Path(tempfile.mkdtemp()), "~")
    file, attr = sys.argv[sys.argv.index("--src") + 1].split(":")
    env = Environment()
    env.parse(getattr(_module(file), attr))
    return Dialects.DUCK_DB.default_executor(environment=env)


def _rows(ex, q: str):
    rows = [tuple(r) for r in ex.execute_text(q + ";")[-1].fetchall()]
    return sorted(rows, key=lambda r: tuple((v is None, str(v)) for v in r))


if __name__ == "__main__":
    if "--caps" in sys.argv:
        install_caps()
    install()
    ex = _executor()
    args = iter(sys.argv[1:])
    for x in args:
        if x == "--src":
            next(args)
        elif not x.startswith("--"):
            print(f"\n=== {x}")
            print("  rows:", _rows(ex, x))
            if "--sql" in sys.argv:
                print(ex.generate_sql(x + ";")[-1])
