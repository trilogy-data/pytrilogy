"""A/B the `_fd_facts` hoist in `build_fd_closure` against the CURRENT tree.

The hoist is a pure representation change — it must be byte-identical on both
corpora, so ANY drift is a bug in the refactor, not an improvement. This holds
the pre-hoist algorithm inline (it reads every attribute off the BuildConcepts
inside the fixpoint) and renders both corpora twice in ONE process: reference
leg first, tree leg second, then diffs the two.

Usage: python local_scripts/v4_fd_closure_ab.py
"""

from __future__ import annotations

import sys
from collections.abc import Iterable
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment
from trilogy.core.processing.v4_helper import functional_dependency as fd
from trilogy.dialect.config import DuckDBConfig

ROOT = Path(__file__).parent.parent / "tests" / "modeling"
CORPORA = {"tpcds": ROOT / "tpc_ds_duckdb", "tpch": ROOT / "tpc_h"}


def reference_closure(
    environment: BuildEnvironment,
    determinants: Iterable[str],
    *,
    include_empty_grain: bool = True,
) -> frozenset[str]:
    """The pre-s53 implementation, verbatim."""
    closure = set(determinants)
    changed = True
    while changed:
        changed = False
        for address in list(closure):
            concept = environment.concepts.get(address)
            if concept is None:
                continue
            for equivalent in concept.equivalent_addresses:
                if equivalent not in closure:
                    closure.add(equivalent)
                    changed = True
        for address, concept in environment.concepts.items():
            if address in closure:
                continue
            if concept.address in closure or bool(
                concept.equivalent_addresses & closure
            ):
                closure.add(address)
                changed = True
        for concept in fd._build_fd_concepts(environment):
            if concept.address in closure:
                continue
            grain = concept.grain.components if concept.grain else frozenset()
            if not grain:
                if include_empty_grain:
                    closure.add(concept.address)
                    changed = True
                continue
            keys = frozenset(concept.keys or set())
            if grain <= closure or (bool(keys) and keys <= closure):
                closure.add(concept.address)
                changed = True
    return frozenset(closure)


def sweep() -> dict[str, str]:
    out: dict[str, str] = {}
    for label, working in CORPORA.items():
        env = Environment(working_path=working)
        engine: Executor = Dialects.DUCK_DB.default_executor(
            environment=env, conf=DuckDBConfig()
        )
        if (working / "memory").exists():
            engine.execute_raw_sql(f"IMPORT DATABASE '{working / 'memory'}';")
        for f in sorted(working.glob("query*.preql")):
            engine.environment = Environment(working_path=working)
            key = f"{label}:{f.stem}"
            try:
                out[key] = "\n---\n".join(engine.generate_sql(f.read_text()))
            except Exception as e:
                out[key] = f"ERR:{type(e).__name__}: {e}\n"
    return out


def main() -> None:
    tree = fd.build_fd_closure
    fd.build_fd_closure = reference_closure  # type: ignore[assignment]
    base = sweep()
    fd.build_fd_closure = tree  # type: ignore[assignment]
    hoisted = sweep()
    changed = [q for q in base if base[q] != hoisted[q]]
    print(f"\nplans    {len(base) - len(changed)} identical, {len(changed)} changed")
    for q in changed:
        print(f"  {q}: {len(hoisted[q]) - len(base[q]):+d} chars")


if __name__ == "__main__":
    main()
