"""Find WHERE atoms that do nothing.

Oracle: delete one AND-atom from a statement's WHERE, regenerate, and diff the
SQL. Byte-identical output means the atom had no effect on the plan — it is
either provably redundant (the model or another atom already implies it) or
SILENTLY DROPPED, which is a wrong-rows bug that no row test can catch when the
data happens not to exercise it (tpch q02, s50).

Runs each atom under BOTH planners, because that is the triage signal:

  v3 no-op AND v4 no-op   -> redundant; both planners agree it buys nothing
  v3 EFFECTIVE, v4 no-op  -> SUSPECT: v4 alone dropped it
  v3 no-op, v4 effective  -> v4 is stricter; note it, not a defect

Operates on the parsed statement, not the query text, so it is exact for any
model rather than relying on how the .preql happens to be formatted.

Usage: python local_scripts/v4_predicate_audit.py [suite ...]
       suites: tpcds tpch (default both); or a path to a directory of .preql
"""

from __future__ import annotations

import sys
from dataclasses import replace
from pathlib import Path

sys.setrecursionlimit(20000)

from trilogy import Dialects, Executor, parse
from trilogy.constants import CONFIG
from trilogy.core.enums import BooleanOperator
from trilogy.core.models.author import Comment, Conditional
from trilogy.core.models.environment import Environment
from trilogy.dialect.config import DuckDBConfig

ROOT = Path(__file__).parent.parent
SUITES = {
    "tpcds": ROOT / "tests" / "modeling" / "tpc_ds_duckdb",
    "tpch": ROOT / "tests" / "modeling" / "tpc_h",
}


def and_atoms(expr) -> list:
    """Flatten a conjunction. A non-AND node is one opaque atom — an OR is not
    decomposable without changing meaning."""
    if isinstance(expr, Conditional) and expr.operator == BooleanOperator.AND:
        return and_atoms(expr.left) + and_atoms(expr.right)
    return [expr]


def rebuild(atoms: list):
    if not atoms:
        return None
    combined = atoms[0]
    for atom in atoms[1:]:
        combined = Conditional(left=combined, right=atom, operator=BooleanOperator.AND)
    return combined


def render(engine: Executor, working: Path, text: str, drop: tuple[int, int] | None):
    """Generate SQL with one atom removed. `drop` is (statement index, atom
    index); None renders the query unchanged."""
    environment = Environment(working_path=working)
    env, statements = parse(text, environment)
    selects = [s for s in statements if getattr(s, "where_clause", None) is not None]
    if drop is not None:
        stmt_index, atom_index = drop
        statement = selects[stmt_index]
        atoms = and_atoms(statement.where_clause.conditional)
        remaining = atoms[:atom_index] + atoms[atom_index + 1 :]
        combined = rebuild(remaining)
        if combined is None:
            statement.where_clause = None
        else:
            statement.where_clause = replace(
                statement.where_clause, conditional=combined
            )
    engine.environment = env
    # `generate_queries` raises on statement types the executor filters first
    # (a top-level Comment is the common one in these corpora).
    generatable = [s for s in statements if not isinstance(s, Comment)]
    return engine.generator.compile_statement(
        engine.generator.generate_queries(env, generatable)[-1]
    )


def audit_file(engine: Executor, working: Path, path: Path) -> list[tuple]:
    text = path.read_text()
    findings: list[tuple] = []
    try:
        _, statements = parse(text, Environment(working_path=working))
    except Exception as exc:
        return [(path.stem, "-", f"PARSE_ERROR {type(exc).__name__}: {exc}", "", "")]
    selects = [s for s in statements if getattr(s, "where_clause", None) is not None]
    if not selects:
        return findings
    plans: dict[bool, str] = {}
    for v4 in (False, True):
        CONFIG.use_v4_discovery = v4
        try:
            plans[v4] = render(engine, working, text, None)
        except Exception as exc:
            return [
                (
                    path.stem,
                    "-",
                    f"BASELINE_ERROR({'v4' if v4 else 'v3'})" f" {type(exc).__name__}",
                    "",
                    "",
                )
            ]
    for stmt_index, statement in enumerate(selects):
        atoms = and_atoms(statement.where_clause.conditional)
        if len(atoms) < 1:
            continue
        for atom_index, atom in enumerate(atoms):
            effect: dict[bool, str] = {}
            for v4 in (False, True):
                CONFIG.use_v4_discovery = v4
                try:
                    dropped = render(engine, working, text, (stmt_index, atom_index))
                except Exception as exc:
                    effect[v4] = f"error:{type(exc).__name__}"
                    continue
                effect[v4] = "noop" if dropped == plans[v4] else "effective"
            if effect[False] == effect[True] == "effective":
                continue
            findings.append(
                (path.stem, stmt_index, str(atom)[:90], effect[False], effect[True])
            )
    return findings


def main() -> None:
    args = sys.argv[1:] or ["tpcds", "tpch"]
    rows: list[tuple] = []
    for arg in args:
        working = SUITES.get(arg, ROOT / arg)
        engine: Executor = Dialects.DUCK_DB.default_executor(
            environment=Environment(working_path=working), conf=DuckDBConfig()
        )
        memory = working / "memory"
        if memory.exists():
            engine.execute_raw_sql(f"IMPORT DATABASE '{memory}';")
        for path in sorted(
            list(working.glob("query*.preql")) + list(working.glob("adhoc*.preql"))
        ):
            found = audit_file(engine, working, path)
            for row in found:
                print("\t".join(str(x) for x in row), flush=True)
            rows.extend(found)
    print("\n== summary ==")
    suspect = [r for r in rows if r[3] == "effective" and r[4] == "noop"]
    both = [r for r in rows if r[3] == "noop" and r[4] == "noop"]
    stricter = [r for r in rows if r[3] == "noop" and r[4] == "effective"]
    errors = [r for r in rows if "error" in str(r[3]) + str(r[4]).lower()]
    print(f"SUSPECT (v3 effective, v4 no-op): {len(suspect)}")
    for row in suspect:
        print(f"   {row[0]}\t{row[2]}")
    print(f"redundant under both planners:    {len(both)}")
    print(f"v4 stricter than v3:              {len(stricter)}")
    print(f"errors:                           {len(errors)}")


if __name__ == "__main__":
    main()
