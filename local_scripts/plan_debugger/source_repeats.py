"""Count `plan_source` requests a statement repeats verbatim.

    python local_scripts/plan_debugger/source_repeats.py "tests/modeling/tpc_h/*.preql" "tests/modeling/tpc_ds_duckdb/query*.preql"
    python local_scripts/plan_debugger/source_repeats.py --detail tests/modeling/tpc_h/query11.preql

A request's signature is everything `plan_source` reads that can vary within a
statement: the output addresses, the WHERE and deferred WHERE, the two flags,
the span scope, and the environment/history identity. The summary mode reports
per file how many calls repeat an earlier signature, whether any repeat
returned a structurally DIFFERENT plan (the safety question for a cache), and
the time the repeats cost. `--detail` lists one file's calls with their
planner call path. See docs/handoff_duplicate_source_requests.md.
"""

from __future__ import annotations

import argparse
import glob
import sys
import time
import traceback
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trilogy import Environment
from trilogy.core.processing.v4_helper import source_planning, strategy_builder
from trilogy.core.processing.v4_node_generators import root as root_generator
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement

PATH_FRAMES = {
    "build_strategy_node",
    "_assemble_final_node",
    "_fresh_final_root_projection",
    "_parent_nodes_for",
    "parent_for_consumer",
    "build_node",
    "gen_root",
    "search_concepts",
}
ORIGINAL = source_planning.plan_source
CALLS: list[tuple[tuple, tuple | None, float, str]] = []


def signature(request: source_planning.SourceRequest) -> tuple:
    scope = request.environment.span_scope
    return (
        tuple(sorted(c.address for c in request.outputs)),
        str(request.conditions),
        str(request.deferred_conditions),
        request.require_full,
        request.complete_partials,
        tuple(sorted(scope.owned)),
        tuple(sorted(scope.unextended)),
        tuple(sorted(scope.extent_free)),
        tuple(
            sorted((k, tuple(sorted(v))) for k, v in scope.extent_free_carried.items())
        ),
        id(request.environment),
        id(request.history),
    )


def shape(node) -> tuple | None:
    if node is None:
        return None
    datasource = node.datasource if hasattr(node, "datasource") else None
    return (
        type(node).__name__,
        tuple(sorted(c.address for c in node.output_concepts)),
        datasource.identifier if datasource is not None else None,
        str(node.conditions),
        tuple(shape(p) for p in node.parents),
    )


def call_path() -> str:
    names = [f.name for f in traceback.extract_stack()[:-2]]
    return " > ".join(n for n in names if n in PATH_FRAMES)


def requester(path: str) -> str:
    """The planner frame that asked, above the generic build/source frames."""
    frames = [f for f in path.split(" > ") if f not in ("build_node", "gen_root")]
    return frames[-1] if frames else "?"


def recording_plan_source(request):
    start = time.perf_counter()
    node = ORIGINAL(request)
    CALLS.append(
        (signature(request), shape(node), time.perf_counter() - start, call_path())
    )
    return node


def install() -> None:
    for module in (source_planning, strategy_builder, root_generator):
        module.plan_source = recording_plan_source  # type: ignore[assignment]


def statements_of(path: Path):
    env, statements = Environment(working_path=path.parent).parse(
        path.read_text(encoding="utf-8")
    )
    selects = [
        s for s in statements if isinstance(s, (SelectStatement, MultiSelectStatement))
    ]
    return env, selects


def repeats(calls) -> tuple[int, int, float]:
    first: dict[tuple, tuple | None] = {}
    count = differ = 0
    cost = 0.0
    for sig, result, elapsed, _ in calls:
        if sig in first:
            count += 1
            cost += elapsed
            differ += first[sig] != result
        else:
            first[sig] = result
    return count, differ, cost


def summary(patterns: list[str]) -> None:
    totals: Counter = Counter()
    paths: Counter = Counter()
    rows = []
    for pattern in patterns:
        for name in sorted(glob.glob(pattern)):
            path = Path(name)
            try:
                env, selects = statements_of(path)
            except Exception as exc:
                print(f"skip {path}: {exc!r}", file=sys.stderr)
                continue
            for select in selects:
                CALLS.clear()
                try:
                    process_query(env, select)
                except Exception as exc:
                    print(f"skip a statement of {path}: {exc!r}", file=sys.stderr)
                    continue
                count, differ, cost = repeats(CALLS)
                seen: set[tuple] = set()
                for sig, _, _, where in CALLS:
                    if sig in seen:
                        paths[requester(where)] += 1
                    seen.add(sig)
                total = sum(c[2] for c in CALLS)
                totals.update(calls=len(CALLS), repeats=count, differ=differ)
                totals["time"] += total
                totals["repeat_time"] += cost
                if count:
                    rows.append(
                        (f"{path.parent.name}/{path.name}", len(CALLS), count, differ)
                    )
    print("file calls repeats differing")
    for row in sorted(rows, key=lambda r: -r[2]):
        print(*row)
    print(
        f"TOTAL {totals['calls']} calls, {totals['repeats']} verbatim repeats, "
        f"{totals['differ']} repeats with a different plan; "
        f"{totals['repeat_time']:.2f}s of {totals['time']:.2f}s sourcing is repeats"
    )
    print("repeats by caller:", paths.most_common())


def detail(name: str) -> None:
    env, selects = statements_of(Path(name))
    CALLS.clear()
    process_query(env, selects[-1])
    first: dict[tuple, int] = {}
    for i, (sig, result, _, where) in enumerate(CALLS):
        tag = f"  REPEAT of #{first[sig]}" if sig in first else ""
        first.setdefault(sig, i)
        print(
            f"#{i} outputs={list(sig[0])} where={sig[1]} extent_free={list(sig[7])}{tag}"
        )
        print(f"    {where}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("patterns", nargs="*", help="globs of .preql files")
    parser.add_argument("--detail", help="list one file's calls")
    args = parser.parse_args()
    install()
    if args.detail:
        detail(args.detail)
    else:
        summary(args.patterns)


if __name__ == "__main__":
    main()
