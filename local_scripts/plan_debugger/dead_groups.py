"""Count built groups whose node never reaches their plan's FINAL tree.

    python local_scripts/plan_debugger/dead_groups.py "tests/modeling/tpc_h/*.preql" "tests/modeling/tpc_ds_duckdb/query*.preql"
    python local_scripts/plan_debugger/dead_groups.py --detail tests/modeling/tpc_ds_duckdb/query37.preql

The viewer strikes such a group through (`notInFinal` in viewer.html); this
applies the same rule to whole corpora. A dead group is classified by its
derivation and by why it is dead:

    unbuilt      the build produced no node
    twin         the FINAL tree holds an untagged node of the same type, outputs
                 and datasource: the group was rebuilt or copied without its tag
    resourced    FINAL re-sourced the group under its own context and used that
    subtree      every consumer of the group in the group graph is dead too
    consumed     some consumer is alive but did not read this node (absorbed)
    leaf         the group has no consumer in the group graph

and whether a nested plan (an existence feeder or rowset body) builds a group
of the same id, which is the q37 shape: the outer root is dead and the
feeder's own plan answers the question. See
docs/handoff_duplicate_source_requests.md, "What remains".
"""

from __future__ import annotations

import argparse
import glob
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from trilogy import Environment
from trilogy.core.processing import plan_trace
from trilogy.core.processing.v4_helper.group_graph import FINAL_NODE_ID
from trilogy.core.query_processor import process_query
from trilogy.core.statements.author import MultiSelectStatement, SelectStatement


@dataclass
class Dead:
    file: str
    plan: str
    depth: int
    group: str
    derivation: str
    reason: str
    nested_twin: bool
    outputs: list[str]
    origin: str


def tree_groups(node: dict | None, out: set[str] | None = None) -> set[str]:
    out = set() if out is None else out
    if not node or "ref" in node:
        return out
    if node.get("group"):
        out.add(node["group"])
    for p in node["parents"]:
        tree_groups(p, out)
    return out


def tree_nodes(node: dict | None) -> list[dict]:
    if not node or "ref" in node:
        return []
    return [node] + [n for p in node["parents"] for n in tree_nodes(p)]


def statements_of(path: Path):
    env, statements = Environment(working_path=path.parent).parse(
        path.read_text(encoding="utf-8")
    )
    return env, [
        s for s in statements if isinstance(s, (SelectStatement, MultiSelectStatement))
    ]


def trace_of(env, select) -> dict:
    with plan_trace.recording() as trace:
        process_query(env, select)
    return trace.to_dict()


def consumers_of(steps: list[dict], plan: str) -> dict[str, set[tuple[str, str]]]:
    """group -> its (consumer, edge kind) pairs from the plan's last group_graph
    pass, without the merge edge every bucket has into FINAL."""
    graphs = [s for s in steps if s["phase"] == "group_graph" and s["plan"] == plan]
    out: dict[str, set[tuple[str, str]]] = defaultdict(set)
    if graphs:
        for e in graphs[-1]["data"]["graph"]["edges"]:
            if e["v"] != FINAL_NODE_ID:
                out[e["u"]].add((e["v"], e["kind"] or "?"))
    return out


def reason_for(
    gid: str,
    built: dict[str, dict],
    dead: set[str],
    consumers: dict[str, set[tuple[str, str]]],
    final_nodes: list[dict],
) -> str:
    step = built[gid]
    node = step["data"]["node"]
    if node is None:
        return "unbuilt"
    if any(
        n.get("group") is None
        and n["type"] == node["type"]
        and set(n["outputs"]) == set(node["outputs"])
        and n.get("datasource") == node.get("datasource")
        for n in final_nodes
    ):
        return "twin"
    outputs = set(step["data"]["outputs"])
    if any(
        n.get("sourced_in") == "FINAL" and outputs <= set(n["outputs"])
        for n in final_nodes
    ):
        return "resourced"
    readers = consumers.get(gid, set())
    if not readers:
        return "final-only"
    live = {(g, kind) for g, kind in readers if g not in dead}
    if not live:
        return "subtree"
    if all(kind == "existence" for _, kind in live):
        return "existence"
    return "consumed"


def census(file: str, trace: dict) -> list[Dead]:
    steps = trace["steps"]
    depth = {p["id"]: p["depth"] for p in trace["plans"]}
    built_by_plan: dict[str, dict[str, dict]] = defaultdict(dict)
    final_by_plan: dict[str, dict | None] = {}
    for s in steps:
        if s["phase"] == "node":
            built_by_plan[s["plan"]][s["data"]["group"]] = s
        elif s["phase"] == "final":
            final_by_plan[s["plan"]] = s["data"]["node"]
    out: list[Dead] = []
    for plan, built in built_by_plan.items():
        if plan not in final_by_plan:
            continue
        final = final_by_plan[plan]
        alive = tree_groups(final)
        dead = set(built) - alive
        if not dead:
            continue
        consumers = consumers_of(steps, plan)
        final_nodes = tree_nodes(final)
        nested = {
            g
            for p, b in built_by_plan.items()
            if p != plan and depth.get(p, 0) > depth.get(plan, 0)
            for g in b
        }
        for gid in sorted(dead):
            step = built[gid]
            out.append(
                Dead(
                    file=file,
                    plan=plan,
                    depth=depth.get(plan, 0),
                    group=gid,
                    derivation=step["data"]["derivation"],
                    reason=reason_for(gid, built, dead, consumers, final_nodes),
                    nested_twin=gid in nested,
                    outputs=step["data"]["outputs"],
                    origin=" > ".join(step["origin"]),
                )
            )
    return out


def collect(patterns: list[str]) -> tuple[list[Dead], Counter]:
    found: list[Dead] = []
    totals: Counter = Counter()
    for pattern in patterns:
        for name in sorted(glob.glob(pattern)):
            path = Path(name)
            try:
                env, selects = statements_of(path)
            except Exception as exc:
                print(f"skip {path}: {exc!r}", file=sys.stderr)
                continue
            label = f"{path.parent.name}/{path.name}"
            for select in selects:
                try:
                    trace = trace_of(env, select)
                except Exception as exc:
                    print(f"skip a statement of {path}: {exc!r}", file=sys.stderr)
                    continue
                totals["statements"] += 1
                totals["built"] += sum(
                    1 for s in trace["steps"] if s["phase"] == "node"
                )
                found.extend(census(label, trace))
    return found, totals


def summary(patterns: list[str]) -> None:
    found, totals = collect(patterns)
    per_file: Counter = Counter(d.file for d in found)
    print("file dead")
    for file, n in per_file.most_common():
        print(file, n)
    print(
        f"TOTAL {len(found)} dead of {totals['built']} built groups "
        f"in {totals['statements']} statements"
    )
    print("by derivation:", Counter(d.derivation for d in found).most_common())
    print("by reason:", Counter(d.reason for d in found).most_common())
    print(
        "by derivation+reason:",
        Counter((d.derivation, d.reason) for d in found).most_common(),
    )
    print(
        "nested twin (same group id built in a nested plan):",
        sum(d.nested_twin for d in found),
    )
    print("in nested plans (depth>0):", sum(d.depth > 0 for d in found))


def detail(patterns: list[str]) -> None:
    found, _ = collect(patterns)
    for d in found:
        twin = " [nested twin]" if d.nested_twin else ""
        print(
            f"{d.file} {d.plan}(d{d.depth}) {d.group} {d.derivation} {d.reason}{twin}"
        )
        print(f"    outputs={d.outputs}")
        print(f"    {d.origin}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("patterns", nargs="*", help="globs of .preql files")
    parser.add_argument("--detail", action="store_true", help="list every dead group")
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")  # group ids carry "∅"
    if args.detail:
        detail(args.patterns)
    else:
        summary(args.patterns)


if __name__ == "__main__":
    main()
