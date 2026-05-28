"""Inspect q04's concept and group graphs after the phase split.

We want to know: where do the WHERE atoms land, and do their row_args
exist in the relevant buckets / lineage chains?
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import networkx as nx  # noqa: E402

from discovery_v4 import _find_select, _materialize_for_query, TPCDS_DIR  # noqa: E402
from trilogy import Environment  # noqa: E402
from trilogy.core.processing.concept_strategies_v4 import History  # noqa: E402
from trilogy.core.processing.condition_utility import decompose_condition  # noqa: E402
from trilogy.core.processing.v4_helper.concept_graph import (  # noqa: E402
    build_concept_graph,
)
from trilogy.core.processing.v4_helper.group_graph import build_group_graph  # noqa: E402


def main() -> None:
    path = TPCDS_DIR / "query04.preql"
    env = Environment(working_path=TPCDS_DIR)
    _, queries = env.parse(path.read_text())
    select = _find_select(queries)
    history = History(base_environment=env)
    build_stmt, build_env, conditions = _materialize_for_query(env, select, history)

    cg = build_concept_graph(
        mandatory_list=list(build_stmt.output_components),
        environment=build_env,
        conditions=[conditions] if conditions else [],
    )

    print(f"=== concept_graph ({cg.number_of_nodes()} nodes) ===\n")
    by_label: dict[str, list[str]] = {}
    for n, d in cg.nodes(data=True):
        by_label.setdefault(d.get("label", ""), []).append(
            f"  [{d.get('derivation'):>10}] {d.get('depth_label'):>3} {n}"
        )
    for lbl in sorted(by_label):
        print(f"-- label={lbl!r}")
        for line in by_label[lbl]:
            print(line)

    print("\n=== lineage edges ===")
    for u, v, ed in cg.edges(data=True):
        if ed.get("kind") == "lineage":
            print(f"  {u}  ->  {v}")
    print("\n=== constraint edges ===")
    for u, v, ed in cg.edges(data=True):
        if ed.get("kind") == "constraint":
            print(f"  {u}  ->  {v}")

    if conditions:
        print("\n=== WHERE atoms ===")
        for atom in decompose_condition(conditions.conditional):
            row_args = [c.address for c in atom.row_arguments]
            existence = [
                c.address for grp in atom.existence_arguments for c in grp
            ]
            print(f"  atom: {atom}")
            print(f"    row_args: {row_args}")
            print(f"    existence_args: {existence}")

    gg, attrs = build_group_graph(
        cg,
        [conditions] if conditions else [],
        mandatory_list=list(build_stmt.output_components),
    )
    print(f"\n=== group_graph ({gg.number_of_nodes()} nodes) ===\n")
    for n in gg.nodes:
        if n == "__final__":
            continue
        a = attrs[n]
        marker = " <-- ATOMS HERE" if a.condition_atoms else ""
        print(f"  {n}{marker}")
        print(f"    primary_members: {a.primary_members}")
        if a.output_concepts:
            print(f"    output_concepts: {a.output_concepts}")
        if a.condition_atoms:
            print(f"    condition_atoms: {[str(x) for x in a.condition_atoms]}")


if __name__ == "__main__":
    main()
