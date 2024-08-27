from typing import Dict, List
from trilogy.core.models import Concept
from collections import defaultdict
from trilogy.utility import unique


def extract_required_subgraphs(
    assocs: defaultdict[str, list], path: List[str]
) -> defaultdict[str, list]:

    ds = path[0]
    current: list[str] = []
    for idx, val in enumerate(path):
        if val.startswith("ds~"):

            if current:
                assocs[ds] += current
                current = [path[idx - 1]] if idx > 0 else []
            ds = val
        else:
            current.append(val)
    else:
        if current:
            assocs[ds] += current

    return assocs


def extract_mandatory_subgraphs(paths: Dict[str, List[str]], g) -> List[List[Concept]]:
    final: list[list[str]] = []
    assocs: defaultdict[str, list] = defaultdict(list)
    for path in paths.values():
        extract_required_subgraphs(assocs, path)

    for _, v in assocs.items():
        final.append(v)
    final_concepts = []
    for value in final:
        final_concepts.append(
            unique(
                [g.nodes[v]["concept"] for v in value if v.startswith("c~")], "address"
            )
        )
    return final_concepts
