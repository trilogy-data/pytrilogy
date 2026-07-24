from collections import defaultdict

from trilogy.core.models.author import Concept
from trilogy.utility import unique


def extract_required_subgraphs(
    assocs: defaultdict[str, list], path: list[str]
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
    if current:
        assocs[ds] += current

    return assocs


def extract_mandatory_subgraphs(paths: dict[str, list[str]], g) -> list[list[Concept]]:
    final: list[list[str]] = []
    assocs: defaultdict[str, list] = defaultdict(list)
    for path in paths.values():
        extract_required_subgraphs(assocs, path)

    final.extend(assocs.values())
    final_concepts = []
    for value in final:
        final_concepts.append(
            unique(
                [g.nodes[v]["concept"] for v in value if v.startswith("c~")], "address"
            )
        )
    return final_concepts
