from typing import Dict, List
from preql.core.models import Concept
from collections import defaultdict
def extract_required_subgraphs(assocs, path:List[str])->List[List[str]]:

    ds = path[0]
    current = []
    for idx, val in enumerate(path):
        if val.startswith('ds~'):
            ds = val
            if current:
                assocs[ds]+= current
                current = [path[idx-1]] if idx>0 else []
        else:
            current.append(val)
    else:
        if current:
            assocs[ds] +=current
    return assocs

def extract_mandatory_subgraphs(paths: Dict[str, List[str]], g)->List[List[Concept]]:
    final:list[list[str]] = []
    assocs = defaultdict(list)
    for path in paths:
        subs = extract_required_subgraphs(assocs, paths[path])
    
    for k, v in assocs.items():
        final.append(v)
    final_concepts = []
    for value in final:
        final_concepts.append([g.nodes[v]["concept"] for v in value if v.startswith("c~")])
    return final_concepts
    


