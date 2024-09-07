from trilogy.core.models import (
    CTE,
    Datasource,
)

from trilogy.core.optimizations.base_optimization import OptimizationRule
from collections import defaultdict
from trilogy.constants import CONFIG


class InlineDatasource(OptimizationRule):

    def __init__(self):
        super().__init__()
        self.candidates = defaultdict(lambda: set())
        self.count = defaultdict(lambda: 0)

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:
        if not cte.parent_ctes:
            return False

        self.debug(
            f"Checking {cte.name} for consolidating inline tables with {len(cte.parent_ctes)} parents"
        )
        to_inline: list[CTE] = []
        force_group = False
        for parent_cte in cte.parent_ctes:
            if not parent_cte.is_root_datasource:
                self.debug(f"parent {parent_cte.name} is not root")
                continue
            if parent_cte.parent_ctes:
                self.debug(f"parent {parent_cte.name} has parents")
                continue
            if parent_cte.condition:
                self.debug(f"parent {parent_cte.name} has condition, cannot be inlined")
                continue
            raw_root = parent_cte.source.datasources[0]
            if not isinstance(raw_root, Datasource):
                self.debug(f"Parent {parent_cte.name} is not datasource")
                continue
            root: Datasource = raw_root
            if not root.can_be_inlined:
                self.debug(f"Parent {parent_cte.name} datasource is not inlineable")
                continue
            root_outputs = {x.address for x in root.output_concepts}
            inherited = {
                x for x, v in cte.source_map.items() if v and parent_cte.name in v
            }
            if not inherited.issubset(root_outputs):
                cte_missing = inherited - root_outputs
                self.log(
                    f"Not all {parent_cte.name} require inputs are found on datasource, missing {cte_missing}"
                )
                continue
            if not root.grain.issubset(parent_cte.grain):
                self.log(
                    f"{parent_cte.name} is at wrong grain to inline ({root.grain} vs {parent_cte.grain})"
                )
                continue
            to_inline.append(parent_cte)

        optimized = False
        for replaceable in to_inline:
            if replaceable.name not in self.candidates[cte.name]:
                self.candidates[cte.name].add(replaceable.name)
                self.count[replaceable.source.name] += 1
                return True
            if (
                self.count[replaceable.source.name]
                > CONFIG.optimizations.constant_inline_cutoff
            ):
                self.log(
                    f"Skipping inlining raw datasource {replaceable.source.name} ({replaceable.name}) due to multiple references"
                )
                continue
            if not replaceable.source.datasources[0].grain.issubset(replaceable.grain):
                self.log(
                    f"Forcing group ({parent_cte.grain} being replaced by inlined source {root.grain})"
                )
                force_group = True
            result = cte.inline_parent_datasource(replaceable, force_group=force_group)
            if result:
                self.log(
                    f"Inlined parent {replaceable.name} with {replaceable.source.name}"
                )
                optimized = True
            else:
                self.log(f"Failed to inline {replaceable.name}")
        return optimized
