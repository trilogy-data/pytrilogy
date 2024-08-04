from trilogy.core.models import (
    CTE,
    Datasource,
)

from trilogy.core.optimizations.base_optimization import OptimizationRule


class InlineDatasource(OptimizationRule):

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:
        if not cte.parent_ctes:
            return False

        optimized = False
        self.log(
            f"Checking {cte.name} for consolidating inline tables with {len(cte.parent_ctes)} parents"
        )
        to_inline: list[CTE] = []
        force_group = False
        for parent_cte in cte.parent_ctes:
            if not parent_cte.is_root_datasource:
                self.log(f"parent {parent_cte.name} is not root")
                continue
            if parent_cte.parent_ctes:
                self.log(f"parent {parent_cte.name} has parents")
                continue
            raw_root = parent_cte.source.datasources[0]
            if not isinstance(raw_root, Datasource):
                self.log(f"parent {parent_cte.name} is not datasource")
                continue
            root: Datasource = raw_root
            if not root.can_be_inlined:
                self.log(f"parent {parent_cte.name} datasource is not inlineable")
                continue
            root_outputs = {x.address for x in root.output_concepts}
            cte_outputs = {x.address for x in parent_cte.output_columns}
            grain_components = {x.address for x in root.grain.components}
            if not cte_outputs.issubset(root_outputs):
                self.log(f"Not all {parent_cte.name} outputs are found on datasource")
                continue
            if not grain_components.issubset(cte_outputs):
                self.log("Not all datasource components in cte outputs, forcing group")
                force_group = True
            to_inline.append(parent_cte)

        for replaceable in to_inline:

            result = cte.inline_parent_datasource(replaceable, force_group=force_group)
            if result:
                self.log(f"Inlined parent {replaceable.name}")
            else:
                self.log(f"Failed to inline {replaceable.name}")
        return optimized
