from trilogy.core.models import (
    CTE,
    SelectStatement,
    PersistStatement,
    Datasource,
    MultiSelectStatement,
)
from trilogy.core.enums import PurposeLineage
from trilogy.constants import logger
from abc import ABC


class OptimizationRule(ABC):

    def optimize(self, cte: CTE) -> bool:
        raise NotImplementedError

    def log(self, message: str):
        logger.info(f"[Optimization][{self.__class__.__name__}] {message}")


class InlineDatasource(OptimizationRule):

    def optimize(self, cte: CTE) -> bool:
        if not cte.parent_ctes:
            return False

        optimized = False
        self.log(
            f"Checking {cte.name} for consolidating inline tables with {len(cte.parent_ctes)} parents"
        )
        to_inline: list[CTE] = []
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
            if not cte_outputs.issubset(root_outputs):
                self.log(f"Not all {parent_cte.name} outputs are found on datasource")
                continue

            to_inline.append(parent_cte)

        for replaceable in to_inline:
            self.log(f"Inlining parent {replaceable.name}")
            cte.inline_parent_datasource(replaceable)

        return optimized


REGISTERED_RULES: list[OptimizationRule] = [InlineDatasource()]


def filter_irrelevant_ctes(input: list[CTE], root_cte: CTE):
    relevant_ctes = set()

    def recurse(cte: CTE):
        relevant_ctes.add(cte.name)
        for cte in cte.parent_ctes:
            recurse(cte)

    recurse(root_cte)
    return [cte for cte in input if cte.name in relevant_ctes]


def is_direct_return_eligible(
    cte: CTE, select: SelectStatement | PersistStatement | MultiSelectStatement
) -> bool:
    if isinstance(select, (PersistStatement, MultiSelectStatement)):
        return False
    derived_concepts = [
        c for c in cte.source.output_concepts if c not in cte.source.input_concepts
    ]
    eligible = True
    conditions = (
        set(x.address for x in select.where_clause.concept_arguments)
        if select.where_clause
        else set()
    )
    if conditions and select.limit:
        return False
    for x in derived_concepts:
        if x.derivation == PurposeLineage.WINDOW:
            return False
        if x.derivation == PurposeLineage.AGGREGATE:
            if x.address in conditions:
                return False
    logger.info(
        f"Upleveling output select to final CTE with derived_concepts {[x.address for x in derived_concepts]}"
    )
    return eligible


def sort_select_output(cte: CTE, query: SelectStatement | MultiSelectStatement):
    hidden_addresses = [c.address for c in query.hidden_components]
    output_addresses = [
        c.address for c in query.output_components if c.address not in hidden_addresses
    ]

    mapping = {x.address: x for x in cte.output_columns}

    new_output = []
    for x in output_addresses:
        new_output.append(mapping[x])
    cte.output_columns = new_output


def optimize_ctes(
    input: list[CTE], root_cte: CTE, select: SelectStatement | MultiSelectStatement
):
    complete = False

    while not complete:
        actions_taken = False
        for rule in REGISTERED_RULES:
            for cte in input:
                actions_taken = rule.optimize(cte)
        complete = not actions_taken

    if is_direct_return_eligible(root_cte, select):
        root_cte.order_by = select.order_by
        root_cte.limit = select.limit
        root_cte.condition = (
            select.where_clause.conditional if select.where_clause else None
        )
        root_cte.requires_nesting = False
        sort_select_output(root_cte, select)

    return filter_irrelevant_ctes(input, root_cte)
