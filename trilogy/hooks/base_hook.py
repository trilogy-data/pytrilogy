from trilogy.core.models.execute import (
    CTE,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.processing.nodes import StrategyNode
from trilogy.core.statements.author import (
    MultiSelectStatement,
    PersistStatement,
    RowsetDerivationStatement,
    SelectStatement,
)


class BaseHook:
    pass

    def process_multiselect_info(self, select: MultiSelectStatement):
        print("Multiselect with components:")
        for x in select.selects:
            self.process_select_info(x)

    def process_select_info(self, select: SelectStatement):
        print(f"Select statement grain: {str(select.grain)}")

    def process_persist_info(self, persist: PersistStatement):
        print(f"Persist statement persisting to {persist.address}")
        self.process_select_info(persist.select)

    def process_rowset_info(self, rowset: RowsetDerivationStatement):
        print(f"Rowset statement with grain {str(rowset.select.grain)}")

    def process_root_datasource(self, datasource: QueryDatasource):
        pass

    def process_root_cte(self, cte: CTE | UnionCTE):
        pass

    def process_root_strategy_node(self, node: StrategyNode):
        pass
