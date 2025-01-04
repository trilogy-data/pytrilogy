from trilogy.core.execute_models import (
    CTE,
    QueryDatasource,
    UnionCTE,
)
from trilogy.core.author_models import MultiSelectStatement, PersistStatement, RowsetDerivationStatement, SelectStatement
from trilogy.core.processing.nodes import StrategyNode
from trilogy.core.execute_models import BoundSelectStatement, BoundMultiSelectStatement


class BaseHook:
    pass


    def process_select_info(self, select: BoundSelectStatement):
        print(f"Select statement with {len(select.output_components)} concepts, grain {select.grain}")

    def process_persist_info(self, persist: PersistStatement):
        print(f"Persist statement persisting to {persist.address}")
        self.process_select_info(persist.select)

    def process_rowset_info(self, rowset: RowsetDerivationStatement):
        print(f"Rowset statement with {len(rowset.select.output_components)} concepts")

    def process_root_datasource(self, datasource: QueryDatasource):
        pass

    def process_root_cte(self, cte: CTE | UnionCTE):
        pass

    def process_root_strategy_node(self, node: StrategyNode):
        pass
