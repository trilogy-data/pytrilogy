from preql.core.models import (
    QueryDatasource,
    CTE,
    Select,
    Persist,
    MultiSelect,
    RowsetDerivation,
)
from preql.core.processing.nodes import StrategyNode


class BaseHook:
    pass

    def process_multiselect_info(self, select: MultiSelect):
        print("Multiselect with components:")
        for x in select.selects:
            self.process_select_info(x)

    def process_select_info(self, select: Select):
        print(f"Select statement grain: {str(select.grain)}")

    def process_persist_info(self, persist: Persist):
        print(f"Persist statement persisting to {persist.address}")
        self.process_select_info(persist.select)

    def process_rowset_info(self, rowset: RowsetDerivation):
        print(f"Rowset statement with grain {str(rowset.select.grain)}")

    def process_root_datasource(self, datasource: QueryDatasource):
        pass

    def process_root_cte(self, cte: CTE):
        pass

    def process_root_strategy_node(self, node: StrategyNode):
        pass
