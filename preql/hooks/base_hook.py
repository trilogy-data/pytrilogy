from preql.core.models import QueryDatasource, CTE, Select, Persist
from preql.core.processing.concept_strategies_v2 import StrategyNode


class BaseHook:
    pass

    def process_select_info(self, select: Select):
        print(f"Select statement grain: {str(select.grain)}")

    def process_persist_info(self, persist: Persist):
        print(f"Persist statement persisting to {persist.address}")
        self.process_select_info(persist.select)

    def process_root_datasource(self, datasource: QueryDatasource):
        pass

    def process_root_cte(self, cte: CTE):
        pass

    def process_root_strategy_node(self, node: StrategyNode):
        pass
