from preql.core.models import QueryDatasource, CTE
from preql.core.processing.concept_strategies_v2 import StrategyNode


class BaseHook:
    pass

    def process_root_datasource(self, datasource: QueryDatasource):
        pass

    def process_root_cte(self, cte: CTE):
        pass

    def process_root_strategy_node(self, node: StrategyNode):
        pass
