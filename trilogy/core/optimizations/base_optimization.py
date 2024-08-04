from trilogy.core.models import (
    CTE,
    SelectStatement,
    PersistStatement,
    Datasource,
    MultiSelectStatement,
    Conditional,
    BooleanOperator,
)
from trilogy.core.enums import PurposeLineage
from trilogy.constants import logger, CONFIG
from abc import ABC


class OptimizationRule(ABC):

    def optimize(self, cte: CTE, inverse_map: dict[str, list[CTE]]) -> bool:
        raise NotImplementedError

    def log(self, message: str):
        logger.info(f"[Optimization][{self.__class__.__name__}] {message}")

    def debug(self, message: str):
        logger.debug(f"[Optimization][{self.__class__.__name__}] {message}")
