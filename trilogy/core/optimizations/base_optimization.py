from abc import ABC

from trilogy.constants import logger
from trilogy.core.models.execute import CTE, UnionCTE


class OptimizationRule(ABC):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> bool:
        raise NotImplementedError

    def log(self, message: str):
        logger.info(f"[Optimization][{self.__class__.__name__}] {message}")

    def debug(self, message: str):
        logger.debug(f"[Optimization][{self.__class__.__name__}] {message}")
