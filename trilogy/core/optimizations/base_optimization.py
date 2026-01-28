from abc import ABC

from trilogy.constants import logger
from trilogy.core.models.execute import CTE, UnionCTE

# Maps old CTE name -> new CTE name for merged/replaced CTEs
MergedCTEMap = dict[str, str]


class OptimizationRule(ABC):
    def optimize(
        self, cte: CTE | UnionCTE, inverse_map: dict[str, list[CTE | UnionCTE]]
    ) -> tuple[bool, MergedCTEMap | None]:
        """Returns (actions_taken, merged_cte_map).

        merged_cte_map maps old CTE names to their replacements when a CTE
        is merged into another. Used to update root_cte references.
        """
        raise NotImplementedError

    def log(self, message: str):
        logger.info(f"[Optimization][{self.__class__.__name__}] {message}")

    def debug(self, message: str):
        logger.debug(f"[Optimization][{self.__class__.__name__}] {message}")
