from abc import ABC, abstractmethod
from typing import Any

from trilogy.core.statements.author import ChartConfig


class BaseRenderer(ABC):
    @abstractmethod
    def render(self, config: ChartConfig, data: list[dict]) -> Any:
        """Render chart from config and query results."""
        pass

    @abstractmethod
    def to_spec(self, config: ChartConfig, data: list[dict]) -> dict:
        """Generate raw visualization spec (e.g., Vega-Lite JSON)."""
        pass
