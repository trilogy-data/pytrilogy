from abc import ABC, abstractmethod
from typing import Any

from trilogy.core.statements.execute import ProcessedChartStatement


def prettify_label(name: str | None) -> str | None:
    """Humanize a concept address for axis / legend display (rendering only).

    Replaces underscores with spaces and title-cases each word. Data values
    and field lookup keys are left untouched.
    """
    if not name:
        return name
    return name.replace("_", " ").title()


class BaseRenderer(ABC):
    @abstractmethod
    def render(
        self,
        statement: ProcessedChartStatement,
        layer_data: list[list[dict]],
    ) -> Any:
        """Render chart from processed chart statement and per-layer query results."""
        pass

    @abstractmethod
    def to_spec(
        self,
        statement: ProcessedChartStatement,
        layer_data: list[list[dict]],
    ) -> dict:
        """Generate raw visualization spec (e.g., Vega-Lite JSON)."""
        pass
