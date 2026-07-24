"""Backend protocol for rendering a report document to a final artifact."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from trilogy.rendering.theme import DEFAULT_THEME, Theme
from trilogy.report.document import RenderedElement


class ReportBackend(ABC):
    """Renders a list of report elements to an output file."""

    extension: str

    @abstractmethod
    def render(
        self,
        elements: list[RenderedElement],
        output_path: Path,
        theme: Theme = DEFAULT_THEME,
    ) -> None: ...
