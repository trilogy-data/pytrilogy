"""Report rendering backends and format registry."""

from __future__ import annotations

from trilogy.report.backends.base import ReportBackend
from trilogy.report.backends.html import HtmlBackend
from trilogy.report.backends.png import PngBackend

_BACKENDS: dict[str, type[ReportBackend]] = {
    "html": HtmlBackend,
    "png": PngBackend,
}


def get_backend(name: str) -> ReportBackend:
    try:
        return _BACKENDS[name]()
    except KeyError:
        raise ValueError(
            f"Unknown report format '{name}'. Available: {available_formats()}"
        )


def available_formats() -> list[str]:
    return sorted(_BACKENDS)


__all__ = ["ReportBackend", "available_formats", "get_backend"]
