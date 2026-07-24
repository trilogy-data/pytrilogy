"""PNG backend: render the report HTML and snapshot it with a headless browser."""

from __future__ import annotations

from pathlib import Path

from trilogy.rendering.theme import DEFAULT_THEME, REPORT_LAYOUT, Theme
from trilogy.report.backends.base import ReportBackend
from trilogy.report.backends.html import build_html
from trilogy.report.document import RenderedElement


def _snapshot(html: str, output_path: Path) -> None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "PNG output requires playwright. Install pytrilogy[report], "
            "then run 'playwright install chromium'."
        ) from exc

    with sync_playwright() as playwright:
        try:
            browser = playwright.chromium.launch()
        except Exception as exc:
            raise RuntimeError(
                "Could not launch chromium. Run 'playwright install chromium'."
            ) from exc
        try:
            page = browser.new_page(
                viewport={"width": REPORT_LAYOUT.viewport_width, "height": 900},
                device_scale_factor=2,
            )
            page.set_content(html, wait_until="networkidle")
            page.evaluate("() => document.fonts.ready.then(() => true)")
            page.screenshot(path=str(output_path), full_page=True)
        finally:
            browser.close()


class PngBackend(ReportBackend):
    extension = "png"

    def render(
        self,
        elements: list[RenderedElement],
        output_path: Path,
        theme: Theme = DEFAULT_THEME,
    ) -> None:
        _snapshot(build_html(elements, theme=theme, interactive=False), output_path)
