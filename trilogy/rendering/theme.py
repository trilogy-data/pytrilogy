"""Visual themes and layout for rendered output.

A `Theme` covers what is meant to vary aesthetically — the font stack and the
color palette. `Layout` covers structural decisions (widths, spacing, the type
scale) that stay fixed so output keeps a disciplined, editorial look.

Themes are exposed here as constants; plumbing them through from the config
file is a planned follow-up.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Dict, Optional, Tuple


@dataclass(frozen=True)
class Theme:
    """Themeable surface: typeface and colors. Accents are used sparingly."""

    name: str
    font_stack: str
    webfont_url: Optional[str]
    # text
    text_primary: str
    text_secondary: str
    text_muted: str
    # surfaces (layered warm neutrals — never pure white)
    page_background: str
    card_background: str
    section_tint: str
    # hairlines
    border: str
    # accents
    accent_primary: str
    accent_secondary: str
    accent_warning: str
    # desaturated, slightly pastel chart palette
    chart_palette: Tuple[str, ...]


@dataclass(frozen=True)
class Layout:
    """Fixed structural rhythm: widths, type scale, spacing. Not themeable."""

    content_width: int = 1000
    content_padding: int = 80
    page_margin: int = 48
    viewport_width: int = 1240
    card_radius: int = 18
    card_shadow: str = "0 1px 2px rgba(0,0,0,0.03), 0 8px 24px rgba(0,0,0,0.04)"
    # type scale (px)
    title_size: int = 40
    section_size: int = 26
    subsection_size: int = 17
    body_size: int = 15
    caption_size: int = 12
    # weights
    title_weight: int = 700
    section_weight: int = 650
    subsection_weight: int = 600
    body_weight: int = 420
    # vertical rhythm
    heading_tracking: str = "-0.02em"
    heading_line_height: float = 1.2
    body_line_height: float = 1.65
    section_gap: int = 56
    block_gap: int = 18
    table_padding: str = "14px 18px"
    table_line_height: float = 1.45
    # charts
    chart_height: int = 380
    chart_axis_allowance: int = 84
    row_gap: int = 28
    headline_height: int = 140

    @property
    def inner_width(self) -> int:
        """Readable content width inside the card padding."""
        return self.content_width - 2 * self.content_padding

    @property
    def chart_width(self) -> int:
        """Plot width for a static chart, leaving room for axis labels."""
        return self.inner_width - self.chart_axis_allowance

    def chart_box(self, columns: int = 1) -> Tuple[int, int]:
        """Pixel (width, height) for a static chart in an N-column row."""
        columns = max(columns, 1)
        if columns == 1:
            return self.chart_width, self.chart_height
        cell = (self.inner_width - (columns - 1) * self.row_gap) // columns
        width = cell - self.chart_axis_allowance
        height = round(width * self.chart_height / self.chart_width)
        return width, height


REPORT_LAYOUT = Layout()

INTER_THEME = Theme(
    name="inter",
    font_stack='"Inter", "IBM Plex Sans", "SF Pro Display", system-ui, sans-serif',
    webfont_url="https://fonts.googleapis.com/css2?family=Inter:wght@400..700&display=swap",
    text_primary="#1f1f1c",
    text_secondary="#5f5f58",
    text_muted="#77736b",
    page_background="#f5f5f3",
    card_background="#fbfbf9",
    section_tint="#efeee8",
    border="rgba(0,0,0,0.06)",
    accent_primary="#6D7CE8",
    accent_secondary="#7CC6A6",
    accent_warning="#E6B450",
    chart_palette=(
        "#6D7CE8",
        "#7CC6A6",
        "#E6B450",
        "#9C8AD1",
        "#E0917E",
        "#6FB1C4",
    ),
)

# Editorial/premium feel: same color system, a more characterful typeface.
EDITORIAL_THEME = replace(
    INTER_THEME,
    name="editorial",
    font_stack='"Instrument Sans", "Manrope", "Satoshi", system-ui, sans-serif',
    webfont_url=(
        "https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400..700"
        "&family=Manrope:wght@400..700&display=swap"
    ),
)

DEFAULT_THEME = INTER_THEME

THEMES: Dict[str, Theme] = {
    INTER_THEME.name: INTER_THEME,
    EDITORIAL_THEME.name: EDITORIAL_THEME,
}


def get_theme(name: str) -> Theme:
    try:
        return THEMES[name]
    except KeyError:
        raise ValueError(f"Unknown theme '{name}'. Available: {sorted(THEMES)}")
