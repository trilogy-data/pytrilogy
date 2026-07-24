"""Visual themes and layout for rendered output.

A `Theme` covers what is meant to vary aesthetically — the font stack and the
color palette. `Layout` covers structural decisions (widths, spacing, the type
scale) that stay fixed so output keeps a disciplined, editorial look.

Theme resolution precedence: built-in default → `trilogy.toml [report].theme`
→ CLI `--theme` → per-statement `copy (theme=...)`.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum


class Appearance(str, Enum):
    LIGHT = "light"
    DARK = "dark"


@dataclass(frozen=True)
class Theme:
    """Themeable surface: typeface and colors. Accents are used sparingly."""

    name: str
    font_stack: str
    webfont_url: str | None
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
    chart_palette: tuple[str, ...]
    # drives derived chart chrome (grid/label contrast); themes are not
    # required to have an opposite-appearance sibling
    appearance: Appearance = Appearance.LIGHT
    counterpart: str | None = None


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
    title_size: int = 48
    section_size: int = 32
    subsection_size: int = 20
    body_size: int = 16
    caption_size: int = 12
    # weights
    title_weight: int = 750
    section_weight: int = 700
    subsection_weight: int = 600
    body_weight: int = 420
    # letter-spacing — tighter for larger display type
    title_tracking: str = "-0.04em"
    section_tracking: str = "-0.03em"
    subsection_tracking: str = "-0.02em"
    # line heights
    title_line_height: float = 1.0
    section_line_height: float = 1.15
    subsection_line_height: float = 1.3
    body_line_height: float = 1.65
    # vertical rhythm
    section_gap: int = 72
    heading_gap: int = 20
    block_gap: int = 18
    table_padding: str = "14px 18px"
    table_line_height: float = 1.45
    # charts
    chart_height: int = 380
    chart_axis_allowance: int = 84
    row_gap: int = 28
    headline_height: int = 100

    @property
    def inner_width(self) -> int:
        """Readable content width inside the card padding."""
        return self.content_width - 2 * self.content_padding

    @property
    def chart_width(self) -> int:
        """Plot width for a static chart, leaving room for axis labels."""
        return self.inner_width - self.chart_axis_allowance

    def chart_box(self, columns: int = 1) -> tuple[int, int]:
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
    text_secondary="#5f5c56",
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
    counterpart="inter-dark",
)

# Same hue system on warm dark neutrals; accents brightened a step so they
# keep contrast against the dark surfaces.
INTER_DARK_THEME = replace(
    INTER_THEME,
    name="inter-dark",
    appearance=Appearance.DARK,
    counterpart="inter",
    text_primary="#ecebe8",
    text_secondary="#b5b2ab",
    text_muted="#8f8c84",
    page_background="#161514",
    card_background="#1e1d1b",
    section_tint="#26241f",
    border="rgba(255,255,255,0.08)",
    accent_primary="#8B98F0",
    accent_secondary="#8FD4B6",
    accent_warning="#EEC36A",
    chart_palette=(
        "#8B98F0",
        "#8FD4B6",
        "#EEC36A",
        "#B3A3E3",
        "#EDA893",
        "#89C5D8",
    ),
)

# Editorial/premium feel: same color system, a more characterful typeface.
EDITORIAL_THEME = replace(
    INTER_THEME,
    name="editorial",
    counterpart="editorial-dark",
    font_stack='"Instrument Sans", "Manrope", "Satoshi", system-ui, sans-serif',
    webfont_url=(
        "https://fonts.googleapis.com/css2?family=Instrument+Sans:wght@400..700"
        "&family=Manrope:wght@400..700&display=swap"
    ),
)

EDITORIAL_DARK_THEME = replace(
    INTER_DARK_THEME,
    name="editorial-dark",
    counterpart="editorial",
    font_stack=EDITORIAL_THEME.font_stack,
    webfont_url=EDITORIAL_THEME.webfont_url,
)

DEFAULT_THEME = INTER_THEME

THEMES: dict[str, Theme] = {
    INTER_THEME.name: INTER_THEME,
    INTER_DARK_THEME.name: INTER_DARK_THEME,
    EDITORIAL_THEME.name: EDITORIAL_THEME,
    EDITORIAL_DARK_THEME.name: EDITORIAL_DARK_THEME,
}


def register_theme(theme: Theme) -> None:
    THEMES[theme.name] = theme


def get_theme(name: str) -> Theme:
    try:
        return THEMES[name]
    except KeyError:
        raise ValueError(f"Unknown theme '{name}'. Available: {sorted(THEMES)}")


def default_theme(appearance: Appearance = Appearance.LIGHT) -> Theme:
    if appearance == Appearance.LIGHT:
        return DEFAULT_THEME
    counterpart = DEFAULT_THEME.counterpart
    if counterpart is None:
        return DEFAULT_THEME
    return THEMES[counterpart]
