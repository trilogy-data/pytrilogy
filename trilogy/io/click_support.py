"""Contract flags as click options, for authors who already have a click group.

Import this module only if you want click; ``trilogy.io`` does not, because
``import click`` costs ~270ms and these scripts run once per query.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from trilogy.io.contract import Filter, SourceRequest
from trilogy.io.sinks import Format


def click_options(fn: Callable) -> Callable:
    """Add ``--limit/--columns/--filter/--since/--partition/--format/--output``."""
    import click

    options = [
        click.option("--limit", type=int, default=None, help="maximum rows to emit"),
        click.option("--columns", default=None, help="comma-separated projection"),
        click.option(
            "--filter", "filters", multiple=True, help="row predicate, repeatable"
        ),
        click.option("--since", default=None, help="watermark low bound"),
        click.option(
            "--partition", multiple=True, metavar="KEY=VALUE", help="partition selector"
        ),
        click.option(
            "--format",
            "fmt",
            type=click.Choice([f.value for f in Format]),
            default=Format.ARROW.value,
        ),
        click.option("--output", default=None, help="destination URI; default stdout"),
    ]
    for option in reversed(options):
        fn = option(fn)
    return fn


def request_from_kwargs(**kwargs: Any) -> SourceRequest:
    """Build a request from the values :func:`click_options` collected."""
    return SourceRequest(
        limit=kwargs.get("limit"),
        columns=_split(kwargs.get("columns")),
        filters=tuple(Filter.parse(f) for f in kwargs.get("filters") or ()),
        since=kwargs.get("since"),
        partition=dict(_pair(p) for p in kwargs.get("partition") or ()),
    )


def _split(raw: str | None) -> tuple[str, ...] | None:
    if not raw:
        return None
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def _pair(raw: str) -> tuple[str, str]:
    key, _, value = raw.partition("=")
    return key.strip(), value.strip()


__all__: Sequence[str] = ["click_options", "request_from_kwargs"]
