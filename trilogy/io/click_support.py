"""Contract flags as click options, for authors who already have a click group.

Import this module only if you want click; ``trilogy.io`` does not, because
``import click`` costs ~270ms and these scripts run once per query.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from trilogy.io.contract import SourceRequest, request_from_strings
from trilogy.io.sinks import Format


def click_options(fn: Callable) -> Callable:
    """Add the request flags plus ``--format`` and ``--output``.

    The flag set mirrors :func:`trilogy.io.runner.build_parser`; a source is
    still expected to answer ``--describe``, which stays the author's to wire up
    because it is a mode rather than a request field.
    """
    import click

    options = [
        click.option("--limit", type=int, default=None, help="maximum rows to emit"),
        click.option("--columns", default=None, help="comma-separated projection"),
        click.option(
            "--filter", "filters", multiple=True, help="row predicate, repeatable"
        ),
        click.option(
            "--order-by",
            "order_by",
            default=None,
            help="comma-separated sort keys, e.g. 'score:desc,id'",
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
    return request_from_strings(
        limit=kwargs.get("limit"),
        columns=kwargs.get("columns"),
        filters=kwargs.get("filters") or (),
        order_by=kwargs.get("order_by"),
        since=kwargs.get("since"),
        partition=kwargs.get("partition") or (),
    )


__all__: Sequence[str] = ["click_options", "request_from_kwargs"]
