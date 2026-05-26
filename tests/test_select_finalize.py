"""Coverage for select-finalize helpers."""

from __future__ import annotations

from trilogy.parsing.v2.select_finalize import (
    _aggregate_full_signature,
    _aggregate_grain_signature,
    _render_aggregate,
    _strip_local_namespace,
)


def test_strip_local_namespace_strips_only_local_prefix():
    assert _strip_local_namespace("local.x") == "x"
    assert _strip_local_namespace("foo.bar") == "foo.bar"
    assert _strip_local_namespace("local") == "local"


def test_aggregate_full_signature_for_non_aggregate_returns_none():
    class _Plain:
        pass

    assert _aggregate_full_signature(_Plain()) is None
    assert _aggregate_full_signature(None) is None


def test_render_aggregate_falls_through_to_str_for_non_aggregate():
    class _Plain:
        def __str__(self):
            return "<plain>"

    assert _render_aggregate(_Plain()) == "<plain>"


def test_aggregate_grain_signature_returns_empty_for_no_by():
    """Mirror the empty-by case where `aggregate_grain_signature(agg)` returns ()."""

    class _Stub:
        by = []

    assert _aggregate_grain_signature(_Stub()) == ()
