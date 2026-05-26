"""Coverage for rendering.base — small helper + abstract methods presence."""

from __future__ import annotations

import pytest

from trilogy.rendering.base import BaseRenderer, prettify_label


def test_prettify_label_none_passthrough():
    assert prettify_label(None) is None


def test_prettify_label_empty_passthrough():
    assert prettify_label("") == ""


def test_prettify_label_humanizes_address():
    assert prettify_label("order_total_price") == "Order Total Price"
    assert prettify_label("revenue") == "Revenue"


def test_base_renderer_is_abstract():
    with pytest.raises(TypeError):
        BaseRenderer()  # type: ignore[abstract]
