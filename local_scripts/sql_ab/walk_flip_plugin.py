"""pytest plugin: run the suite with the join walk's shape order reversed,
so a test whose rows move names an order that decides rows.

    WALK_FLIP=shape,name PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p walk_flip_plugin tests ...

`shape` reverses the shape terms of the order the walk reads (`_shape_terms`:
the `~` tiers, the plan's order, hosting); `name` reverses the side name
that breaks its ties (`tests/helpers/walk_order.py`). The row terms stay, so
a plan that moves moved only on shape. A test that fails under the flip and
passes without it is a shape pin or an order that decides rows; read its
assertion.
"""

from __future__ import annotations

import os

from tests.helpers.walk_order import NAME_FLIPS, SHAPE_FLIPS


def pytest_configure(config) -> None:
    flips = {f for f in os.environ.get("WALK_FLIP", "").split(",") if f}
    for flip, replacements in (("shape", SHAPE_FLIPS), ("name", NAME_FLIPS)):
        if flip in flips:
            for owner, name, replacement in replacements:
                setattr(owner, name, replacement)
