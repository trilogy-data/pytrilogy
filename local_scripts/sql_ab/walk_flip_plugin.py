"""pytest plugin: run the suite with the join walk's order changed, so a
test whose rows move names an order that decides rows.

    WALK_FLIP=shape,name PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p walk_flip_plugin tests ...

`shape` reverses the shape terms of the order the walk reads (each key's,
`_key_terms`, and each side's, `_shape_terms`: the `~` tiers, the plan's
order, hosting); `name` reverses the side name that breaks its ties
(`tests/helpers/walk_order.py`); `holders` reverses the holder count, a row
term, to find what it still decides. A test that fails
under the flip and passes without it is a shape pin or an order that
decides rows; read its assertion.
"""

from __future__ import annotations

import os

from tests.helpers.walk_order import HOLDER_FLIPS, NAME_FLIPS, SHAPE_FLIPS


def pytest_configure(config) -> None:
    flips = {f for f in os.environ.get("WALK_FLIP", "").split(",") if f}
    for flip, replacements in (
        ("shape", SHAPE_FLIPS),
        ("name", NAME_FLIPS),
        ("holders", HOLDER_FLIPS),
    ):
        if flip in flips:
            for owner, name, replacement in replacements:
                setattr(owner, name, replacement)
