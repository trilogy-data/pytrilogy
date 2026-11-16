"""pytest plugin: run the suite with the join walk's spelling tie-breaks
reversed, so a test whose rows move names an order that decides rows.

    WALK_FLIP=key,side PYTHONPATH=local_scripts/sql_ab \\
        python -m pytest -p walk_flip_plugin tests ...

`key` reverses the rank's last two terms (key length, key name) and the
pairing order under one key; `side` reverses the side name the scorer and
the keyless order break ties on (`tests/helpers/walk_order.py`). Every other
term stays, so a plan that moves moved only on a spelling. A test that fails
under the flip and passes without it is a shape pin or an order that decides
rows; read its assertion.
"""

from __future__ import annotations

import os

from tests.helpers.walk_order import KEY_FLIPS, SIDE_FLIPS
from trilogy.core.processing import join_resolution as jr


def pytest_configure(config) -> None:
    flips = {f for f in os.environ.get("WALK_FLIP", "").split(",") if f}
    for flip, replacements in (("key", KEY_FLIPS), ("side", SIDE_FLIPS)):
        if flip in flips:
            for name, replacement in replacements.items():
                setattr(jr, name, replacement)
