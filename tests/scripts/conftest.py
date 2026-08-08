import os

import pytest


@pytest.fixture(autouse=True)
def restore_process_env():
    """Undo environment changes a CLI invocation made in this process.

    ``trilogy ... --env`` legitimately writes into ``os.environ``
    (``execution/config.apply_env_vars``), and ``CliRunner`` runs that in the
    pytest process rather than a subprocess. ``monkeypatch`` cannot undo it
    because the test never made the assignment, so a dummy credential written
    by one test outlives it -- and a later test that skips itself only when a
    key is absent then stops skipping and makes a real API call with a bogus
    key. Ordering hid this (``tests/ai`` sorts before ``tests/scripts``) until
    pytest-randomly reordered them.
    """
    before = dict(os.environ)
    yield
    if os.environ != before:
        os.environ.clear()
        os.environ.update(before)
