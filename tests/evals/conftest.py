"""Keep eval tests off the real history db.

Building the viewer's grid now syncs runs into ``evals/eval_history.db``. Under
test that would write synthetic runs into the developer's actual history, so
every test in this directory gets its own throwaway database file."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "evals"))

from common import archive


@pytest.fixture(autouse=True)
def isolated_history_db(tmp_path_factory, monkeypatch):
    path = tmp_path_factory.mktemp("history") / "eval_history.db"
    monkeypatch.setattr(archive, "default_db_path", lambda: path)
    return path
