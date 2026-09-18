"""Each eval suite builds its own database.

Every eval dir has a ``spec.py`` and several a ``db_build.py``, and a spec
imported its sibling by bare name. In a process that loads more than one suite
(the viewer, pytest) the first ``db_build`` imported answered for all of them:
dabstep and thelook silently got acme's database builder."""

from __future__ import annotations

import inspect
import sys
from pathlib import Path

import pytest

EVALS = Path(__file__).resolve().parents[2] / "evals"
sys.path.insert(0, str(EVALS))

from common.siblings import load_sibling
from viewer.suites import discover_suites

DB_BUILD_DIRS = sorted(p.parent.name for p in EVALS.glob("*/db_build.py"))


def test_several_suites_share_the_sibling_name():
    assert len(DB_BUILD_DIRS) > 1


def test_each_suite_builds_its_own_database():
    built = {
        suite.spec.eval_dir.name: Path(
            inspect.getsourcefile(suite.spec.database_builder) or ""
        ).parent.name
        for suite in discover_suites().values()
        if suite.spec.eval_dir.name in DB_BUILD_DIRS
    }
    assert built == {name: name for name in DB_BUILD_DIRS}


@pytest.mark.parametrize("name", DB_BUILD_DIRS)
def test_load_sibling_is_scoped_to_its_directory(name: str):
    module = load_sibling(EVALS / name / "spec.py", "db_build")
    assert Path(module.__file__ or "").parent.name == name
    assert load_sibling(EVALS / name / "spec.py", "db_build") is module
    assert "db_build" not in sys.modules
