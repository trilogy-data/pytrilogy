"""Import-cost guards for the CLI startup path.

Every invocation pays for whatever the entry module drags in, so a single
module-level import in the wrong file taxes the whole CLI: an eager engine
import in ``trilogy/__init__.py`` (which runs before ANY ``trilogy.*``
submodule) once made ``trilogy --version`` take ~700ms instead of ~120ms.

These assert the set of modules loaded, not a wall-clock budget — a timing
threshold flakes on a loaded CI box and doesn't say what regressed.
"""

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# The engine and its drivers. None of these belong on a startup path that only
# prints a version or writes a few files.
FORBIDDEN_MODULES = frozenset(
    {
        "trilogy.core.models.environment",
        "trilogy.executor",
        "trilogy.parser",
        "trilogy.parsing.parse_engine_v2",
        # urllib.request pulls http.client + the email package (~0.1s)
        "urllib.request",
        "http.client",
    }
)
FORBIDDEN_ROOTS = frozenset(
    {"sqlalchemy", "pydantic", "lark", "duckdb", "numpy", "pandas"}
)

# `trilogy --version` resolves no subcommand, so this is the CLI's floor. Extend
# it only for a module as trivial as these — everything here is import-only.
VERSION_TRILOGY_MODULES = frozenset(
    {"trilogy", "trilogy.scripts", "trilogy.scripts.click_utils"}
)

_PROBE = """
import json, runpy, sys
sys.argv = {argv!r}
try:
    runpy.run_module("trilogy.scripts.trilogy", run_name="__main__")
except SystemExit:
    pass
with open({out!r}, "w") as fh:
    json.dump(sorted(sys.modules), fh)
"""


def modules_loaded_by(argv: list[str], out_file: Path) -> set[str]:
    """Run the CLI in a fresh interpreter and return the modules it imported."""
    probe = _PROBE.format(argv=["trilogy", *argv], out=str(out_file))
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        # Explicit utf-8: the CLI prints rich box-drawing characters, which a
        # cp1252 default decode raises on (in a reader thread, where it surfaces
        # as an unrelated-looking warning rather than a failure).
        encoding="utf-8",
        errors="replace",
        cwd=REPO_ROOT,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return set(json.loads(out_file.read_text(encoding="utf-8")))


def forbidden_hits(modules: set[str]) -> list[str]:
    return sorted(
        m
        for m in modules
        if m in FORBIDDEN_MODULES or m.split(".")[0] in FORBIDDEN_ROOTS
    )


def test_version_imports_nothing_but_the_group(tmp_path):
    modules = modules_loaded_by(["--version"], tmp_path / "version.json")

    loaded = {m for m in modules if m == "trilogy" or m.startswith("trilogy.")}
    assert loaded == set(VERSION_TRILOGY_MODULES), (
        "`trilogy --version` import set changed. Every command pays this cost — "
        "if the new module is genuinely trivial, add it to "
        f"VERSION_TRILOGY_MODULES; otherwise make the import lazy. Loaded: {sorted(loaded)}"
    )
    assert not forbidden_hits(modules)
    assert "rich" not in modules


def test_init_does_not_import_the_engine(tmp_path):
    modules = modules_loaded_by(["init", str(tmp_path / "ws")], tmp_path / "init.json")

    # A probe that didn't actually run the command would pass this vacuously.
    assert (tmp_path / "ws" / "trilogy.toml").exists()
    hits = forbidden_hits(modules)
    assert not hits, f"`trilogy init` should not need the engine, but imported: {hits}"


def test_probe_detects_the_engine(tmp_path):
    """A guard that cannot see the engine is not a guard — prove this one can."""
    out = tmp_path / "engine.json"
    probe = (
        "import json, sys, trilogy.executor\n"
        f"json.dump(sorted(sys.modules), open({str(out)!r}, 'w'))\n"
    )
    subprocess.run([sys.executable, "-c", probe], check=True, cwd=REPO_ROOT)
    assert forbidden_hits(set(json.loads(out.read_text(encoding="utf-8"))))
