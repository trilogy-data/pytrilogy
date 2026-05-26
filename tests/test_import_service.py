"""Coverage for parsing.v2.import_service helpers — missing-import suggestion +
read_import_text branches that don't trigger from regular env.parse()."""

from __future__ import annotations

import pytest

from trilogy.parsing.v2.import_service import _suggest_import_paths


def test_suggest_import_paths_missing_root_returns_empty(tmp_path):
    assert _suggest_import_paths("store_sales", tmp_path / "nope") == []


def test_suggest_import_paths_root_is_file_returns_empty(tmp_path):
    """A search_root that is a file (not a dir) is treated as empty."""
    f = tmp_path / "a_file.txt"
    f.write_text("")
    assert _suggest_import_paths("store_sales", f) == []


def test_suggest_import_paths_skips_hidden_underscored_dirs(tmp_path):
    """Hidden / `_*` / `node_modules` / `venv` dirs are skipped."""
    (tmp_path / "_worker_0" / "raw").mkdir(parents=True)
    (tmp_path / "_worker_0" / "raw" / "items.preql").write_text("")
    (tmp_path / "node_modules" / "raw").mkdir(parents=True)
    (tmp_path / "node_modules" / "raw" / "items.preql").write_text("")
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "items.preql").write_text("")
    out = _suggest_import_paths("items", tmp_path)
    assert "raw.items" in out
    assert all("_worker" not in p for p in out)
    assert all("node_modules" not in p for p in out)


def test_suggest_import_paths_respects_max_depth(tmp_path):
    """A match deeper than max_depth is excluded."""
    deep = tmp_path / "a" / "b" / "c" / "d" / "e" / "f" / "g"
    deep.mkdir(parents=True)
    (deep / "deep.preql").write_text("")
    out = _suggest_import_paths("deep", tmp_path, max_depth=2)
    assert out == []


def test_suggest_import_paths_returns_max_hits(tmp_path):
    """Even when many files match, the result is capped to max_hits."""
    for i in range(20):
        d = tmp_path / f"ns{i}"
        d.mkdir()
        (d / "store.preql").write_text("")
    out = _suggest_import_paths("store", tmp_path, max_hits=3)
    assert len(out) == 3


def test_read_import_text_unsupported_resolver_raises(tmp_path):
    """An Environment whose config.import_resolver isn't a known resolver
    surfaces a 'resolver type ... not supported' ImportError."""
    from trilogy.parsing.v2.import_service import _read_import_text

    class _StubResolver:
        pass

    class _StubConfig:
        import_resolver = _StubResolver()

    class _StubEnvironment:
        config = _StubConfig()
        working_path = str(tmp_path)

    with pytest.raises(ImportError, match="not supported"):
        _read_import_text("anything", _StubEnvironment())  # type: ignore[arg-type]


def test_read_import_text_dict_resolver_unknown_address(tmp_path):
    from trilogy.core.models.environment import DictImportResolver
    from trilogy.parsing.v2.import_service import _read_import_text

    class _StubConfig:
        import_resolver = DictImportResolver(content={"known.preql": "key id int;"})

    class _StubEnv:
        config = _StubConfig()
        working_path = str(tmp_path)

    with pytest.raises(ImportError, match="not resolvable"):
        _read_import_text("missing.preql", _StubEnv())  # type: ignore[arg-type]


def test_read_import_text_stdlib_propagates_oserror(tmp_path):
    """is_stdlib=True must NOT swallow OSError into a 'Did you mean' hint —
    stdlib resolution failures bubble up unchanged."""
    from trilogy.core.models.environment import FileSystemImportResolver
    from trilogy.parsing.v2.import_service import _read_import_text

    class _StubConfig:
        import_resolver = FileSystemImportResolver()

    class _StubEnv:
        config = _StubConfig()
        working_path = str(tmp_path)

    with pytest.raises(OSError):
        _read_import_text(
            str(tmp_path / "missing.preql"), _StubEnv(), is_stdlib=True  # type: ignore[arg-type]
        )
