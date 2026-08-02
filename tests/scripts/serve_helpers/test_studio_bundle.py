import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

from trilogy.scripts.serve_helpers.studio_bundle import (
    StudioBundleError,
    StudioManifest,
    cached_bundles,
    download_bundle,
    fetch_manifest,
    load_bundle_directory,
    normalize_base_path,
    resolve_studio_bundle,
)


def _tarball(files: dict[str, str] | None = None) -> bytes:
    files = files or {"index.html": "<html>studio</html>", "assets/app.js": "//"}
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, content in files.items():
            payload = content.encode()
            info = tarfile.TarInfo(f"./{name}")
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


def _manifest_dict(payload: bytes, version: str = "1.2.3", contract: int = 1) -> dict:
    return {
        "name": "trilogy-studio",
        "version": version,
        "contractVersion": contract,
        "basePath": "/trilogy-studio-core/",
        "commit": "abc123",
        "builtAt": "2026-08-01T21:48:53.737Z",
        "tarball": {
            "name": "trilogy-studio.tgz",
            "bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        },
    }


@pytest.fixture
def release(tmp_path, monkeypatch):
    """A fake releases/latest/download/ directory, served through _read_url."""
    payload = _tarball()
    manifest = _manifest_dict(payload)
    served: dict[str, bytes] = {
        "https://release/manifest.json": json.dumps(manifest).encode(),
        "https://release/trilogy-studio.tgz": payload,
    }

    def fake_read(url: str, timeout: float) -> bytes:
        if url not in served:
            raise OSError(f"404 {url}")
        return served[url]

    monkeypatch.setattr(
        "trilogy.scripts.serve_helpers.studio_bundle._read_url", fake_read
    )
    return served


@pytest.mark.parametrize(
    "value,expected",
    [
        ("/trilogy-studio-core/", "/trilogy-studio-core/"),
        ("trilogy-studio-core", "/trilogy-studio-core/"),
        ("/nested/path", "/nested/path/"),
        ("/", "/"),
    ],
)
def test_normalize_base_path(value, expected):
    assert normalize_base_path(value) == expected


def test_fetch_manifest_parses_camel_case(release):
    manifest = fetch_manifest("https://release")
    assert manifest is not None
    assert manifest.version == "1.2.3"
    assert manifest.contract_version == 1
    assert manifest.base_path == "/trilogy-studio-core/"


def test_fetch_manifest_returns_none_when_unreachable(monkeypatch):
    def boom(url: str, timeout: float) -> bytes:
        raise OSError("no network")

    monkeypatch.setattr("trilogy.scripts.serve_helpers.studio_bundle._read_url", boom)
    assert fetch_manifest("https://release") is None


def test_download_extracts_and_caches(release, tmp_path):
    manifest = fetch_manifest("https://release")
    bundle = download_bundle(manifest, "https://release", tmp_path)
    assert bundle.directory == tmp_path / "1.2.3"
    assert (bundle.directory / "index.html").read_text() == "<html>studio</html>"
    assert (bundle.directory / "assets" / "app.js").is_file()
    assert bundle.base_path == "/trilogy-studio-core/"
    assert load_bundle_directory(bundle.directory) is not None


def test_download_rejects_checksum_mismatch(release, tmp_path, monkeypatch):
    manifest = fetch_manifest("https://release")
    tampered = manifest.model_copy(deep=True)
    tampered.tarball.sha256 = "0" * 64

    with pytest.raises(StudioBundleError, match="checksum mismatch"):
        download_bundle(tampered, "https://release", tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_download_leaves_no_partial_cache_on_failure(release, tmp_path):
    manifest = fetch_manifest("https://release")
    broken = manifest.model_copy(deep=True)
    broken.tarball.name = "missing.tgz"

    with pytest.raises(StudioBundleError):
        download_bundle(broken, "https://release", tmp_path)
    assert cached_bundles(tmp_path) == []


def test_resolve_downloads_then_reuses_cache(release, tmp_path):
    calls: list[str] = []
    first = resolve_studio_bundle(
        cache_root=tmp_path,
        release_base="https://release",
        on_progress=lambda m: calls.append(m.version),
    )
    assert first is not None
    assert calls == ["1.2.3"]

    second = resolve_studio_bundle(
        cache_root=tmp_path,
        release_base="https://release",
        on_progress=lambda m: calls.append(m.version),
    )
    assert second is not None
    assert second.directory == first.directory
    assert calls == ["1.2.3"]


def test_resolve_serves_cache_when_offline(release, tmp_path, monkeypatch):
    bundle = resolve_studio_bundle(cache_root=tmp_path, release_base="https://release")
    assert bundle is not None

    def offline(url: str, timeout: float) -> bytes:
        raise OSError("no network")

    monkeypatch.setattr(
        "trilogy.scripts.serve_helpers.studio_bundle._read_url", offline
    )
    assert (
        resolve_studio_bundle(
            cache_root=tmp_path, release_base="https://release"
        ).directory
        == bundle.directory
    )


def test_resolve_returns_none_when_offline_with_no_cache(tmp_path, monkeypatch):
    def offline(url: str, timeout: float) -> bytes:
        raise OSError("no network")

    monkeypatch.setattr(
        "trilogy.scripts.serve_helpers.studio_bundle._read_url", offline
    )
    assert (
        resolve_studio_bundle(cache_root=tmp_path, release_base="https://release")
        is None
    )


def test_resolve_refuses_newer_contract_version(tmp_path, monkeypatch, caplog):
    payload = _tarball()
    manifest = _manifest_dict(payload, version="9.0.0", contract=99)
    served = {
        "https://release/manifest.json": json.dumps(manifest).encode(),
        "https://release/trilogy-studio.tgz": payload,
    }
    monkeypatch.setattr(
        "trilogy.scripts.serve_helpers.studio_bundle._read_url",
        lambda url, timeout: served[url],
    )

    assert (
        resolve_studio_bundle(cache_root=tmp_path, release_base="https://release")
        is None
    )
    assert "Upgrade pytrilogy" in caplog.text
    assert cached_bundles(tmp_path) == []


def test_resolve_keeps_older_cache_when_new_bundle_is_too_new(
    release, tmp_path, monkeypatch
):
    cached = resolve_studio_bundle(cache_root=tmp_path, release_base="https://release")
    assert cached is not None

    payload = _tarball()
    manifest = _manifest_dict(payload, version="9.0.0", contract=99)
    served = {"https://release/manifest.json": json.dumps(manifest).encode()}
    monkeypatch.setattr(
        "trilogy.scripts.serve_helpers.studio_bundle._read_url",
        lambda url, timeout: served[url],
    )

    resolved = resolve_studio_bundle(
        cache_root=tmp_path, release_base="https://release"
    )
    assert resolved is not None
    assert resolved.version == cached.version


def test_explicit_directory_must_be_a_bundle(tmp_path):
    with pytest.raises(StudioBundleError, match="not an extracted studio bundle"):
        resolve_studio_bundle(explicit_directory=tmp_path, cache_root=tmp_path)


def test_explicit_directory_is_used_verbatim(release, tmp_path):
    downloaded = resolve_studio_bundle(
        cache_root=tmp_path, release_base="https://release"
    )
    assert downloaded is not None
    resolved = resolve_studio_bundle(explicit_directory=downloaded.directory)
    assert resolved is not None
    assert resolved.directory == downloaded.directory
    assert resolved.base_path == "/trilogy-studio-core/"


def test_incomplete_cache_entry_is_ignored(tmp_path):
    half = tmp_path / "0.9.0"
    (half / "assets").mkdir(parents=True)
    (half / "index.html").write_text("<html></html>")
    assert load_bundle_directory(half) is None
    assert cached_bundles(tmp_path) == []


def test_manifest_round_trips_through_cache(release, tmp_path):
    bundle = resolve_studio_bundle(cache_root=tmp_path, release_base="https://release")
    assert bundle is not None
    written = json.loads((bundle.directory / "manifest.json").read_text())
    assert written["contractVersion"] == 1
    assert StudioManifest.model_validate(written).version == "1.2.3"


def test_cached_bundles_orders_newest_install_first(release, tmp_path):
    first = resolve_studio_bundle(cache_root=tmp_path, release_base="https://release")
    assert first is not None
    older = tmp_path / "0.5.0"
    older.mkdir()
    (older / "index.html").write_text("<html></html>")
    (older / "manifest.json").write_text(
        json.dumps(_manifest_dict(_tarball(), version="0.5.0"))
    )
    import os

    os.utime(older, (0, 0))

    assert [bundle.version for bundle in cached_bundles(tmp_path)] == ["1.2.3", "0.5.0"]


def test_bundle_directory_reads_base_path_from_manifest(tmp_path):
    directory = tmp_path / "2.0.0"
    directory.mkdir()
    (directory / "index.html").write_text("<html></html>")
    manifest = _manifest_dict(_tarball(), version="2.0.0")
    manifest["basePath"] = "/somewhere-else/"
    (directory / "manifest.json").write_text(json.dumps(manifest))

    bundle = load_bundle_directory(Path(directory))
    assert bundle is not None
    assert bundle.base_path == "/somewhere-else/"
