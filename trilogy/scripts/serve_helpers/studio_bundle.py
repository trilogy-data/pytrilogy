"""Fetch, cache and resolve the Trilogy Studio bundle that `serve` hosts.

A studio page served from ``https://trilogydata.dev`` cannot talk to a store on
``http://localhost:8100``: Local Network Access is a browser *permission*, and a
fetch that fires unattended on page load is auto-denied. No response header
fixes it. Serving the studio from the same loopback origin as the store means
the permission never comes into play — that is the whole reason this exists.

Contract with the studio repo (docs/studio-bundle-hosting.md):

- Assets live at ``releases/latest/download/<name>`` — a redirect, not
  ``api.github.com``, so there is no 60/hour unauthenticated rate limit to trip.
- ``manifest.json`` carries the version (cache key), the sha256 to verify
  before extracting, and ``basePath``, which is where the bundle *must* be
  mounted: it is built with an absolute vite base, so anywhere else 404s every
  asset.
- ``contractVersion`` is the remote store contract the bundle speaks. A bundle
  newer than this server understands is refused rather than served broken.
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from pydantic import BaseModel, Field

from trilogy.constants import logger
from trilogy.scripts.serve_helpers.models import REMOTE_STORE_CONTRACT_VERSION

STUDIO_RELEASE_BASE = (
    "https://github.com/trilogy-data/trilogy-studio-core/releases/latest/download"
)
STUDIO_CACHE_ROOT = Path.home() / ".trilogy" / "studio"
MANIFEST_FILENAME = "manifest.json"
# The manifest poll is best-effort: a cached bundle must not be held hostage to
# a slow or absent network, so this stays short.
MANIFEST_TIMEOUT = 5.0
DOWNLOAD_TIMEOUT = 300.0


class StudioBundleError(Exception):
    """The bundle could not be fetched or verified."""


class StudioTarball(BaseModel):
    name: str
    bytes: int
    sha256: str


class StudioManifest(BaseModel):
    name: str
    version: str
    contract_version: int = Field(alias="contractVersion")
    base_path: str = Field(alias="basePath")
    tarball: StudioTarball
    commit: str | None = None
    built_at: str | None = Field(default=None, alias="builtAt")


@dataclass
class StudioBundle:
    """An extracted bundle ready to mount."""

    directory: Path
    base_path: str
    version: str


def normalize_base_path(value: str) -> str:
    """`/trilogy-studio-core/` — leading and trailing slash, always."""
    stripped = value.strip().strip("/")
    return f"/{stripped}/" if stripped else "/"


def _read_url(url: str, timeout: float) -> bytes:
    import urllib.request

    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read()


def fetch_manifest(
    release_base: str = STUDIO_RELEASE_BASE, timeout: float = MANIFEST_TIMEOUT
) -> StudioManifest | None:
    """The published manifest, or None if it can't be reached or parsed."""
    url = f"{release_base}/{MANIFEST_FILENAME}"
    try:
        raw = _read_url(url, timeout)
    except Exception as e:
        logger.debug("Studio manifest fetch failed (%s): %s", url, e)
        return None
    try:
        return StudioManifest.model_validate_json(raw)
    except Exception as e:
        logger.warning("Studio manifest at %s is not readable: %s", url, e)
        return None


def load_bundle_directory(directory: Path) -> StudioBundle | None:
    """Read an extracted bundle, or None if the directory isn't one.

    A bundle is only usable with its manifest, which is what carries the mount
    path — guessing that is how you get a page whose every asset 404s.
    """
    manifest_path = directory / MANIFEST_FILENAME
    index_path = directory / "index.html"
    if not manifest_path.is_file() or not index_path.is_file():
        return None
    try:
        manifest = StudioManifest.model_validate_json(manifest_path.read_bytes())
    except Exception as e:
        logger.warning("Ignoring studio bundle at %s: %s", directory, e)
        return None
    return StudioBundle(
        directory=directory,
        base_path=normalize_base_path(manifest.base_path),
        version=manifest.version,
    )


def cached_bundles(cache_root: Path = STUDIO_CACHE_ROOT) -> list[StudioBundle]:
    """Every intact cached bundle, newest install first."""
    if not cache_root.is_dir():
        return []
    found = [
        (child.stat().st_mtime, bundle)
        for child in cache_root.iterdir()
        if child.is_dir() and (bundle := load_bundle_directory(child)) is not None
    ]
    return [
        bundle for _, bundle in sorted(found, key=lambda pair: pair[0], reverse=True)
    ]


def _verify(payload: bytes, tarball: StudioTarball) -> None:
    import hashlib

    if len(payload) != tarball.bytes:
        raise StudioBundleError(
            f"Studio bundle size mismatch: expected {tarball.bytes} bytes, got {len(payload)}"
        )
    digest = hashlib.sha256(payload).hexdigest()
    if digest != tarball.sha256:
        raise StudioBundleError(
            f"Studio bundle checksum mismatch: expected {tarball.sha256}, got {digest}"
        )


def _extract(payload: bytes, manifest: StudioManifest, cache_root: Path) -> Path:
    """Unpack into a temp dir and rename it in, so a killed download never
    leaves a half-populated directory that later reads as a cache hit."""
    import io
    import shutil
    import tarfile
    import tempfile

    cache_root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(dir=cache_root, prefix=".incoming-"))
    try:
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            archive.extractall(staging, filter="data")
        (staging / MANIFEST_FILENAME).write_text(
            manifest.model_dump_json(by_alias=True, indent=2), encoding="utf-8"
        )
        destination = cache_root / manifest.version
        if destination.exists():
            shutil.rmtree(destination, ignore_errors=True)
        staging.rename(destination)
        return destination
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise


def download_bundle(
    manifest: StudioManifest,
    release_base: str = STUDIO_RELEASE_BASE,
    cache_root: Path = STUDIO_CACHE_ROOT,
    timeout: float = DOWNLOAD_TIMEOUT,
) -> StudioBundle:
    url = f"{release_base}/{manifest.tarball.name}"
    last_error: Exception | None = None
    # One retry: a truncated body fails the size check, and re-reading it is
    # cheaper than making the user re-run serve.
    for _ in range(2):
        try:
            payload = _read_url(url, timeout)
            _verify(payload, manifest.tarball)
        except Exception as e:
            last_error = e
            logger.warning("Studio bundle download failed: %s", e)
            continue
        directory = _extract(payload, manifest, cache_root)
        bundle = load_bundle_directory(directory)
        if bundle is None:
            raise StudioBundleError(
                f"Studio bundle extracted to {directory} but has no index.html"
            )
        return bundle
    raise StudioBundleError(f"Could not fetch studio bundle from {url}: {last_error}")


def resolve_studio_bundle(
    explicit_directory: Path | None = None,
    cache_root: Path = STUDIO_CACHE_ROOT,
    release_base: str = STUDIO_RELEASE_BASE,
    manifest_timeout: float = MANIFEST_TIMEOUT,
    download_timeout: float = DOWNLOAD_TIMEOUT,
    on_progress: Callable[[StudioManifest], None] | None = None,
) -> StudioBundle | None:
    """The bundle to host, or None to fall back to the hosted studio.

    Never fatal: every failure downgrades to a cached bundle, then to the
    hosted deep link. A local serve that refuses to start because GitHub is
    unreachable would be a worse trade than a link that needs the network
    anyway.
    """
    if explicit_directory is not None:
        bundle = load_bundle_directory(explicit_directory)
        if bundle is None:
            raise StudioBundleError(
                f"{explicit_directory} is not an extracted studio bundle "
                f"(expected index.html and {MANIFEST_FILENAME})"
            )
        return bundle

    cached = cached_bundles(cache_root)
    manifest = fetch_manifest(release_base, manifest_timeout)
    if manifest is None:
        if cached:
            logger.info("Studio manifest unreachable; serving cached bundle.")
        return cached[0] if cached else None

    if manifest.contract_version > REMOTE_STORE_CONTRACT_VERSION:
        logger.warning(
            "Studio %s speaks store contract v%s, this server implements v%s. "
            "Upgrade pytrilogy to use it.",
            manifest.version,
            manifest.contract_version,
            REMOTE_STORE_CONTRACT_VERSION,
        )
        return cached[0] if cached else None

    for bundle in cached:
        if bundle.version == manifest.version:
            return bundle

    if on_progress is not None:
        on_progress(manifest)
    try:
        return download_bundle(manifest, release_base, cache_root, download_timeout)
    except StudioBundleError as e:
        logger.warning("%s", e)
        return cached[0] if cached else None
