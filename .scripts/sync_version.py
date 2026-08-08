import re
from pathlib import Path

# The crate the wheel itself builds (see [tool.maturin] manifest-path). Its
# absence means a broken checkout, so a missing file here is fatal.
WHEEL_MANIFEST = Path("trilogy") / "scripts" / "dependency" / "Cargo.toml"

# Crates published alongside the wheel on the same version stream, but not part
# of it. `crates/` is not in the sdist, so these are synced only when present --
# a wheel built from an sdist must not fail for want of them. CI publishes from
# a full checkout, where they are.
COMPANION_MANIFESTS = (Path("crates") / "trilogy-io" / "Cargo.toml",)


def project_root() -> Path:
    """Parent of the .scripts directory. Read from ``__file__`` at call time so
    a caller can relocate the module (the build-backend tests do)."""
    return Path(__file__).parent.parent


def read_version(root: Path) -> str:
    init_file = root / "trilogy" / "__init__.py"
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', init_file.read_text())
    if not match:
        raise ValueError("Could not find __version__ in trilogy/__init__.py")
    return match.group(1)


def sync_manifest(path: Path, version: str) -> None:
    """Rewrite the first ``version = "..."`` -- the package's own -- in place."""
    content = path.read_text()
    if 'version = ""' in content:
        updated = content.replace('version = ""', f'version = "{version}"', 1)
    else:
        updated = re.sub(
            r'(version\s*=\s*")[^"]*(")', rf"\g<1>{version}\g<2>", content, count=1
        )
    path.write_text(updated)


def sync_version() -> str:
    """Sync version from trilogy/__init__.py to every Cargo.toml present."""
    root = project_root()
    version = read_version(root)
    sync_manifest(root / WHEEL_MANIFEST, version)
    for manifest in COMPANION_MANIFESTS:
        if (root / manifest).exists():
            sync_manifest(root / manifest, version)
    return version


if __name__ == "__main__":
    synced = sync_version()
    root = project_root()
    present = [WHEEL_MANIFEST] + [m for m in COMPANION_MANIFESTS if (root / m).exists()]
    print(f"Synced version to {synced}:")
    for manifest in present:
        print(f"  {manifest.as_posix()}")
    for manifest in COMPANION_MANIFESTS:
        if not (root / manifest).exists():
            print(f"  (skipped, not in this tree) {manifest.as_posix()}")
