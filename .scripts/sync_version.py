import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

# Every crate published on the pytrilogy version stream. A crate version always
# corresponds to a real pytrilogy release, so they all move together.
CARGO_MANIFESTS = (
    Path("trilogy") / "scripts" / "dependency" / "Cargo.toml",
    Path("crates") / "trilogy-io" / "Cargo.toml",
)


def read_version() -> str:
    init_file = PROJECT_ROOT / "trilogy" / "__init__.py"
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
    """Sync version from trilogy/__init__.py to every published Cargo.toml."""
    version = read_version()
    for manifest in CARGO_MANIFESTS:
        sync_manifest(PROJECT_ROOT / manifest, version)
    return version


if __name__ == "__main__":
    synced = sync_version()
    print(f"Synced version to {synced} across {len(CARGO_MANIFESTS)} crates")
