import re
from pathlib import Path


def sync_version():
    """Sync version from trilogy/__init__.py to Cargo.toml"""
    # Get project root (parent of .scripts directory)
    project_root = Path(__file__).parent.parent

    # Read version from Python
    init_file = project_root / "trilogy" / "__init__.py"
    content = init_file.read_text()
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
    if not match:
        raise ValueError("Could not find __version__ in trilogy/__init__.py")

    version = match.group(1)

    # Update Cargo.toml
    cargo_file = project_root / "trilogy" / "scripts" / "dependency" / "Cargo.toml"
    cargo_content = cargo_file.read_text()

    # Replace version in Cargo.toml (handle empty string or existing version)
    if 'version = ""' in cargo_content:
        new_cargo_content = cargo_content.replace(
            'version = ""', f'version = "{version}"', 1
        )
    else:
        new_cargo_content = re.sub(
            r'(version\s*=\s*")[^"]*(")',
            rf"\g<1>{version}\g<2>",
            cargo_content,
            count=1,
        )

    cargo_file.write_text(new_cargo_content)
    return version


if __name__ == "__main__":
    version = sync_version()
    print(f"Synced version to {version}")
