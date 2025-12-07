"""Custom build backend that syncs version before building with maturin"""

import re
from pathlib import Path

# Import all maturin backend functions
import maturin


def _read_dependencies():
    """Read dependencies from requirements.txt"""
    # Get project root (parent of .scripts directory)
    project_root = Path(__file__).parent.parent
    req_file = project_root / "requirements.txt"
    if not req_file.exists():
        return []

    deps = []
    for line in req_file.read_text().splitlines():
        line = line.strip()
        # Skip empty lines and comments
        if line and not line.startswith("#"):
            deps.append(line)
    return deps


def _patch_metadata(metadata_directory):
    """Patch the METADATA file to include dependencies from requirements.txt"""
    metadata_dir = Path(metadata_directory)

    # Find the METADATA file - check both the directory itself and subdirectories
    metadata_file = None
    if (metadata_dir / "METADATA").exists():
        metadata_file = metadata_dir / "METADATA"
    else:
        # Look for *.dist-info/METADATA
        metadata_files = list(metadata_dir.glob("*.dist-info/METADATA"))
        if metadata_files:
            metadata_file = metadata_files[0]

    if not metadata_file or not metadata_file.exists():
        return

    deps = _read_dependencies()
    if not deps:
        return

    # Read the current content
    content = metadata_file.read_text(encoding="utf-8")

    # Find the end of headers (blank line before body)
    # METADATA format: headers, blank line, body
    lines = content.splitlines(keepends=True)
    header_end = 0
    for i, line in enumerate(lines):
        if line.strip() == "":
            header_end = i
            break

    # Insert Requires-Dist entries at the end of headers (before blank line)
    dep_lines = [f"Requires-Dist: {dep}\n" for dep in deps]
    new_lines = lines[:header_end] + dep_lines + lines[header_end:]

    # Write back
    metadata_file.write_text("".join(new_lines), encoding="utf-8")


def _sync_version():
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


# Expose all maturin functions with dependency injection
def get_requires_for_build_wheel(config_settings=None):
    """PEP 517 get_requires_for_build_wheel - returns build-time dependencies"""
    # This returns build-time dependencies (like maturin), not runtime dependencies
    return maturin.get_requires_for_build_wheel(config_settings)


def get_requires_for_build_sdist(config_settings=None):
    """PEP 517 get_requires_for_build_sdist"""
    return maturin.get_requires_for_build_sdist(config_settings)


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    """PEP 517 prepare_metadata_for_build_wheel with version sync and dependency injection"""
    version = _sync_version()
    print(f"Synced version to {version}")
    result = maturin.prepare_metadata_for_build_wheel(
        metadata_directory, config_settings
    )
    # The result is the .dist-info directory name, and it's located in metadata_directory
    dist_info_path = Path(metadata_directory) / result
    _patch_metadata(dist_info_path)
    return result


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    """PEP 517 build_wheel with version sync"""
    version = _sync_version()
    print(f"Synced version to {version}")
    return maturin.build_wheel(wheel_directory, config_settings, metadata_directory)


def build_sdist(sdist_directory, config_settings=None):
    """PEP 517 build_sdist with version sync"""
    version = _sync_version()
    print(f"Synced version to {version}")
    return maturin.build_sdist(sdist_directory, config_settings)


# PEP 660 editable install support
def get_requires_for_build_editable(config_settings=None):
    """PEP 660 get_requires_for_build_editable"""
    return maturin.get_requires_for_build_editable(config_settings)


def prepare_metadata_for_build_editable(metadata_directory, config_settings=None):
    """PEP 660 prepare_metadata_for_build_editable with version sync and dependency injection"""
    version = _sync_version()
    print(f"Synced version to {version}")
    result = maturin.prepare_metadata_for_build_editable(
        metadata_directory, config_settings
    )
    # The result is the .dist-info directory name
    dist_info_path = Path(metadata_directory) / result
    _patch_metadata(dist_info_path)
    return result


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    """PEP 660 build_editable with version sync"""
    version = _sync_version()
    print(f"Synced version to {version}")
    return maturin.build_editable(wheel_directory, config_settings, metadata_directory)
