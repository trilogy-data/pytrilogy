"""Custom build backend that syncs version before building with maturin"""

import re
from pathlib import Path

# Import all maturin backend functions
import maturin


def _read_dependencies():
    """Read dependencies from requirements.txt"""
    req_file = Path(__file__).parent / "requirements.txt"
    if not req_file.exists():
        return []

    deps = []
    for line in req_file.read_text().splitlines():
        line = line.strip()
        # Skip empty lines and comments
        if line and not line.startswith('#'):
            deps.append(line)
    return deps


def _patch_metadata(metadata_directory):
    """Patch the METADATA file to include dependencies from requirements.txt"""
    metadata_file = Path(metadata_directory)

    # Find the METADATA file
    if metadata_file.is_dir():
        metadata_files = list(metadata_file.glob("*.dist-info/METADATA"))
        if not metadata_files:
            return
        metadata_file = metadata_files[0]

    if not metadata_file.exists():
        return

    # Read dependencies and append to metadata
    deps = _read_dependencies()
    if not deps:
        return

    # Append dependencies as Requires-Dist entries
    with metadata_file.open('a', encoding='utf-8') as f:
        for dep in deps:
            f.write(f"Requires-Dist: {dep}\n")


def _patch_wheel(wheel_path):
    """Patch the METADATA file inside a built wheel"""
    import zipfile
    import tempfile

    wheel_path = Path(wheel_path)
    if not wheel_path.exists():
        return

    deps = _read_dependencies()
    if not deps:
        return

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Extract wheel
        with zipfile.ZipFile(wheel_path, 'r') as zf:
            zf.extractall(tmpdir)

        # Find and patch METADATA
        metadata_files = list(tmpdir.glob("*.dist-info/METADATA"))
        if metadata_files:
            metadata_file = metadata_files[0]
            with metadata_file.open('a', encoding='utf-8') as f:
                for dep in deps:
                    f.write(f"Requires-Dist: {dep}\n")

            # Rebuild wheel
            wheel_path.unlink()
            with zipfile.ZipFile(wheel_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for file in tmpdir.rglob('*'):
                    if file.is_file():
                        zf.write(file, file.relative_to(tmpdir))


def _sync_version():
    """Sync version from trilogy/__init__.py to Cargo.toml"""
    # Read version from Python
    init_file = Path(__file__).parent / "trilogy" / "__init__.py"
    content = init_file.read_text()
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
    if not match:
        raise ValueError("Could not find __version__ in trilogy/__init__.py")

    version = match.group(1)

    # Update Cargo.toml
    cargo_file = (
        Path(__file__).parent / "trilogy" / "scripts" / "dependency" / "Cargo.toml"
    )
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


# Expose all maturin functions
def get_requires_for_build_wheel(config_settings=None):
    """PEP 517 get_requires_for_build_wheel"""
    return maturin.get_requires_for_build_wheel(config_settings)


def get_requires_for_build_sdist(config_settings=None):
    """PEP 517 get_requires_for_build_sdist"""
    return maturin.get_requires_for_build_sdist(config_settings)


def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    """PEP 517 prepare_metadata_for_build_wheel with version sync"""
    version = _sync_version()
    print(f"Synced version to {version}")
    result = maturin.prepare_metadata_for_build_wheel(metadata_directory, config_settings)
    _patch_metadata(metadata_directory)
    return result


def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    """PEP 517 build_wheel with version sync"""
    version = _sync_version()
    print(f"Synced version to {version}")
    result = maturin.build_wheel(wheel_directory, config_settings, metadata_directory)

    # Patch the built wheel to include dependencies
    if result:
        # Patch both the output wheel and maturin's target wheel
        wheel_path = Path(wheel_directory) / result
        print(f"Patching wheel at {wheel_path}")
        _patch_wheel(wheel_path)

        # Also patch maturin's target directory wheel if it exists
        maturin_wheel = Path(__file__).parent / "trilogy" / "scripts" / "dependency" / "target" / "wheels" / result
        if maturin_wheel.exists():
            print(f"Patching maturin target wheel at {maturin_wheel}")
            _patch_wheel(maturin_wheel)

        print(f"Wheel patched with dependencies from requirements.txt")

    return result


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
    """PEP 660 prepare_metadata_for_build_editable with version sync"""
    version = _sync_version()
    print(f"Synced version to {version}")
    result = maturin.prepare_metadata_for_build_editable(
        metadata_directory, config_settings
    )
    _patch_metadata(metadata_directory)
    return result


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    """PEP 660 build_editable with version sync"""
    version = _sync_version()
    print(f"Synced version to {version}")
    result = maturin.build_editable(wheel_directory, config_settings, metadata_directory)

    # Patch the built wheel to include dependencies
    if result:
        # Patch both the output wheel and maturin's target wheel
        wheel_path = Path(wheel_directory) / result
        _patch_wheel(wheel_path)

        # Also patch maturin's target directory wheel if it exists
        maturin_wheel = Path(__file__).parent / "trilogy" / "scripts" / "dependency" / "target" / "wheels" / result
        if maturin_wheel.exists():
            _patch_wheel(maturin_wheel)

    return result
