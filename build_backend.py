"""Custom build backend that syncs version before building with maturin"""

import re
from pathlib import Path

# Import all maturin backend functions
import maturin


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
    return maturin.prepare_metadata_for_build_wheel(metadata_directory, config_settings)


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
    """PEP 660 prepare_metadata_for_build_editable with version sync"""
    version = _sync_version()
    print(f"Synced version to {version}")
    return maturin.prepare_metadata_for_build_editable(
        metadata_directory, config_settings
    )


def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    """PEP 660 build_editable with version sync"""
    version = _sync_version()
    print(f"Synced version to {version}")
    return maturin.build_editable(wheel_directory, config_settings, metadata_directory)
