#!/usr/bin/env python3
"""Sync version from trilogy/__init__.py to Cargo.toml"""
import re
from pathlib import Path

# Read version from Python
init_file = Path(__file__).parent / "trilogy" / "__init__.py"
content = init_file.read_text()
match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
if not match:
    raise ValueError("Could not find __version__ in trilogy/__init__.py")

version = match.group(1)
print(f"Found version: {version}")

# Update Cargo.toml
cargo_file = Path(__file__).parent / "trilogy" / "scripts" / "dependency" / "Cargo.toml"
cargo_content = cargo_file.read_text()

# Replace version in Cargo.toml
new_cargo_content = re.sub(
    r'(version\s*=\s*")[^"]+(")',
    rf'\g<1>{version}\g<2>',
    cargo_content,
    count=1
)

cargo_file.write_text(new_cargo_content)
print(f"Updated Cargo.toml to version {version}")
