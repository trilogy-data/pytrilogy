"""Patch maturin-built wheels to include dependencies from requirements.txt"""

import sys
import tempfile
import zipfile
from pathlib import Path


def read_dependencies() -> list[str]:
    """Read dependencies from requirements.txt"""
    # Get project root (parent of .scripts directory if running from .scripts)
    project_root = Path(__file__).parent

    req_file = project_root / "requirements.txt"
    if not req_file.exists():
        return []

    deps = []
    for line in req_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            deps.append(line)
    return deps


def patch_metadata(dist_info_path: Path) -> None:
    """Patch the METADATA file to include dependencies"""
    metadata_file = dist_info_path / "METADATA"
    if not metadata_file.exists():
        print(f"METADATA file not found at {metadata_file}")
        return

    deps = read_dependencies()
    if not deps:
        print("No dependencies found in requirements.txt")
        return

    content = metadata_file.read_text(encoding="utf-8")
    lines = content.splitlines(keepends=True)

    # Find the end of headers (blank line before body)
    header_end = 0
    for i, line in enumerate(lines):
        if line.strip() == "":
            header_end = i
            break

    # Insert Requires-Dist entries at the end of headers
    dep_lines = [f"Requires-Dist: {dep}\n" for dep in deps]
    new_lines = lines[:header_end] + dep_lines + lines[header_end:]

    metadata_file.write_text("".join(new_lines), encoding="utf-8")
    print(f"Patched {metadata_file} with {len(deps)} dependencies")


def patch_wheel(wheel_path: Path | str) -> bool:
    """Patch a wheel file to include dependencies"""
    wheel_path = Path(wheel_path)
    if not wheel_path.exists():
        print(f"ERROR: Wheel file not found: {wheel_path}")
        return False

    print(f"Patching {wheel_path.name}")

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Extract wheel
            with zipfile.ZipFile(wheel_path, "r") as zip_ref:
                zip_ref.extractall(tmpdir_path)

            # Find and patch METADATA
            dist_info_dirs = list(tmpdir_path.glob("*.dist-info"))
            if not dist_info_dirs:
                print(f"ERROR: No .dist-info directory found in {wheel_path.name}")
                return False

            patch_metadata(dist_info_dirs[0])

            # Repack the wheel
            wheel_path.unlink()
            with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as zip_ref:
                for file in tmpdir_path.rglob("*"):
                    if file.is_file():
                        arcname = file.relative_to(tmpdir_path)
                        zip_ref.write(file, arcname)

        print(f"Successfully patched {wheel_path.name}")
        return True
    except Exception as e:
        print(f"ERROR: Failed to patch {wheel_path.name}: {e}")
        return False


def main(args: list[str] | None = None) -> int:
    """Main entry point for the script"""
    if args is None:
        args = sys.argv[1:]

    if len(args) < 1:
        print("Usage: python patch_wheels.py <wheel_file_or_directory>")
        return 1

    target = Path(args[0])

    if target.is_dir():
        wheels = list(target.glob("*.whl"))
        if not wheels:
            print(f"No wheel files found in {target}")
            return 1

        success_count = 0
        for wheel in wheels:
            if patch_wheel(wheel):
                success_count += 1

        print(f"\nPatched {success_count}/{len(wheels)} wheels successfully")
        return 0
    elif target.suffix == ".whl":
        return 0 if patch_wheel(target) else 1
    else:
        print(f"Invalid target: {target}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
