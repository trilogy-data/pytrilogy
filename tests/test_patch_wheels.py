import sys
import zipfile
from pathlib import Path

import pytest  # <-- Add pytest import for fixture

sys.path.insert(0, str(Path(__file__).parent.parent / ".scripts"))
import patch_wheels


# --- Pytest Fixture for Tempfile Setup ---
@pytest.fixture
def set_patch_wheels_location(tmp_path: Path):
    """
    A fixture to temporarily change the __file__ attribute of the patch_wheels module
    to simulate a specific requirements.txt location relative to the test path.
    The requirements.txt is expected to be next to this simulated file.
    It yields the original __file__ path if needed, but primarily manages cleanup.
    """
    # Use a nested path to simulate a typical setup for tests that need it
    simulated_path = tmp_path / "nested" / "patch_wheels.py"

    original_file = patch_wheels.__file__
    patch_wheels.__file__ = str(simulated_path)

    # Yield control to the test function
    yield original_file  # Can yield the path or just None, but original_file is handy

    # Teardown: Reset the original __file__ attribute
    patch_wheels.__file__ = original_file


# --- End Fixture ---


def test_read_dependencies(tmp_path: Path, set_patch_wheels_location) -> None:
    """Test reading dependencies from requirements.txt"""
    req_file = tmp_path / "requirements.txt"
    req_file.write_text("""lark
jinja2
# This is a comment
sqlalchemy<2.0.0

networkx
pyodbc
""")

    deps = patch_wheels.read_dependencies()

    assert deps == ["lark", "jinja2", "sqlalchemy<2.0.0", "networkx", "pyodbc"]


def test_read_dependencies_empty(tmp_path: Path, set_patch_wheels_location) -> None:
    """Test reading dependencies when requirements.txt doesn't exist"""

    # The simulated path in the fixture ensures requirements.txt won't be found
    deps = patch_wheels.read_dependencies()

    assert deps == []


def test_patch_metadata(tmp_path: Path, set_patch_wheels_location) -> None:
    """Test patching METADATA file with dependencies"""
    dist_info = tmp_path / "test-1.0.0.dist-info"
    dist_info.mkdir()

    metadata_file = dist_info / "METADATA"
    metadata_file.write_text("""Metadata-Version: 2.4
Name: test
Version: 1.0.0
Classifier: Programming Language :: Python
Requires-Dist: existing-dep ; extra == 'extra'

This is the description.
""")

    req_file = tmp_path / "requirements.txt"
    req_file.write_text("lark\njinja2\nsqlalchemy<2.0.0\n")

    patch_wheels.patch_metadata(dist_info)

    content = metadata_file.read_text()

    # Verify dependencies were added before the blank line
    assert "Requires-Dist: lark\n" in content
    assert "Requires-Dist: jinja2\n" in content
    assert "Requires-Dist: sqlalchemy<2.0.0\n" in content

    # Verify existing content is preserved
    assert "Requires-Dist: existing-dep ; extra == 'extra'" in content
    assert "This is the description." in content

    # Verify structure: headers, deps, blank line, body
    lines = content.splitlines()
    blank_line_idx = None
    for i, line in enumerate(lines):
        if line == "":
            blank_line_idx = i
            break

    assert blank_line_idx is not None
    # New dependencies should be before blank line
    dep_lines = [
        i
        for i, line in enumerate(lines[:blank_line_idx])
        if "Requires-Dist: lark" in line
    ]
    assert len(dep_lines) == 1
    assert dep_lines[0] < blank_line_idx


def test_patch_metadata_no_dependencies(
    tmp_path: Path, set_patch_wheels_location
) -> None:
    """Test patching when no dependencies exist"""
    dist_info = tmp_path / "test-1.0.0.dist-info"
    dist_info.mkdir()

    metadata_file = dist_info / "METADATA"
    original_content = """Metadata-Version: 2.4
Name: test

Description
"""
    metadata_file.write_text(original_content)

    patch_wheels.patch_metadata(dist_info)

    # Content should be unchanged when no requirements.txt
    assert metadata_file.read_text() == original_content


# ... (Helper function create_test_wheel remains unchanged) ...
def create_test_wheel(
    wheel_path: Path, metadata_content: str, package_name: str = "test"
) -> None:
    """Helper to create a minimal wheel for testing"""
    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as whl:
        # Add METADATA
        dist_info = f"{package_name}-1.0.0.dist-info"
        whl.writestr(f"{dist_info}/METADATA", metadata_content)
        whl.writestr(f"{dist_info}/WHEEL", "Wheel-Version: 1.0\n")
        whl.writestr(f"{dist_info}/RECORD", "")
        # Add a dummy module
        whl.writestr(f"{package_name}/__init__.py", "# test module\n")


def test_patch_wheel_end_to_end(tmp_path: Path, set_patch_wheels_location) -> None:
    """Test patching a complete wheel file"""
    wheel_path = tmp_path / "test-1.0.0-py3-none-any.whl"

    original_metadata = """Metadata-Version: 2.4
Name: test
Version: 1.0.0

Test package
"""
    create_test_wheel(wheel_path, original_metadata)

    req_file = tmp_path / "requirements.txt"
    req_file.write_text("lark\njinja2\n")

    result = patch_wheels.patch_wheel(wheel_path)

    assert result is True
    assert wheel_path.exists()

    # Verify the wheel was patched correctly
    with zipfile.ZipFile(wheel_path, "r") as whl:
        metadata = whl.read("test-1.0.0.dist-info/METADATA").decode("utf-8")

    assert "Requires-Dist: lark" in metadata
    assert "Requires-Dist: jinja2" in metadata
    assert "Test package" in metadata


def test_patch_wheel_preserves_structure(
    tmp_path: Path, set_patch_wheels_location
) -> None:
    """Test that patching preserves all wheel files"""
    wheel_path = tmp_path / "test-1.0.0-py3-none-any.whl"

    original_metadata = """Metadata-Version: 2.4
Name: test
Version: 1.0.0

"""
    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as whl:
        dist_info = "test-1.0.0.dist-info"
        whl.writestr(f"{dist_info}/METADATA", original_metadata)
        whl.writestr(f"{dist_info}/WHEEL", "Wheel-Version: 1.0\n")
        whl.writestr(f"{dist_info}/RECORD", "file1,hash1\nfile2,hash2\n")
        whl.writestr("test/__init__.py", "# init\n")
        whl.writestr("test/module.py", "def func():\n    pass\n")

    req_file = tmp_path / "requirements.txt"
    req_file.write_text("lark\n")

    patch_wheels.patch_wheel(wheel_path)

    # Verify all files are preserved
    with zipfile.ZipFile(wheel_path, "r") as whl:
        names = set(whl.namelist())

    expected_files = {
        "test-1.0.0.dist-info/METADATA",
        "test-1.0.0.dist-info/WHEEL",
        "test-1.0.0.dist-info/RECORD",
        "test/__init__.py",
        "test/module.py",
    }
    assert expected_files == names


def test_patch_wheel_nonexistent(tmp_path: Path, set_patch_wheels_location) -> None:
    """Test patching a wheel that doesn't exist"""
    wheel_path = tmp_path / "nonexistent.whl"

    result = patch_wheels.patch_wheel(wheel_path)

    assert result is False


def test_patch_wheel_no_dist_info(tmp_path: Path, set_patch_wheels_location) -> None:
    """Test patching a wheel without .dist-info directory"""
    wheel_path = tmp_path / "test-1.0.0-py3-none-any.whl"

    with zipfile.ZipFile(wheel_path, "w", zipfile.ZIP_DEFLATED) as whl:
        whl.writestr("test/__init__.py", "# test\n")

    req_file = tmp_path / "requirements.txt"
    req_file.write_text("lark\n")

    result = patch_wheels.patch_wheel(wheel_path)

    assert result is False


def test_patch_multiple_wheels_in_directory(
    tmp_path: Path, set_patch_wheels_location
) -> None:
    """Test patching all wheels in a directory via main"""
    # Create multiple wheels
    for i in range(3):
        wheel_path = tmp_path / f"test{i}-1.0.0-py3-none-any.whl"
        metadata = f"""Metadata-Version: 2.4
Name: test{i}
Version: 1.0.0

Package {i}
"""
        create_test_wheel(wheel_path, metadata, package_name=f"test{i}")

    req_file = tmp_path / "requirements.txt"
    req_file.write_text("lark\njinja2\n")

    # Simulate running the script on directory
    wheels = list(tmp_path.glob("*.whl"))
    success_count = 0
    for wheel in wheels:
        if patch_wheels.patch_wheel(wheel):
            success_count += 1

    assert success_count == 3

    # Verify all wheels were patched
    for i in range(3):
        wheel_path = tmp_path / f"test{i}-1.0.0-py3-none-any.whl"
        with zipfile.ZipFile(wheel_path, "r") as whl:
            metadata = whl.read(f"test{i}-1.0.0.dist-info/METADATA").decode("utf-8")
        assert "Requires-Dist: lark" in metadata
        assert "Requires-Dist: jinja2" in metadata


def test_main_with_directory(tmp_path: Path, capsys, set_patch_wheels_location) -> None:
    """Test main function with directory argument"""
    # Create test wheels
    for i in range(2):
        wheel_path = tmp_path / f"pkg{i}-1.0.0-py3-none-any.whl"
        metadata = f"Metadata-Version: 2.4\nName: pkg{i}\nVersion: 1.0.0\n\n"
        create_test_wheel(wheel_path, metadata, package_name=f"pkg{i}")

    req_file = tmp_path / "requirements.txt"
    req_file.write_text("lark\n")

    exit_code = patch_wheels.main([str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Patched 2/2 wheels successfully" in captured.out


def test_main_with_single_wheel(
    tmp_path: Path, capsys, set_patch_wheels_location
) -> None:
    """Test main function with single wheel argument"""
    wheel_path = tmp_path / "test-1.0.0-py3-none-any.whl"
    metadata = "Metadata-Version: 2.4\nName: test\nVersion: 1.0.0\n\n"
    create_test_wheel(wheel_path, metadata)

    req_file = tmp_path / "requirements.txt"
    req_file.write_text("lark\n")

    exit_code = patch_wheels.main([str(wheel_path)])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "Successfully patched" in captured.out


# Tests without the repeated setup remain the same
def test_main_with_no_args(capsys) -> None:
    """Test main function with no arguments"""
    exit_code = patch_wheels.main([])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Usage:" in captured.out


def test_main_with_none_args(capsys, monkeypatch) -> None:
    """Test main function with None args (uses sys.argv)"""
    # Simulate running with no command line args
    monkeypatch.setattr("sys.argv", ["patch_wheels.py"])
    exit_code = patch_wheels.main(None)

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Usage:" in captured.out


def test_main_with_invalid_target(
    tmp_path: Path, capsys, set_patch_wheels_location
) -> None:
    """Test main function with invalid target"""
    invalid_path = tmp_path / "not_a_wheel.txt"
    invalid_path.write_text("test")

    exit_code = patch_wheels.main([str(invalid_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "Invalid target:" in captured.out


def test_main_with_empty_directory(
    tmp_path: Path, capsys, set_patch_wheels_location
) -> None:
    """Test main function with directory containing no wheels"""
    exit_code = patch_wheels.main([str(tmp_path)])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "No wheel files found" in captured.out
