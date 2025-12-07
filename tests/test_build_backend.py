import sys
from pathlib import Path
from unittest.mock import patch

from pytest import raises

# Add .scripts to path to import build_backend
sys.path.insert(0, str(Path(__file__).parent.parent / ".scripts"))
import build_backend


def test_read_dependencies_success(tmp_path):
    """Test reading dependencies from requirements.txt"""
    req_file = tmp_path / "requirements.txt"
    req_file.write_text(
        """lark
jinja2
# comment line
sqlalchemy<2.0.0

networkx
"""
    )

    # Mock __file__ to point to a fake .scripts directory
    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        deps = build_backend._read_dependencies()

    assert deps == ["lark", "jinja2", "sqlalchemy<2.0.0", "networkx"]


def test_read_dependencies_missing_file(tmp_path):
    """Test reading dependencies when requirements.txt doesn't exist"""
    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        deps = build_backend._read_dependencies()

    assert deps == []


def test_read_dependencies_empty_file(tmp_path):
    """Test reading dependencies from empty requirements.txt"""
    req_file = tmp_path / "requirements.txt"
    req_file.write_text("")

    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        deps = build_backend._read_dependencies()

    assert deps == []


def test_sync_version_success(tmp_path):
    """Test syncing version from __init__.py to Cargo.toml"""
    trilogy_dir = tmp_path / "trilogy"
    trilogy_dir.mkdir()
    init_file = trilogy_dir / "__init__.py"
    init_file.write_text(
        """from trilogy.constants import CONFIG
__version__ = "0.3.136"
"""
    )

    cargo_dir = trilogy_dir / "scripts" / "dependency"
    cargo_dir.mkdir(parents=True)
    cargo_file = cargo_dir / "Cargo.toml"
    cargo_file.write_text(
        """[package]
name = "preql-import-resolver"
version = ""
edition = "2021"
"""
    )

    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        version = build_backend._sync_version()

    assert version == "0.3.136"
    cargo_content = cargo_file.read_text()
    assert 'version = "0.3.136"' in cargo_content


def test_sync_version_with_existing_version(tmp_path):
    """Test syncing version when Cargo.toml already has a version"""
    trilogy_dir = tmp_path / "trilogy"
    trilogy_dir.mkdir()
    init_file = trilogy_dir / "__init__.py"
    init_file.write_text('__version__ = "1.2.3"')

    cargo_dir = trilogy_dir / "scripts" / "dependency"
    cargo_dir.mkdir(parents=True)
    cargo_file = cargo_dir / "Cargo.toml"
    cargo_file.write_text(
        """[package]
name = "preql-import-resolver"
version = "0.1.0"
edition = "2021"
"""
    )

    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        version = build_backend._sync_version()

    assert version == "1.2.3"
    cargo_content = cargo_file.read_text()
    assert 'version = "1.2.3"' in cargo_content


def test_sync_version_missing_version(tmp_path):
    """Test syncing version when __init__.py lacks __version__"""
    trilogy_dir = tmp_path / "trilogy"
    trilogy_dir.mkdir()
    init_file = trilogy_dir / "__init__.py"
    init_file.write_text("from trilogy import parse")

    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        with raises(ValueError, match="Could not find __version__"):
            build_backend._sync_version()


def test_patch_metadata_with_dependencies(tmp_path):
    """Test patching METADATA file with dependencies"""
    metadata_file = tmp_path / "METADATA"
    metadata_file.write_text(
        """Metadata-Version: 2.1
Name: pytrilogy
Version: 0.3.136

This is the description.
"""
    )

    req_file = tmp_path / "requirements.txt"
    req_file.write_text("lark\njinja2\n")

    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        build_backend._patch_metadata(tmp_path)

    content = metadata_file.read_text()
    assert "Requires-Dist: lark\n" in content
    assert "Requires-Dist: jinja2\n" in content
    assert "This is the description." in content


def test_patch_metadata_dist_info_subdirectory(tmp_path):
    """Test patching METADATA file in *.dist-info subdirectory"""
    dist_info = tmp_path / "pytrilogy-1.0.0.dist-info"
    dist_info.mkdir()
    metadata_file = dist_info / "METADATA"
    metadata_file.write_text(
        """Metadata-Version: 2.1
Name: pytrilogy

"""
    )

    req_file = tmp_path / "requirements.txt"
    req_file.write_text("networkx\n")

    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        build_backend._patch_metadata(tmp_path)

    content = metadata_file.read_text()
    assert "Requires-Dist: networkx\n" in content


def test_patch_metadata_no_dependencies(tmp_path):
    """Test patching METADATA when no dependencies exist"""
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir()

    metadata_file = metadata_dir / "METADATA"
    original_content = """Metadata-Version: 2.1
Name: pytrilogy

"""
    metadata_file.write_text(original_content)

    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        build_backend._patch_metadata(metadata_dir)

    content = metadata_file.read_text()
    assert content == original_content


def test_patch_metadata_missing_file(tmp_path):
    """Test patching METADATA when file doesn't exist"""
    req_file = tmp_path / "requirements.txt"
    req_file.write_text("lark\n")

    fake_scripts = tmp_path / ".scripts"
    fake_scripts.mkdir()
    with patch.object(
        build_backend, "__file__", str(fake_scripts / "build_backend.py")
    ):
        build_backend._patch_metadata(tmp_path)
    # Should not crash


def test_build_wheel_syncs_version():
    """Test build_wheel calls _sync_version"""
    with patch("build_backend._sync_version", return_value="1.0.0") as mock_sync:
        with patch("build_backend.maturin.build_wheel", return_value="wheel.whl"):
            result = build_backend.build_wheel("/fake/wheel_dir")

    mock_sync.assert_called_once()
    assert result == "wheel.whl"


def test_build_sdist_syncs_version():
    """Test build_sdist calls _sync_version"""
    with patch("build_backend._sync_version", return_value="1.0.0") as mock_sync:
        with patch("build_backend.maturin.build_sdist", return_value="source.tar.gz"):
            result = build_backend.build_sdist("/fake/sdist_dir")

    mock_sync.assert_called_once()
    assert result == "source.tar.gz"


def test_prepare_metadata_for_build_wheel():
    """Test prepare_metadata_for_build_wheel syncs version and patches metadata"""
    with patch("build_backend._sync_version", return_value="1.0.0"):
        with patch(
            "build_backend.maturin.prepare_metadata_for_build_wheel",
            return_value="pytrilogy-1.0.0.dist-info",
        ):
            with patch("build_backend._patch_metadata") as mock_patch:
                result = build_backend.prepare_metadata_for_build_wheel(
                    "/fake/metadata_dir"
                )

    assert result == "pytrilogy-1.0.0.dist-info"
    mock_patch.assert_called_once()


def test_prepare_metadata_for_build_editable():
    """Test prepare_metadata_for_build_editable syncs version and patches metadata"""
    with patch("build_backend._sync_version", return_value="1.0.0"):
        with patch(
            "build_backend.maturin.prepare_metadata_for_build_editable",
            return_value="pytrilogy-1.0.0.dist-info",
        ):
            with patch("build_backend._patch_metadata") as mock_patch:
                result = build_backend.prepare_metadata_for_build_editable(
                    "/fake/metadata_dir"
                )

    assert result == "pytrilogy-1.0.0.dist-info"
    mock_patch.assert_called_once()


def test_build_editable_syncs_version():
    """Test build_editable calls _sync_version"""
    with patch("build_backend._sync_version", return_value="1.0.0") as mock_sync:
        with patch("build_backend.maturin.build_editable", return_value="editable.whl"):
            result = build_backend.build_editable("/fake/wheel_dir")

    mock_sync.assert_called_once()
    assert result == "editable.whl"


def test_get_requires_for_build_wheel():
    """Test get_requires_for_build_wheel delegates to maturin"""
    with patch(
        "build_backend.maturin.get_requires_for_build_wheel",
        return_value=["maturin>=1.0"],
    ) as mock_get:
        result = build_backend.get_requires_for_build_wheel()

    mock_get.assert_called_once_with(None)
    assert result == ["maturin>=1.0"]


def test_get_requires_for_build_sdist():
    """Test get_requires_for_build_sdist delegates to maturin"""
    with patch(
        "build_backend.maturin.get_requires_for_build_sdist",
        return_value=["maturin>=1.0"],
    ) as mock_get:
        result = build_backend.get_requires_for_build_sdist()

    mock_get.assert_called_once_with(None)
    assert result == ["maturin>=1.0"]


def test_get_requires_for_build_editable():
    """Test get_requires_for_build_editable delegates to maturin"""
    with patch(
        "build_backend.maturin.get_requires_for_build_editable",
        return_value=["maturin>=1.0"],
    ) as mock_get:
        result = build_backend.get_requires_for_build_editable()

    mock_get.assert_called_once_with(None)
    assert result == ["maturin>=1.0"]
