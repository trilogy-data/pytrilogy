import tempfile
from pathlib import Path

from trilogy.scripts.serve_helpers.index_generation import (
    find_model_by_name,
    generate_model_index,
)
from trilogy.scripts.serve_helpers.models import ImportFile


def test_generate_model_index_empty_directory():
    """Test generating index for empty directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        result = generate_model_index(tmppath, "http://localhost:8100", engine="duckdb")
        assert len(result) == 1


def test_generate_model_index_single_file():
    """Test generating index for single file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        test_file.write_text("select 1;")

        result = generate_model_index(tmppath, "http://localhost:8100", engine="duckdb")
        assert len(result) == 1
        assert result[0].name == tmppath.name
        assert result[0].url == f"http://localhost:8100/models/{tmppath.name}.json"


def test_generate_model_index_nested_files():
    """Test generating index for nested files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        file1 = tmppath / "root.preql"
        file1.write_text("select 1;")

        subdir = tmppath / "models"
        subdir.mkdir()
        file2 = subdir / "revenue.preql"
        file2.write_text("select 2;")

        result = generate_model_index(tmppath, "http://localhost:8100", engine="duckdb")
        assert len(result) == 1

        names = {model.name for model in result}
        assert names == {tmppath.name}

        urls = {model.url for model in result}
        assert urls == {
            f"http://localhost:8100/models/{tmppath.name}.json",
        }


def test_generate_model_index_with_custom_base_url():
    """Test generating index with custom base URL."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        test_file.write_text("select 1;")

        result = generate_model_index(tmppath, "https://example.com:9000", "duckdb")
        assert len(result) == 1
        assert result[0].url == f"https://example.com:9000/models/{tmppath.name}.json"


def test_find_model_by_name_not_found():
    """Test finding a model that doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        result = find_model_by_name(
            "nonexistent", tmppath, "http://localhost:8100", "duckdb"
        )
        assert result is None


def test_find_model_by_name_root_level():
    """Test finding a root level model."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "customer.preql"
        test_file.write_text("# Customer model\nselect 1;")

        result = find_model_by_name(
            tmppath.name, tmppath, "http://localhost:8100", "duckdb"
        )
        assert result is not None
        assert result.name == tmppath.name
        assert result.description == "Customer model"
        assert result.engine == "duckdb"
        assert len(result.components) == 1
        assert result.components[0].url == "http://localhost:8100/files/customer.preql"
        assert result.components[0].type == "trilogy"
        assert result.components[0].purpose == "source"


def test_find_model_by_name_nested():
    """Test finding a nested model."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        subdir = tmppath / "finance" / "reports"
        subdir.mkdir(parents=True)
        test_file = subdir / "revenue.preql"
        test_file.write_text("# Revenue report\nselect sum(revenue);")

        result = find_model_by_name(
            tmppath.name, tmppath, "http://localhost:8100", "duckdb"
        )
        assert result is not None
        assert result.name == tmppath.name
        assert result.description == "Revenue report"
        assert (
            result.components[0].url
            == "http://localhost:8100/files/finance/reports/revenue.preql"
        )


def test_find_model_by_name_no_comment():
    """Test finding a model with no comment."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "basic.preql"
        test_file.write_text("select 1;")

        result = find_model_by_name(
            tmppath.name, tmppath, "http://localhost:8100", "duckdb"
        )
        assert result is not None
        assert result.description == "Trilogy model: basic"


def test_find_model_by_name_validates_components():
    """Test that found model has valid ImportFile components."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "test.preql"
        test_file.write_text("select 1;")

        result = find_model_by_name(
            tmppath.name, tmppath, "http://localhost:8100", "duckdb"
        )
        assert result is not None

        component = result.components[0]
        assert isinstance(component, ImportFile)
        assert component.url.startswith("http://")
        assert component.name == "test"
        assert component.alias == ""
        assert component.type == "trilogy"
        assert component.purpose == "source"
