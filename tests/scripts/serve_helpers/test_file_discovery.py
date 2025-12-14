import tempfile
from pathlib import Path

from trilogy.scripts.serve_helpers.file_discovery import (
    extract_description_from_file,
    find_preql_files,
    get_relative_model_name,
    get_safe_model_name,
    read_file_content,
)


def test_find_preql_files_empty_directory():
    """Test finding preql files in an empty directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        result = find_preql_files(tmppath)
        assert result == []


def test_find_preql_files_single_file():
    """Test finding a single preql file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "test.preql"
        test_file.write_text("select 1;")

        result = find_preql_files(tmppath)
        assert len(result) == 1
        assert result[0] == test_file


def test_find_preql_files_nested_directories():
    """Test finding preql files in nested directories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        file1 = tmppath / "root.preql"
        file1.write_text("select 1;")

        subdir = tmppath / "models"
        subdir.mkdir()
        file2 = subdir / "model.preql"
        file2.write_text("select 2;")

        nested = subdir / "nested"
        nested.mkdir()
        file3 = nested / "deep.preql"
        file3.write_text("select 3;")

        result = find_preql_files(tmppath)
        assert len(result) == 3
        assert set(result) == {file1, file2, file3}


def test_find_preql_files_ignores_other_extensions():
    """Test that only .preql files are found."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        preql_file = tmppath / "model.preql"
        preql_file.write_text("select 1;")

        sql_file = tmppath / "query.sql"
        sql_file.write_text("select 2;")

        txt_file = tmppath / "readme.txt"
        txt_file.write_text("some text")

        result = find_preql_files(tmppath)
        assert len(result) == 1
        assert result[0] == preql_file


def test_get_relative_model_name_root_level():
    """Test getting relative model name for root level file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        test_file.write_text("select 1;")

        result = get_relative_model_name(test_file, tmppath)
        assert result == "model"


def test_get_relative_model_name_nested():
    """Test getting relative model name for nested file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        subdir = tmppath / "models" / "finance"
        subdir.mkdir(parents=True)
        test_file = subdir / "revenue.preql"
        test_file.write_text("select 1;")

        result = get_relative_model_name(test_file, tmppath)
        assert result == "models/finance/revenue"


def test_get_relative_model_name_handles_backslashes():
    """Test that backslashes are converted to forward slashes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        subdir = tmppath / "a" / "b"
        subdir.mkdir(parents=True)
        test_file = subdir / "c.preql"
        test_file.write_text("select 1;")

        result = get_relative_model_name(test_file, tmppath)
        assert "/" in result
        assert "\\" not in result


def test_get_safe_model_name_no_slashes():
    """Test safe model name with no slashes."""
    result = get_safe_model_name("model")
    assert result == "model"


def test_get_safe_model_name_with_slashes():
    """Test safe model name conversion with slashes."""
    result = get_safe_model_name("models/finance/revenue")
    assert result == "models-finance-revenue"


def test_get_safe_model_name_multiple_slashes():
    """Test safe model name with multiple consecutive slashes."""
    result = get_safe_model_name("a//b///c")
    assert result == "a--b---c"


def test_extract_description_from_file_no_comments():
    """Test extracting description when there are no comments."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        test_file.write_text("select 1;")

        result = extract_description_from_file(test_file)
        assert result == "Trilogy model: model"


def test_extract_description_from_file_with_comment():
    """Test extracting description from file with comment."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        test_file.write_text("# This is a revenue model\nselect 1;")

        result = extract_description_from_file(test_file)
        assert result == "This is a revenue model"


def test_extract_description_from_file_comment_with_whitespace():
    """Test extracting description from comment with extra whitespace."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        test_file.write_text("#   Extra whitespace   \nselect 1;")

        result = extract_description_from_file(test_file)
        assert result == "Extra whitespace"


def test_extract_description_from_file_comment_not_first_line():
    """Test extracting description when comment is not on first line."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        test_file.write_text("\n\n# Customer data model\nselect 1;")

        result = extract_description_from_file(test_file)
        assert result == "Customer data model"


def test_extract_description_from_file_multiple_comments():
    """Test that only the first comment is used."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        test_file.write_text(
            "# First comment\n# Second comment\nselect 1;\n# Third comment"
        )

        result = extract_description_from_file(test_file)
        assert result == "First comment"


def test_extract_description_from_file_beyond_5_lines():
    """Test that comments beyond the first 5 lines are ignored."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        content = "\n" * 6 + "# This comment is too far down"
        test_file.write_text(content)

        result = extract_description_from_file(test_file)
        assert result == "Trilogy model: model"


def test_read_file_content_simple():
    """Test reading file content."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        content = "select 1;\nselect 2;"
        test_file.write_text(content)

        result = read_file_content(test_file)
        assert result == content


def test_read_file_content_empty():
    """Test reading empty file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "empty.preql"
        test_file.write_text("")

        result = read_file_content(test_file)
        assert result == ""
