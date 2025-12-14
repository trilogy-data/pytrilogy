"""Integration tests for the serve command."""

import tempfile
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from trilogy.scripts.serve import create_app  # noqa: E402


def create_test_app(
    directory_path: Path,
    base_url: str = "testserver",
    engine: str = "generic",
    port: int = 80,
):
    """Create a test FastAPI app using the same logic as serve.py."""
    app = FastAPI(title="Trilogy Model Server", version="1.0.0")
    app = create_app(app, engine, directory_path, base_url, port)

    return app


def test_serve_root_endpoint():
    """Test the root endpoint returns server information."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "test.preql"
        test_file.write_text("select 1;")

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Trilogy Model Server"
        assert "with 1 files" in data["description"]
        assert "/index.json" in data["endpoints"]


def test_serve_index_endpoint_empty():
    """Test index endpoint with no files still shows the directory as a model."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/index.json")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        # Even with no files, we still have one model (the directory itself)
        assert len(data["models"]) == 1


def test_serve_index_endpoint_with_files():
    """Test index endpoint with directory represented as single model."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "my_model"
        tmppath.mkdir()

        file1 = tmppath / "model1.preql"
        file1.write_text("select 1;")

        subdir = tmppath / "nested"
        subdir.mkdir()
        file2 = subdir / "model2.preql"
        file2.write_text("select 2;")

        app = create_test_app(tmppath, "testserver")
        client = TestClient(app)

        response = client.get("/index.json")
        assert response.status_code == 200
        data = response.json()

        # Now we should only have one model (the directory)
        assert len(data["models"]) == 1
        assert data["models"][0]["name"] == "my_model"
        assert data["models"][0]["url"] == "http://testserver/models/my_model.json"


def test_serve_get_model_not_found():
    """Test getting a model that doesn't exist returns 404."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/models/nonexistent.json")
        assert response.status_code == 404


def test_serve_get_model_success():
    """Test getting a model successfully - directory as model with components."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "customer"
        tmppath.mkdir()
        test_file = tmppath / "base.preql"
        test_file.write_text("# Customer data model\nkey customer_id int;")

        app = create_test_app(tmppath, "testserver")
        client = TestClient(app)

        response = client.get("/models/customer.json")
        assert response.status_code == 200
        data = response.json()

        assert data["name"] == "customer"
        assert data["description"] == "Customer data model"
        assert data["engine"] == "generic"
        assert len(data["components"]) == 1
        assert data["components"][0]["url"] == "http://testserver/files/base.preql"
        assert data["components"][0]["name"] == "base"
        assert data["components"][0]["type"] == "trilogy"
        assert data["components"][0]["purpose"] == "source"


def test_serve_get_model_with_multiple_components():
    """Test getting a model with multiple component files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "finance"
        tmppath.mkdir()

        file1 = tmppath / "aaa_revenue.preql"
        file1.write_text("# Revenue calculations\nkey revenue_id int;")

        file2 = tmppath / "expenses.preql"
        file2.write_text("key expense_id int;")

        app = create_test_app(tmppath, "http://testserver")
        client = TestClient(app)

        response = client.get("/models/finance.json")
        assert response.status_code == 200
        data = response.json()

        assert data["name"] == "finance"
        # Description comes from first file alphabetically (aaa_revenue)
        assert data["description"] == "Revenue calculations"
        assert len(data["components"]) == 2

        # Check both components are included
        component_names = {c["name"] for c in data["components"]}
        assert component_names == {"aaa_revenue", "expenses"}


def test_serve_get_file_not_found():
    """Test getting a file that doesn't exist returns 404."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/files/nonexistent.preql")
        assert response.status_code == 404


def test_serve_get_file_success():
    """Test getting a file successfully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "query.preql"
        content = "select customer_id, sum(revenue)\nfrom sales\ngroup by customer_id;"
        test_file.write_text(content)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/files/query.preql")
        assert response.status_code == 200
        assert response.text == content
        assert response.headers["content-type"] == "text/plain; charset=utf-8"


def test_serve_get_sql_file_success():
    """Test getting a SQL file successfully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "setup.sql"
        content = "CREATE TABLE customers (id INT, name VARCHAR(100));"
        test_file.write_text(content)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/files/setup.sql")
        assert response.status_code == 200
        assert response.text == content
        assert response.headers["content-type"] == "text/plain; charset=utf-8"


def test_serve_get_file_nested():
    """Test getting a nested file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        subdir = tmppath / "models" / "core"
        subdir.mkdir(parents=True)
        test_file = subdir / "base.preql"
        content = "key id int;\nproperty id.name string;"
        test_file.write_text(content)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/files/models-core-base.preql")
        assert response.status_code == 200
        assert response.text == content


def test_serve_model_with_sql_files():
    """Test that SQL files are included as components with type='sql'."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "mymodel"
        tmppath.mkdir()

        preql_file = tmppath / "data.preql"
        preql_file.write_text("key id int;")

        sql_file = tmppath / "setup.sql"
        sql_file.write_text("CREATE TABLE test (id INT);")

        app = create_test_app(tmppath, "testserver")
        client = TestClient(app)

        response = client.get("/models/mymodel.json")
        assert response.status_code == 200
        data = response.json()

        assert len(data["components"]) == 2

        # Check both file types are included
        types = {c["type"] for c in data["components"]}
        assert types == {"trilogy", "sql"}

        # Verify SQL file has correct type
        sql_component = next(c for c in data["components"] if c["name"] == "setup")
        assert sql_component["type"] == "sql"
        assert sql_component["url"] == "http://testserver/files/setup.sql"


def test_serve_cors_headers():
    """Test that CORS headers are set correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.options("/", headers={"Origin": "http://example.com"})
        assert response.headers["access-control-allow-origin"] == "*"


def test_serve_index_urls_use_base_url():
    """Test that index URLs use the provided base_url correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "mymodel"
        tmppath.mkdir()
        test_file = tmppath / "test.preql"
        test_file.write_text("select 1;")

        # Test with custom base URL
        app = create_test_app(tmppath, "myserver.com", port=9000)
        client = TestClient(app)

        response = client.get("/index.json")
        assert response.status_code == 200
        data = response.json()

        assert len(data["models"]) == 1
        assert (
            data["models"][0]["url"] == "http://myserver.com:9000/models/mymodel.json"
        )


def test_serve_model_components_use_base_url():
    """Test that model components use the provided base_url correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "mymodel"
        tmppath.mkdir()
        test_file = tmppath / "model.preql"
        test_file.write_text("select 1;")

        # Test with custom base URL
        app = create_test_app(tmppath, "myserver.com", port=9000)
        client = TestClient(app)

        response = client.get("/models/mymodel.json")
        assert response.status_code == 200
        data = response.json()

        assert len(data["components"]) == 1
        assert (
            data["components"][0]["url"] == "http://myserver.com:9000/files/model.preql"
        )


def test_serve_with_real_test_files():
    """Test serve with actual test files from the project."""
    test_dir = Path(__file__).parent.parent / "modeling" / "aggregates"

    if not test_dir.exists():
        pytest.skip("Test modeling directory not found")

    app = create_test_app(test_dir, "testserver")
    client = TestClient(app)

    # Test root
    response = client.get("/")
    assert response.status_code == 200

    # Test index - should have one model (the directory)
    response = client.get("/index.json")
    assert response.status_code == 200
    data = response.json()
    assert len(data["models"]) == 1

    # Test getting the model
    model_name = data["models"][0]["name"]
    safe_name = model_name.replace("/", "-")
    response = client.get(f"/models/{safe_name}.json")
    assert response.status_code == 200
    model_data = response.json()

    # The model should have components (the individual files)
    assert len(model_data["components"]) > 0

    # Test getting a file
    first_component = model_data["components"][0]
    file_url = first_component["url"].replace("http://testserver/files/", "")
    response = client.get(f"/files/{file_url}")
    assert response.status_code == 200
    assert len(response.text) > 0


def test_serve_localhost_url_when_host_is_0_0_0_0():
    """Test that URLs use localhost instead of 0.0.0.0 for proper resolution."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "testmodel"
        tmppath.mkdir()
        test_file = tmppath / "test.preql"
        test_file.write_text("select 1;")

        # Simulate what serve.py does when host is 0.0.0.0
        host = "0.0.0.0"
        port = 8100
        url_host = "localhost" if host == "0.0.0.0" else host

        app = create_test_app(tmppath, url_host, port=port)
        client = TestClient(app)

        # Check index URLs use localhost
        response = client.get("/index.json")
        assert response.status_code == 200
        data = response.json()
        assert len(data["models"]) == 1
        assert data["models"][0]["url"] == "http://localhost:8100/models/testmodel.json"

        # Check model component URLs use localhost
        response = client.get("/models/testmodel.json")
        assert response.status_code == 200
        data = response.json()
        assert data["components"][0]["url"] == "http://localhost:8100/files/test.preql"


def test_serve_with_trilogy_toml_setup():
    """Test that setup scripts from trilogy.toml are marked with purpose='setup'."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "mymodel"
        tmppath.mkdir()

        # Create setup directory and script
        setup_dir = tmppath / "setup"
        setup_dir.mkdir()
        setup_file = setup_dir / "init.sql"
        setup_file.write_text("CREATE TABLE base (id INT);")

        # Create regular model file
        model_file = tmppath / "model.preql"
        model_file.write_text("key id int;")

        # Create trilogy.toml with setup script
        toml_content = """[engine]
dialect = "duck_db"

[setup]
sql = ['setup/init.sql']
"""
        toml_file = tmppath / "trilogy.toml"
        toml_file.write_text(toml_content)

        app = create_test_app(tmppath, "http://testserver", "duckdb")
        client = TestClient(app)

        response = client.get("/models/mymodel.json")
        assert response.status_code == 200
        data = response.json()

        # Should have 2 components
        assert len(data["components"]) == 2

        # Find setup and source components
        setup_components = [c for c in data["components"] if c["purpose"] == "setup"]
        source_components = [c for c in data["components"] if c["purpose"] == "source"]

        assert len(setup_components) == 1
        assert len(source_components) == 1

        # Verify setup component
        setup = setup_components[0]
        assert setup["name"] == "setup/init"
        assert setup["type"] == "sql"
        assert "setup-init.sql" in setup["url"]

        # Verify source component
        source = source_components[0]
        assert source["name"] == "model"
        assert source["type"] == "trilogy"


def test_serve_with_engine_parameter():
    """Test that the engine parameter is used in the model."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "duckdb_model"
        tmppath.mkdir()
        test_file = tmppath / "data.preql"
        test_file.write_text("key id int;")

        app = create_test_app(tmppath, "http://testserver", "duckdb")
        client = TestClient(app)

        response = client.get("/models/duckdb_model.json")
        assert response.status_code == 200
        data = response.json()

        assert data["engine"] == "duckdb"


def test_serve_with_readme_description():
    """Test that README.md is used for model description."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "my_model"
        tmppath.mkdir()

        # Create README.md
        readme_content = """# My Model

This is a comprehensive data model for customer analytics.

It includes various metrics and dimensions.
"""
        readme_file = tmppath / "README.md"
        readme_file.write_text(readme_content)

        # Create a model file with its own comment
        model_file = tmppath / "model.preql"
        model_file.write_text("# This comment should be ignored\nkey id int;")

        app = create_test_app(tmppath, "http://testserver")
        client = TestClient(app)

        response = client.get("/models/my_model.json")
        assert response.status_code == 200
        data = response.json()

        # Should use README.md title as description
        assert data["description"] == "My Model"


def test_serve_readme_with_content_before_header():
    """Test README.md with content before any headers."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "my_model"
        tmppath.mkdir()

        # Create README.md with content before header
        readme_content = """A simple model for tracking user events.

# Details

More information here.
"""
        readme_file = tmppath / "README.md"
        readme_file.write_text(readme_content)

        model_file = tmppath / "model.preql"
        model_file.write_text("key id int;")

        app = create_test_app(tmppath, "http://testserver")
        client = TestClient(app)

        response = client.get("/models/my_model.json")
        assert response.status_code == 200
        data = response.json()

        # Should use first non-empty line
        assert data["description"] == "A simple model for tracking user events."


def test_serve_fallback_to_file_comment_when_no_readme():
    """Test that file comments are used when README.md doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "my_model"
        tmppath.mkdir()

        model_file = tmppath / "zzz_model.preql"
        model_file.write_text("# Customer analytics model\nkey id int;")

        app = create_test_app(tmppath, "http://testserver")
        client = TestClient(app)

        response = client.get("/models/my_model.json")
        assert response.status_code == 200
        data = response.json()

        # Should use file comment since no README
        assert data["description"] == "Customer analytics model"


def test_serve_with_csv_files():
    """Test that CSV files are included as components with purpose='data'."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "mymodel"
        tmppath.mkdir()

        # Create a model file
        preql_file = tmppath / "model.preql"
        preql_file.write_text("key id int;")

        # Create a CSV file
        csv_file = tmppath / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob")

        app = create_test_app(tmppath, "testserver")
        client = TestClient(app)

        response = client.get("/models/mymodel.json")
        assert response.status_code == 200
        data = response.json()

        assert len(data["components"]) == 2

        # Find the CSV component
        csv_component = next(c for c in data["components"] if c["type"] == "csv")
        assert csv_component["name"] == "data"
        assert csv_component["alias"] == "data"
        assert csv_component["purpose"] == "data"
        assert csv_component["url"] == "http://testserver/files/data.csv"


def test_serve_get_csv_file():
    """Test getting a CSV file successfully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        csv_file = tmppath / "customers.csv"
        content = "id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com"
        csv_file.write_text(content)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/files/customers.csv")
        assert response.status_code == 200
        assert response.text == content
        assert response.headers["content-type"] == "text/plain; charset=utf-8"


def test_serve_model_with_multiple_file_types():
    """Test a model with preql, sql, and csv files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "full_model"
        tmppath.mkdir()

        # Create different file types
        preql_file = tmppath / "model.preql"
        preql_file.write_text("key id int;")

        sql_file = tmppath / "setup.sql"
        sql_file.write_text("CREATE TABLE test (id INT);")

        csv_file = tmppath / "data.csv"
        csv_file.write_text("id,value\n1,100")

        app = create_test_app(tmppath, "http://testserver")
        client = TestClient(app)

        response = client.get("/models/full_model.json")
        assert response.status_code == 200
        data = response.json()

        assert len(data["components"]) == 3

        # Check all file types are present
        types = {c["type"] for c in data["components"]}
        assert types == {"trilogy", "sql", "csv"}

        # Check purposes
        purposes = {c["purpose"] for c in data["components"]}
        assert purposes == {"source", "data"}

        # Verify CSV component has correct purpose
        csv_component = next(c for c in data["components"] if c["type"] == "csv")
        assert csv_component["purpose"] == "data"


def test_serve_nested_csv_file():
    """Test CSV files in subdirectories."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir) / "mymodel"
        tmppath.mkdir()

        # Create nested directory structure
        data_dir = tmppath / "data"
        data_dir.mkdir()
        csv_file = data_dir / "sales.csv"
        csv_file.write_text("date,amount\n2024-01-01,100")

        app = create_test_app(tmppath, "testserver")
        client = TestClient(app)

        response = client.get("/models/mymodel.json")
        assert response.status_code == 200
        data = response.json()

        # Find the nested CSV
        csv_component = next(c for c in data["components"] if c["type"] == "csv")
        assert csv_component["name"] == "data/sales"
        assert csv_component["url"] == "http://testserver/files/data-sales.csv"

        # Test fetching the file
        response = client.get("/files/data-sales.csv")
        assert response.status_code == 200
        assert "2024-01-01,100" in response.text
