"""Integration tests for the serve command."""

import tempfile
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")

from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import PlainTextResponse  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from trilogy.scripts.serve_helpers import (  # noqa: E402
    ModelImport,
    StoreIndex,
    find_file_content_by_name,
    find_model_by_name,
    find_preql_files,
    generate_model_index,
)


def create_test_app(directory_path: Path, base_url: str = "http://testserver"):
    """Create a test FastAPI app using the same logic as serve.py."""
    app = FastAPI(title="Trilogy Model Server", version="1.0.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/")
    async def root():
        """Root endpoint with server information."""
        preql_count = len(find_preql_files(directory_path))
        return {
            "message": "Trilogy Model Server",
            "description": f"Serving {preql_count} Trilogy models from {directory_path}",
            "endpoints": {
                "/index.json": "Get list of available models",
                "/models/<model-name>.json": "Get specific model details",
            },
        }

    @app.get("/index.json", response_model=StoreIndex)
    async def get_index() -> StoreIndex:
        """Return the store index with list of available models."""
        return StoreIndex(
            name=f"Trilogy Models - {directory_path.name}",
            models=generate_model_index(directory_path, base_url),
        )

    @app.get("/models/{model_name}.json", response_model=ModelImport)
    async def get_model(model_name: str) -> ModelImport:
        """Return a specific model by name."""
        model = find_model_by_name(model_name, directory_path, base_url)
        if model is None:
            raise HTTPException(status_code=404, detail="Model not found")
        return model

    @app.get("/files/{file_name}.preql")
    async def get_file(file_name: str):
        """Return the raw .preql file content."""
        content = find_file_content_by_name(file_name, directory_path)
        if content is None:
            raise HTTPException(status_code=404, detail="File not found")
        return PlainTextResponse(content=content)

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
        assert "1 Trilogy models" in data["description"]
        assert "/index.json" in data["endpoints"]


def test_serve_index_endpoint_empty():
    """Test index endpoint with no files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/index.json")
        assert response.status_code == 200
        data = response.json()
        assert "name" in data
        assert data["models"] == []


def test_serve_index_endpoint_with_files():
    """Test index endpoint with multiple files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        file1 = tmppath / "model1.preql"
        file1.write_text("select 1;")

        subdir = tmppath / "nested"
        subdir.mkdir()
        file2 = subdir / "model2.preql"
        file2.write_text("select 2;")

        app = create_test_app(tmppath, "http://testserver")
        client = TestClient(app)

        response = client.get("/index.json")
        assert response.status_code == 200
        data = response.json()

        assert len(data["models"]) == 2
        model_names = {m["name"] for m in data["models"]}
        assert model_names == {"model1", "nested/model2"}

        # Check URLs are properly formatted
        for model in data["models"]:
            assert model["url"].startswith("http://testserver/models/")
            assert model["url"].endswith(".json")


def test_serve_get_model_not_found():
    """Test getting a model that doesn't exist returns 404."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        app = create_test_app(tmppath)
        client = TestClient(app)

        response = client.get("/models/nonexistent.json")
        assert response.status_code == 404


def test_serve_get_model_success():
    """Test getting a model successfully."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "customer.preql"
        test_file.write_text("# Customer data model\nkey customer_id int;")

        app = create_test_app(tmppath, "http://testserver")
        client = TestClient(app)

        response = client.get("/models/customer.json")
        assert response.status_code == 200
        data = response.json()

        assert data["name"] == "customer"
        assert data["description"] == "Customer data model"
        assert data["engine"] == "generic"
        assert len(data["components"]) == 1
        assert data["components"][0]["url"] == "http://testserver/files/customer.preql"
        assert data["components"][0]["type"] == "trilogy"
        assert data["components"][0]["purpose"] == "source"


def test_serve_get_model_nested():
    """Test getting a nested model."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        subdir = tmppath / "finance" / "models"
        subdir.mkdir(parents=True)
        test_file = subdir / "revenue.preql"
        test_file.write_text("# Revenue calculations\nselect sum(revenue);")

        app = create_test_app(tmppath, "http://testserver")
        client = TestClient(app)

        response = client.get("/models/finance-models-revenue.json")
        assert response.status_code == 200
        data = response.json()

        assert data["name"] == "finance/models/revenue"
        assert data["description"] == "Revenue calculations"
        assert (
            data["components"][0]["url"]
            == "http://testserver/files/finance-models-revenue.preql"
        )


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
        tmppath = Path(tmpdir)
        test_file = tmppath / "test.preql"
        test_file.write_text("select 1;")

        # Test with custom base URL
        app = create_test_app(tmppath, "https://myserver.com:9000")
        client = TestClient(app)

        response = client.get("/index.json")
        assert response.status_code == 200
        data = response.json()

        assert len(data["models"]) == 1
        assert data["models"][0]["url"] == "https://myserver.com:9000/models/test.json"


def test_serve_model_components_use_base_url():
    """Test that model components use the provided base_url correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "model.preql"
        test_file.write_text("select 1;")

        # Test with custom base URL
        app = create_test_app(tmppath, "https://myserver.com:9000")
        client = TestClient(app)

        response = client.get("/models/model.json")
        assert response.status_code == 200
        data = response.json()

        assert len(data["components"]) == 1
        assert (
            data["components"][0]["url"]
            == "https://myserver.com:9000/files/model.preql"
        )


def test_serve_with_real_test_files():
    """Test serve with actual test files from the project."""
    test_dir = Path(__file__).parent.parent / "modeling" / "aggregates"

    if not test_dir.exists():
        pytest.skip("Test modeling directory not found")

    app = create_test_app(test_dir, "http://testserver")
    client = TestClient(app)

    # Test root
    response = client.get("/")
    assert response.status_code == 200

    # Test index
    response = client.get("/index.json")
    assert response.status_code == 200
    data = response.json()
    assert len(data["models"]) > 0

    # Test getting a specific model
    model_name = data["models"][0]["name"]
    safe_name = model_name.replace("/", "-")
    response = client.get(f"/models/{safe_name}.json")
    assert response.status_code == 200

    # Test getting the file
    response = client.get(f"/files/{safe_name}.preql")
    assert response.status_code == 200
    assert len(response.text) > 0


def test_serve_localhost_url_when_host_is_0_0_0_0():
    """Test that URLs use localhost instead of 0.0.0.0 for proper resolution."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)
        test_file = tmppath / "test.preql"
        test_file.write_text("select 1;")

        # Simulate what serve.py does when host is 0.0.0.0
        host = "0.0.0.0"
        port = 8100
        url_host = "localhost" if host == "0.0.0.0" else host
        base_url = f"http://{url_host}:{port}"

        app = create_test_app(tmppath, base_url)
        client = TestClient(app)

        # Check index URLs use localhost
        response = client.get("/index.json")
        assert response.status_code == 200
        data = response.json()
        assert len(data["models"]) == 1
        assert data["models"][0]["url"] == "http://localhost:8100/models/test.json"

        # Check model component URLs use localhost
        response = client.get("/models/test.json")
        assert response.status_code == 200
        data = response.json()
        assert data["components"][0]["url"] == "http://localhost:8100/files/test.preql"
