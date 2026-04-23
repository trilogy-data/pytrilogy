import pytest
from pydantic import ValidationError

from trilogy.dialect.enums import Dialects
from trilogy.scripts.serve_helpers.models import (
    ConnectionSpec,
    ImportFile,
    ModelImport,
    StoreIndex,
    StoreModelIndex,
)


def test_import_file_minimal():
    """Test creating ImportFile with minimal required fields."""
    file = ImportFile(
        url="http://example.com/file.preql", name="test", purpose="source"
    )
    assert file.url == "http://example.com/file.preql"
    assert file.name == "test"
    assert file.alias == ""
    assert file.purpose == "source"
    assert file.type is None


def test_import_file_with_all_fields():
    """Test creating ImportFile with all fields."""
    file = ImportFile(
        url="http://example.com/file.preql",
        name="test",
        alias="test_alias",
        purpose="source",
        type="trilogy",
    )
    assert file.url == "http://example.com/file.preql"
    assert file.name == "test"
    assert file.alias == "test_alias"
    assert file.purpose == "source"
    assert file.type == "trilogy"


def test_model_import_minimal():
    """Test creating ModelImport with minimal required fields."""
    component = ImportFile(
        url="http://example.com/file.preql", name="test", purpose="source"
    )
    model = ModelImport(
        name="test_model",
        engine="duckdb",
        description="Test model",
        components=[component],
    )
    assert model.name == "test_model"
    assert model.engine == "duckdb"
    assert model.description == "Test model"
    assert model.link == ""
    assert model.tags == []
    assert len(model.components) == 1


def test_model_import_with_all_fields():
    """Test creating ModelImport with all fields."""
    component = ImportFile(
        url="http://example.com/file.preql", name="test", purpose="source"
    )
    model = ModelImport(
        name="test_model",
        engine="bigquery",
        description="Test model",
        link="https://github.com/example/repo",
        tags=["analytics", "reporting"],
        components=[component],
    )
    assert model.name == "test_model"
    assert model.engine == "bigquery"
    assert model.description == "Test model"
    assert model.link == "https://github.com/example/repo"
    assert model.tags == ["analytics", "reporting"]
    assert len(model.components) == 1


def test_model_import_multiple_components():
    """Test ModelImport with multiple components."""
    components = [
        ImportFile(
            url="http://example.com/file1.preql", name="test1", purpose="source"
        ),
        ImportFile(
            url="http://example.com/file2.preql", name="test2", purpose="source"
        ),
    ]
    model = ModelImport(
        name="multi_component",
        engine="generic",
        description="Multi",
        components=components,
    )
    assert len(model.components) == 2


def test_store_model_index():
    """Test creating StoreModelIndex."""
    index = StoreModelIndex(
        name="test_model", url="http://example.com/models/test.json"
    )
    assert index.name == "test_model"
    assert index.url == "http://example.com/models/test.json"


def test_store_index_empty():
    """Test creating empty StoreIndex."""
    store = StoreIndex(name="Test Store", models=[])
    assert store.name == "Test Store"
    assert store.models == []


def test_store_index_with_models():
    """Test creating StoreIndex with models."""
    models = [
        StoreModelIndex(name="model1", url="http://example.com/models/model1.json"),
        StoreModelIndex(name="model2", url="http://example.com/models/model2.json"),
    ]
    store = StoreIndex(name="Test Store", models=models)
    assert store.name == "Test Store"
    assert len(store.models) == 2
    assert store.models[0].name == "model1"
    assert store.models[1].name == "model2"


def test_connection_spec_minimal():
    spec = ConnectionSpec(type="duckdb")
    assert spec.type == Dialects.DUCK_DB
    assert spec.options == {}


def test_connection_spec_with_options():
    spec = ConnectionSpec(
        type="snowflake", options={"account": "acme", "warehouse": "wh"}
    )
    assert spec.type == Dialects.SNOWFLAKE
    assert spec.options == {"account": "acme", "warehouse": "wh"}


@pytest.mark.parametrize(
    "dialect",
    list(Dialects),
)
def test_connection_spec_accepts_all_dialects(dialect):
    assert ConnectionSpec(type=dialect.value).type == dialect


def test_connection_spec_accepts_duckdb_alias():
    assert ConnectionSpec(type="duckdb").type == Dialects.DUCK_DB


def test_connection_spec_rejects_unknown_type():
    with pytest.raises(ValidationError):
        ConnectionSpec(type="not_a_dialect")  # type: ignore[arg-type]


def test_store_index_with_connection():
    store = StoreIndex(
        name="Remote Store",
        models=[],
        connection=ConnectionSpec(type="duckdb"),
    )
    assert store.connection is not None
    assert store.connection.type == Dialects.DUCK_DB
    assert store.model_dump(mode="json")["connection"] == {
        "type": "duck_db",
        "options": {},
    }


def test_store_index_connection_defaults_to_none():
    store = StoreIndex(name="Remote Store", models=[])
    assert store.connection is None


def test_models_are_json_serializable():
    """Test that models can be serialized to JSON."""
    component = ImportFile(
        url="http://example.com/file.preql",
        name="test",
        alias="",
        purpose="source",
        type="trilogy",
    )
    model = ModelImport(
        name="test_model",
        engine="generic",
        description="Test",
        components=[component],
    )
    index = StoreModelIndex(name="test", url="http://example.com/test.json")
    store = StoreIndex(name="Store", models=[index])

    # These should not raise exceptions
    assert component.model_dump() is not None
    assert model.model_dump() is not None
    assert index.model_dump() is not None
    assert store.model_dump() is not None
