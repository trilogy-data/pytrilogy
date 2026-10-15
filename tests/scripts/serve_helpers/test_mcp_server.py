from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("mcp")

from fastapi import FastAPI
from fastapi.testclient import TestClient
from mcp.server.mcpserver.exceptions import ToolError

from trilogy import Dialects
from trilogy.scripts.common import get_runtime_config
from trilogy.scripts.serve import create_app
from trilogy.scripts.serve_helpers.mcp_server import ModelSession

ITEMS = """# Items for sale.
key id int;
property id.label string;

datasource items (id: id, label: label)
grain (id)
query '''select * from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(id, label)''';
"""

INIT = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "1"},
    },
}
MCP_HEADERS = {"Accept": "application/json, text/event-stream"}


@pytest.fixture
def model_dir(tmp_path: Path) -> Path:
    (tmp_path / "trilogy.toml").write_text('[engine]\ndialect = "duck_db"\n')
    (tmp_path / "items.preql").write_text(ITEMS)
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "more.preql").write_text("import items;\n")
    return tmp_path


@pytest.fixture
def session(model_dir: Path) -> ModelSession:
    return ModelSession(model_dir, Dialects.DUCK_DB, get_runtime_config(model_dir))


def test_list_models(session: ModelSession):
    assert session.list_models() == {
        "models": [
            {"name": "items", "description": "Items for sale."},
            {"name": "nested.more", "description": "Trilogy model: more"},
        ]
    }


def test_describe_model(session: ModelSession):
    described = session.describe_model("items")
    assert "id int" in described and "label string" in described
    with pytest.raises(ToolError, match="Unknown model"):
        session.describe_model("../items")


def test_run_query_caps_rows(session: ModelSession):
    result = session.run_query(
        "import items; select id, label order by id asc;", limit=2
    )
    assert result == {
        "columns": ["id", "label"],
        "rows": [[1, "a"], [2, "b"]],
        "truncated": True,
    }


def test_run_query_isolates_definitions(session: ModelSession):
    defined = session.run_query(
        "import items; auto doubled <- id * 2; select doubled order by doubled asc;"
    )
    assert defined["rows"] == [[2], [4], [6]]
    with pytest.raises(ToolError, match="doubled"):
        session.run_query("import items; select doubled;")


def test_run_query_without_rows(session: ModelSession):
    assert session.run_query("import items;") == {
        "columns": [],
        "rows": [],
        "truncated": False,
    }


def test_get_syntax(session: ModelSession):
    assert "get_syntax(example=<name>)" in session.get_syntax()
    with pytest.raises(ToolError, match="No example"):
        session.get_syntax("not-an-example")


def _app(
    model_dir: Path, session: ModelSession, token: str | None, host: str = "testserver"
) -> FastAPI:
    return create_app(
        FastAPI(),
        "duck_db",
        model_dir,
        host,
        8100,
        token=token,
        mcp_session=session,
    )


def test_mcp_route_requires_token_beyond_localhost(
    model_dir: Path, session: ModelSession
):
    with TestClient(_app(model_dir, session, "secret")) as client:
        assert client.post("/mcp", json=INIT, headers=MCP_HEADERS).status_code == 401
        for header in (
            {"X-Trilogy-Token": "secret"},
            {"Authorization": "Bearer secret"},
        ):
            response = client.post("/mcp", json=INIT, headers=MCP_HEADERS | header)
            assert response.status_code == 200
            assert "serverInfo" in response.text


def test_mcp_route_is_open_on_localhost(model_dir: Path, session: ModelSession):
    app = _app(model_dir, session, "secret", host="localhost")
    with TestClient(app, base_url="http://localhost:8100") as client:
        response = client.post("/mcp", json=INIT, headers=MCP_HEADERS)
        assert response.status_code == 200
        assert client.get("/index.json").status_code == 401


def test_mcp_on_localhost_rejects_rebound_host(model_dir: Path, session: ModelSession):
    app = _app(model_dir, session, None, host="localhost")
    with TestClient(app, base_url="http://attacker.example:8100") as client:
        response = client.post("/mcp", json=INIT, headers=MCP_HEADERS)
        assert response.status_code in (400, 403, 421)


def test_mcp_route_without_auth(model_dir: Path, session: ModelSession):
    with TestClient(_app(model_dir, session, None)) as client:
        response = client.post("/mcp", json=INIT, headers=MCP_HEADERS)
        assert response.status_code == 200
