"""Tests for the `trilogy public` command."""

import io
import json
import tempfile
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import pytest
from click.testing import CliRunner

from trilogy.scripts.trilogy import cli

SAMPLE_INDEX = {
    "count": 2,
    "files": [
        {
            "name": "bike_data",
            "filename": "bike_data.json",
            "engine": "duckdb",
            "description": "## CityBikes data",
            "tags": ["duckdb"],
        },
        {
            "name": "crypto",
            "filename": "crypto.json",
            "engine": "bigquery",
            "description": "## Crypto dataset",
            "tags": ["bigquery"],
        },
    ],
}

SAMPLE_MANIFEST = {
    "name": "bike_data",
    "engine": "duckdb",
    "description": "## CityBikes data",
    "tags": ["duckdb"],
    "components": [
        {
            "alias": "boulder_data",
            "name": "boulder_data",
            "purpose": "source",
            "type": "trilogy",
            "url": (
                "https://trilogy-data.github.io/trilogy-public-models/"
                "trilogy_public_models/duckdb/bike_data/boulder_data.preql"
            ),
        },
        {
            "alias": "setup",
            "name": "setup",
            "purpose": "setup",
            "type": "sql",
            "url": (
                "https://trilogy-data.github.io/trilogy-public-models/"
                "trilogy_public_models/duckdb/bike_data/setup.sql"
            ),
        },
        {
            "name": "sample_dashboard",
            "purpose": "example",
            "type": "dashboard",
            "url": (
                "https://trilogy-data.github.io/trilogy-public-models/"
                "examples/duckdb/bike_data/sample.json"
            ),
        },
    ],
}

FILE_CONTENTS: dict[str, bytes] = {
    "boulder_data.preql": b"key id string;\n",
    "setup.sql": b"CREATE OR REPLACE TABLE t AS SELECT 1 as id;\n",
    "sample.json": b'{"title": "demo"}\n',
}


def _fake_urlopen(url_or_req, *_args, **_kwargs):
    url = url_or_req.full_url if hasattr(url_or_req, "full_url") else url_or_req
    if url.endswith("/studio/index.json"):
        payload = json.dumps(SAMPLE_INDEX).encode("utf-8")
    elif url.endswith("/studio/bike_data.json"):
        payload = json.dumps(SAMPLE_MANIFEST).encode("utf-8")
    else:
        filename = url.rsplit("/", 1)[-1]
        if filename not in FILE_CONTENTS:
            raise HTTPError(url, 404, "Not Found", {}, None)
        payload = FILE_CONTENTS[filename]
    response = io.BytesIO(payload)
    response.__enter__ = lambda self: self  # type: ignore[attr-defined]
    response.__exit__ = lambda self, *a: None  # type: ignore[attr-defined]
    return response


@pytest.fixture
def patched_urlopen():
    with patch("trilogy.scripts.public.urlopen", side_effect=_fake_urlopen) as m:
        yield m


def test_public_list_duckdb(patched_urlopen):
    runner = CliRunner()
    result = runner.invoke(cli, ["public", "list", "--engine", "duckdb"])
    assert result.exit_code == 0, result.output
    assert "bike_data" in result.output
    assert "crypto" not in result.output


def test_public_list_no_match(patched_urlopen):
    runner = CliRunner()
    result = runner.invoke(cli, ["public", "list", "--engine", "nope"])
    assert result.exit_code == 0
    assert "No models matched" in result.output


def test_public_fetch_bike_data(patched_urlopen):
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "out"
        result = runner.invoke(cli, ["public", "fetch", "bike_data", str(target)])
        assert result.exit_code == 0, result.output
        assert (target / "boulder_data.preql").exists()
        assert (target / "setup.sql").exists()
        assert (target / "sample.json").exists()
        assert (target / "README.md").read_text().startswith("## CityBikes data")
        config = (target / "trilogy.toml").read_text()
        assert 'dialect = "duck_db"' in config
        assert '"setup.sql"' in config


def test_public_fetch_no_examples(patched_urlopen):
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "out"
        result = runner.invoke(
            cli,
            ["public", "fetch", "bike_data", str(target), "--no-examples"],
        )
        assert result.exit_code == 0, result.output
        assert (target / "boulder_data.preql").exists()
        assert not (target / "examples").exists()


def test_public_fetch_missing_model(patched_urlopen):
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "out"
        result = runner.invoke(cli, ["public", "fetch", "nonexistent", str(target)])
        assert result.exit_code == 1
        assert "No public model" in result.output


def test_public_fetch_rejects_non_empty_dir(patched_urlopen):
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "out"
        target.mkdir()
        (target / "existing.preql").write_text("select 1;")
        result = runner.invoke(cli, ["public", "fetch", "bike_data", str(target)])
        assert result.exit_code == 1
        assert "not empty" in " ".join(result.output.split())


def test_public_fetch_force_overwrites(patched_urlopen):
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        target = Path(tmpdir) / "out"
        target.mkdir()
        (target / "existing.preql").write_text("select 1;")
        result = runner.invoke(
            cli,
            ["public", "fetch", "bike_data", str(target), "--force"],
        )
        assert result.exit_code == 0, result.output
        assert (target / "boulder_data.preql").exists()


def test_public_fetch_fails_on_unreachable_component():
    def _side_effect(url_or_req, *args, **kwargs):
        url = url_or_req.full_url if hasattr(url_or_req, "full_url") else url_or_req
        if url.endswith("boulder_data.preql"):
            raise URLError("simulated network failure")
        return _fake_urlopen(url_or_req, *args, **kwargs)

    runner = CliRunner()
    with (
        patch("trilogy.scripts.public.urlopen", side_effect=_side_effect),
        patch("trilogy.scripts.public.RETRY_DELAYS", (0.0,)),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        target = Path(tmpdir) / "out"
        result = runner.invoke(cli, ["public", "fetch", "bike_data", str(target)])
        assert result.exit_code == 1, result.output
        assert "failed to fetch" in result.output
        assert not (target / "boulder_data.preql").exists()


def test_public_fetch_retries_transient_failure():
    calls = {"n": 0}

    def _side_effect(url_or_req, *args, **kwargs):
        url = url_or_req.full_url if hasattr(url_or_req, "full_url") else url_or_req
        if url.endswith("boulder_data.preql"):
            calls["n"] += 1
            if calls["n"] == 1:
                raise HTTPError(url, 429, "Too Many Requests", {}, None)
        return _fake_urlopen(url_or_req, *args, **kwargs)

    runner = CliRunner()
    with (
        patch("trilogy.scripts.public.urlopen", side_effect=_side_effect),
        patch("trilogy.scripts.public.RETRY_DELAYS", (0.0,)),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        target = Path(tmpdir) / "out"
        result = runner.invoke(cli, ["public", "fetch", "bike_data", str(target)])
        assert result.exit_code == 0, result.output
        assert calls["n"] == 2
        assert (target / "boulder_data.preql").exists()


def test_public_fetch_rejects_unsafe_name():
    runner = CliRunner()
    result = runner.invoke(cli, ["public", "fetch", "../evil"])
    assert result.exit_code == 1
    assert "Invalid model name" in result.output


def test_public_list_network_failure():
    def _fail(*_args, **_kwargs):
        raise URLError("simulated network failure")

    with (
        patch("trilogy.scripts.public.urlopen", side_effect=_fail),
        patch("trilogy.scripts.public.RETRY_DELAYS", (0.0,)),
    ):
        runner = CliRunner()
        result = runner.invoke(cli, ["public", "list"])
    assert result.exit_code == 1
    assert "Failed to fetch" in result.output


MODEL_WITH_SCRIPTS = b"""key id int;

root datasource raw (id: id)
grain (id)
address raw
freshness by `./ingest/probe.py`
refresh `ingest/build.py`;

root datasource elsewhere (id: id)
grain (id)
address elsewhere
freshness by `../outside.py`
refresh `gs://bucket/remote.py`;
"""

SECOND_MODEL = b"""import boulder_data;

root datasource other (id: id)
grain (id)
address other
freshness by `ingest/probe.py`;
"""

SCRIPTED_MANIFEST = {
    **SAMPLE_MANIFEST,
    "components": [
        *SAMPLE_MANIFEST["components"],
        {
            "alias": "denver_data",
            "name": "denver_data",
            "purpose": "source",
            "type": "trilogy",
            "url": (
                "https://trilogy-data.github.io/trilogy-public-models/"
                "trilogy_public_models/duckdb/bike_data/denver_data.preql"
            ),
        },
    ],
}


def _scripted_urlopen(requested: list[str]):
    def _side_effect(url_or_req, *args, **kwargs):
        url = url_or_req.full_url if hasattr(url_or_req, "full_url") else url_or_req
        requested.append(url)
        if url.endswith("/studio/bike_data.json"):
            return io.BytesIO(json.dumps(SCRIPTED_MANIFEST).encode("utf-8"))
        if url.endswith("/bike_data/boulder_data.preql"):
            return io.BytesIO(MODEL_WITH_SCRIPTS)
        if url.endswith("/bike_data/denver_data.preql"):
            return io.BytesIO(SECOND_MODEL)
        if url.endswith("/bike_data/ingest/probe.py"):
            return io.BytesIO(b"print('probe')\n")
        return _fake_urlopen(url_or_req, *args, **kwargs)

    return _side_effect


def test_public_fetch_ships_the_scripts_a_model_references():
    requested: list[str] = []
    runner = CliRunner()
    with (
        patch(
            "trilogy.scripts.public.urlopen",
            side_effect=_scripted_urlopen(requested),
        ),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        target = Path(tmpdir) / "out"
        result = runner.invoke(cli, ["public", "fetch", "bike_data", str(target)])

        assert result.exit_code == 0, result.output
        assert (target / "ingest" / "probe.py").read_bytes() == b"print('probe')\n"
        assert not (target / "ingest" / "build.py").exists()
        assert "not published: ingest/build.py" in result.output
    assert len([u for u in requested if u.endswith("/ingest/probe.py")]) == 1
    assert not [u for u in requested if "outside.py" in u or "remote.py" in u]


def test_public_fetch_force_replaces_a_stale_script():
    runner = CliRunner()
    with (
        patch("trilogy.scripts.public.urlopen", side_effect=_scripted_urlopen([])),
        tempfile.TemporaryDirectory() as tmpdir,
    ):
        target = Path(tmpdir) / "out"
        (target / "ingest").mkdir(parents=True)
        (target / "ingest" / "probe.py").write_bytes(b"print('stale')\n")

        result = runner.invoke(
            cli, ["public", "fetch", "bike_data", str(target), "--force"]
        )

        assert result.exit_code == 0, result.output
        assert (target / "ingest" / "probe.py").read_bytes() == b"print('probe')\n"
