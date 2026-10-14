import os
import sys
from pathlib import Path

import pytest

from trilogy import Dialects, Environment
from trilogy.constants import CONFIG, ParserBackend
from trilogy.core.enums import AddressType
from trilogy.dialect.config import DuckDBConfig
from trilogy.dialect.duckdb import python_datasource_failure
from trilogy.parsing.render import Renderer

ROOT = Path(__file__).parents[3]
SUFFIX = ".exe" if sys.platform == "win32" else ""
LANDMARKS = (
    ROOT / "crates" / "trilogy-io" / "target" / "release" / "examples" / "landmarks"
)

MODEL = """
key id int;
property id.name string;
property id.state string;

datasource landmarks (id: id, name: name, state: state)
grain (id)
file `{path}`;
"""


@pytest.fixture(params=list(ParserBackend), ids=lambda b: b.value)
def backend(request, monkeypatch):
    monkeypatch.setattr(CONFIG, "parser_backend", request.param)
    return request.param


@pytest.mark.parametrize(
    "path, expected",
    [
        ("./target/release/landmarks", AddressType.EXECUTABLE),
        ("C:/tools/bin/landmarks.exe", AddressType.EXECUTABLE),
        ("../bin/landmarks", AddressType.EXECUTABLE),
        ("./data/x.parquet", AddressType.PARQUET),
        ("./s.py", AddressType.PYTHON_SCRIPT),
    ],
)
def test_a_program_path_parses_as_an_executable(backend, path, expected):
    env = Environment()
    env.parse(f"key id int; datasource d (id: id) grain (id) file `{path}`;")
    assert env.datasources["d"].address.type == expected
    assert env.datasources["d"].address.type.is_program == (
        expected != AddressType.PARQUET
    )


def test_backtick_identifiers_are_not_programs(backend):
    env = Environment()
    env.parse("key id int; datasource d (`my col`: id) grain (id) address t;")
    assert env.datasources["d"].address.type == AddressType.TABLE


def test_an_executable_address_round_trips():
    env = Environment()
    env.parse("key id int; datasource d (id: id) grain (id) file `./bin/landmarks`;")
    rendered = Renderer().to_string(env.datasources["d"])
    reparsed = Environment()
    reparsed.parse(f"key id int; {rendered}")
    assert reparsed.datasources["d"].address == env.datasources["d"].address


def test_unix_pipe_failure_names_an_executable():
    error = python_datasource_failure(
        Exception(
            "Pipe process exited abnormally code=65: "
            "/w/bin/landmarks --limit 5 2>'/tmp/x.err' |"
        )
    )
    assert error is not None
    assert error.script == "/w/bin/landmarks"


@pytest.fixture
def landmarks() -> Path:
    binary = LANDMARKS.with_suffix(SUFFIX) if SUFFIX else LANDMARKS
    if not binary.exists():
        if os.environ.get("TRILOGY_IO_PREBUILT"):
            pytest.fail(f"TRILOGY_IO_PREBUILT is set but {binary} is missing")
        pytest.skip("trilogy-io landmarks example not built")
    # Declared without the extension: Windows resolves it to the .exe.
    return LANDMARKS


def test_a_rust_binary_is_a_datasource(landmarks: Path):
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(),
        conf=DuckDBConfig(enable_python_datasources=True),
    )
    executor.parse_text(MODEL.format(path=landmarks.as_posix()))
    rows = executor.execute_text(
        "where state = 'NY' select id, name order by id desc limit 3;"
    )[-1].fetchall()

    assert rows == [(97, "landmark-97"), (93, "landmark-93"), (89, "landmark-89")]
