"""The python and rust implementations must be indistinguishable to a consumer.

The libraries are conveniences; the command line is the contract. These run the
same flags against `tests/io/conformance/landmarks.py` and the `landmarks`
example in `crates/trilogy-io` and require identical answers.

Skipped when cargo is unavailable, and the rust binary is built once per session.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

ROOT = Path(__file__).parents[2]
CRATE = ROOT / "crates" / "trilogy-io"
PY_SOURCE = Path(__file__).parent / "conformance" / "landmarks.py"

CASES = [
    [],
    ["--limit", "5"],
    ["--columns", "id,state"],
    ["--filter", "state = CA"],
    ["--filter", 'state in ["CA","NY"]'],
    ["--filter", "id >= 90"],
    ["--filter", "name like landmark-1%"],
    ["--limit", "4", "--filter", "state = CA"],
    ["--limit", "3", "--filter", "state = CA", "--columns", "id,state"],
    ["--filter", 'state not in ["CA"]', "--limit", "6"],
    ["--order-by", "id:desc", "--limit", "5"],
    ["--order-by", "state:asc,id:desc", "--limit", "8"],
    ["--order-by", "name:asc", "--limit", "7"],
    ["--filter", "state = CA", "--order-by", "id:desc", "--limit", "4"],
]


@pytest.fixture(scope="session")
def rust_binary() -> str:
    if shutil.which("cargo") is None:
        pytest.skip("cargo not available")
    build = subprocess.run(
        ["cargo", "build", "--release", "--example", "landmarks"],
        cwd=CRATE,
        capture_output=True,
        check=False,
    )
    if build.returncode != 0:
        pytest.skip(
            f"cargo build failed: {build.stderr.decode(errors='replace')[-2000:]}"
        )
    suffix = ".exe" if sys.platform == "win32" else ""
    return str(CRATE / "target" / "release" / "examples" / f"landmarks{suffix}")


def run_python(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(PY_SOURCE), *args],
        capture_output=True,
        check=False,
        cwd=ROOT,
    )


def run_rust(binary: str, args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run([binary, *args], capture_output=True, check=False)


def rows(stdout: bytes) -> list[dict]:
    with pa.ipc.open_stream(stdout) as reader:
        return reader.read_all().to_pylist()


@pytest.mark.parametrize("args", CASES, ids=lambda a: " ".join(a) or "no-flags")
def test_both_implementations_return_the_same_rows(rust_binary: str, args: list[str]):
    py = run_python(args)
    rs = run_rust(rust_binary, args)
    assert py.returncode == 0, py.stderr.decode(errors="replace")
    assert rs.returncode == 0, rs.stderr.decode(errors="replace")
    assert rows(py.stdout) == rows(rs.stdout)


def test_both_report_the_same_schema(rust_binary: str):
    with pa.ipc.open_stream(run_python([]).stdout) as reader:
        py_schema = reader.schema
    with pa.ipc.open_stream(run_rust(rust_binary, []).stdout) as reader:
        rs_schema = reader.schema
    assert py_schema.names == rs_schema.names
    assert py_schema.types == rs_schema.types


def test_both_stamp_the_same_metadata(rust_binary: str):
    def metadata(stdout: bytes) -> dict:
        with pa.ipc.open_stream(stdout) as reader:
            return {
                k.decode(): v.decode()
                for k, v in (reader.schema.metadata or {}).items()
            }

    assert metadata(run_python([]).stdout) == metadata(run_rust(rust_binary, []).stdout)


def test_both_withdraw_limit_pushdown_when_filters_are_local(rust_binary: str):
    """Both claim `limit` only; a local filter must move it to the fallback."""
    args = ["--limit", "4", "--filter", "state = CA"]
    for stdout in (run_python(args).stdout, run_rust(rust_binary, args).stdout):
        result = rows(stdout)
        assert len(result) == 4
        assert {row["state"] for row in result} == {"CA"}


def test_both_describe_identically(rust_binary: str):
    py = json.loads(run_python(["--describe"]).stdout)
    rs = json.loads(run_rust(rust_binary, ["--describe"]).stdout)
    assert py["contract"] == rs["contract"]
    assert py["schema"] == rs["schema"]
    assert py["pushdown"] == rs["pushdown"]
    # The datasource stub differs only in the path each was invoked by.
    assert py["datasource"].split("file `")[0] == rs["datasource"].split("file `")[0]


def test_describe_ignores_narrowing_flags(rust_binary: str):
    """Describe reports what the source produces, not what a request returns."""
    args = ["--describe", "--columns", "id", "--limit", "1"]
    py = json.loads(run_python(args).stdout)
    rs = json.loads(run_rust(rust_binary, args).stdout)
    assert [f["name"] for f in py["schema"]] == ["id", "name", "state"]
    assert py["schema"] == rs["schema"]


def test_both_fail_the_same_way(rust_binary: str):
    args = ["--filter", "nonexistent = 1"]
    py = run_python(args)
    rs = run_rust(rust_binary, args)
    assert py.returncode == rs.returncode == 65

    def reported(stderr: bytes) -> dict:
        line = next(
            line
            for line in stderr.decode(errors="replace").splitlines()
            if line.startswith("trilogy-io-error: ")
        )
        return json.loads(line[len("trilogy-io-error: ") :])

    py_error, rs_error = reported(py.stderr), reported(rs.stderr)
    assert py_error["type"] == rs_error["type"] == "ContractError"
    assert py_error["retryable"] == rs_error["retryable"] is False
    assert "Available columns" in py_error["message"]
    assert "Available columns" in rs_error["message"]


def test_both_reject_a_malformed_partition_identically(rust_binary: str):
    """A bad flag value is a contract failure on both sides, not a parser quirk."""
    args = ["--partition", "day"]
    py, rs = run_python(args), run_rust(rust_binary, args)
    assert py.returncode == rs.returncode == 65
    for result in (py, rs):
        stderr = result.stderr.decode(errors="replace")
        line = next(
            line
            for line in stderr.splitlines()
            if line.startswith("trilogy-io-error: ")
        )
        detail = json.loads(line[len("trilogy-io-error: ") :])
        assert detail["type"] == "ContractError"
        assert "--partition expects KEY=VALUE" in detail["message"]


def test_tie_order_is_unspecified_but_the_keys_agree(rust_binary: str):
    """`state` repeats every four rows, so sorting on it alone is all ties.

    pyarrow sorts stably; arrow-rs's `lexsort_to_indices` does not, so the two
    return different -- equally valid -- members of each tie group. That is the
    same non-determinism SQL has for an incomplete ORDER BY, and it is why
    `source_pushdown` warns that pushing an ordered limit can change *which*
    tied rows come back. What must agree is the ordering itself.
    """
    args = ["--order-by", "state:asc", "--limit", "12"]
    py = [row["state"] for row in rows(run_python(args).stdout)]
    rs = [row["state"] for row in rows(run_rust(rust_binary, args).stdout)]
    assert py == rs
    assert py == sorted(py)


def test_a_total_ordering_agrees_exactly(rust_binary: str):
    """With a unique tiebreaker there is one right answer, and both give it."""
    args = ["--order-by", "state:asc,id:desc", "--limit", "12"]
    assert rows(run_python(args).stdout) == rows(run_rust(rust_binary, args).stdout)


def test_a_limit_under_an_ordering_returns_the_top_rows(rust_binary: str):
    args = ["--order-by", "id:desc", "--limit", "3"]
    for stdout in (run_python(args).stdout, run_rust(rust_binary, args).stdout):
        assert [row["id"] for row in rows(stdout)] == [99, 98, 97]


@pytest.mark.parametrize("fmt", ["csv", "json"])
def test_text_formats_match(rust_binary: str, fmt: str):
    args = ["--limit", "3", "--format", fmt]
    assert run_python(args).stdout == run_rust(rust_binary, args).stdout
