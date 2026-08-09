from pathlib import Path

import pyarrow as pa

from trilogy.io.sinks import Format, normalize_object_uri, open_uri_sink, write

TABLE = pa.Table.from_pylist([{"i": i} for i in range(3)])


def test_gcs_scheme_is_normalized_to_the_one_pyarrow_registers():
    assert normalize_object_uri("gcs://bucket/key") == "gs://bucket/key"
    assert normalize_object_uri("gs://bucket/key") == "gs://bucket/key"
    assert normalize_object_uri("s3://bucket/key") == "s3://bucket/key"
    assert normalize_object_uri("/local/path") == "/local/path"


def test_a_local_sink_creates_the_parent_of_a_nested_path(tmp_path: Path):
    target = tmp_path / "a" / "b" / "out.arrow"
    assert write(TABLE.to_reader(), Format.ARROW, str(target)) == 3
    assert target.exists()


def test_open_uri_sink_returns_a_reusable_factory(tmp_path: Path):
    factory = open_uri_sink(str(tmp_path / "out.bin"))
    for payload in (b"first", b"second"):
        with factory() as stream:
            stream.write(payload)
    assert (tmp_path / "out.bin").read_bytes() == b"second"
