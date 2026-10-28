"""Datasource `order by` and `storage (...)`: parsed on both backends, inherited
through `extends`, and kept by every path that writes a file target."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects, Executor
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.environment import Environment
from trilogy.parsing.render import Renderer
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.model import HydrationError
from trilogy.parsing.v2.pest_backend import parse_pest

HEADER = """
import std.geography;
key id int;
property id.lat float;
property id.lon float;
"""

ROWS = 50_000
# A ~20 km square of points: a city's worth of trees.
RAW = f"""
CREATE TABLE raw AS SELECT i AS id,
    42.2 + (hash(i) % 100000) / 100000.0 * 0.2 AS lat,
    -71.2 + (hash(i * 7 + 3) % 100000) / 100000.0 * 0.2 AS lon
FROM range({ROWS}) t(i)
"""


def hilbert(lat: float, lon: float) -> int:
    """The textbook xy-to-index walk over a 2^16 grid of the globe."""
    side = 1 << 16
    x = min(max(int((lon + 180) / 360 * side), 0), side - 1)
    y = min(max(int((lat + 90) / 180 * side), 0), side - 1)
    index, s = 0, side // 2
    while s:
        rx, ry = int(bool(x & s)), int(bool(y & s))
        index += s * s * ((3 * rx) ^ ry)
        if not ry:
            if rx:
                x, y = (side - 1) ^ x, (side - 1) ^ y
            x, y = y, x
        s //= 2
    return index


def _model(target: str, trailer: str = "") -> str:
    return (
        HEADER
        + "root datasource raw (id: id, lat: lat, lon: lon) grain (id) address raw;\n"
        + "template datasource points_base (id: id, lat: lat, lon: lon)\ngrain (id)\n"
        + "order by @hilbert_index(lat, lon) asc\nstorage (row_group_size = 2048);\n"
        + f"datasource points extends points_base () file `{target}`{trailer};"
    )


def _executor(tmp_path: Path, target: Path, trailer: str = "") -> Executor:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.execute_raw_sql(RAW)
    executor.parse_text(_model(target.as_posix(), trailer))
    return executor


def _row_groups(target: Path) -> list[tuple[float, float, float, float]]:
    """Each row group's (lat min, lat max, lon min, lon max) from the footer."""
    rows = duckdb.sql(
        f"""SELECT row_group_id, path_in_schema, stats_min_value, stats_max_value
        FROM parquet_metadata('{target.as_posix()}')"""
    ).fetchall()
    groups: dict[int, dict[str, tuple[float, float]]] = {}
    for group, column, low, high in rows:
        groups.setdefault(group, {})[column] = (float(low), float(high))
    return [(*g["lat"], *g["lon"]) for _, g in sorted(groups.items())]


def _overlapping(groups: list[tuple[float, float, float, float]]) -> int:
    """Row groups a reader must fetch for a ~1 km box in the middle."""
    lat0, lat1, lon0, lon1 = 42.295, 42.305, -71.105, -71.095
    return sum(
        1
        for a, b, c, d in groups
        if a <= lat1 and b >= lat0 and c <= lon1 and d >= lon0
    )


def _assert_sorted_and_pruneable(target: Path) -> None:
    columns = [
        r[0]
        for r in duckdb.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{target.as_posix()}')"
        ).fetchall()
    ]
    assert columns == ["id", "lat", "lon"]
    rows = duckdb.sql(f"""SELECT lat, lon FROM
        read_parquet('{target.as_posix()}', file_row_number = true)
        ORDER BY file_row_number""").fetchall()
    keys = [hilbert(lat, lon) for lat, lon in rows]
    assert len(keys) == ROWS
    assert keys == sorted(keys)
    groups = _row_groups(target)
    assert len(groups) == -(-ROWS // 2048)
    assert _overlapping(groups) <= len(groups) // 5


class TestSyntax:
    @pytest.mark.parametrize("backend", [parse_lark, parse_pest])
    def test_parses_on_both_backends(self, backend) -> None:
        backend(_model("points.parquet"))

    def test_extends_inherits_and_overrides(self) -> None:
        env = Environment()
        env.parse(
            _model("points.parquet")
            + "\ndatasource other extends points_base () file `other.parquet`"
            + "\norder by lat desc\nstorage (row_group_size = 4096);"
        )
        points, other = env.datasources["points"], env.datasources["other"]
        assert points.storage == {"row_group_size": 2048}
        assert points.order_by is not None
        assert Renderer().to_string(points.order_by.items[0]) == (
            "@hilbert_index(lat, lon) asc"
        )
        assert other.storage == {"row_group_size": 4096}
        assert other.order_by is not None
        assert Renderer().to_string(other.order_by.items[0]) == "lat desc"

    def test_round_trips(self) -> None:
        env = Environment()
        env.parse(_model("points.parquet"))
        rendered = Renderer().to_string(env.datasource_templates["points_base"])
        assert "order by @hilbert_index(lat, lon) asc" in rendered
        assert "storage (row_group_size = 2048)" in rendered
        again = Environment()
        again.parse(HEADER + rendered)
        assert again.datasource_templates["points_base"].storage == {
            "row_group_size": 2048
        }

    @pytest.mark.parametrize(
        "trailer, message",
        [
            ("file `p.csv` storage (row_group_size = 10)", "not an option for a csv"),
            (
                "file `p.parquet` storage (page_size = 10)",
                "not an option for a parquet",
            ),
            ("file `p.parquet` storage (row_group_size = 'x')", "must be int"),
            ("file `p.parquet` storage (row_group_size = 0)", "must be positive"),
            (
                "file `p.parquet` storage (row_group_size = 1, row_group_size = 2)",
                "sets 'row_group_size' twice",
            ),
            ("address points order by lat", "needs a file target"),
            ("file `p.parquet` order by elsewhere", "neither store nor derive"),
        ],
    )
    def test_rejects(self, trailer: str, message: str) -> None:
        env = Environment()
        with pytest.raises((InvalidSyntaxException, HydrationError), match=message):
            env.parse(
                HEADER
                + "key elsewhere int;\n"
                + f"datasource plain (id: id, lat: lat) grain (id) {trailer};"
            )

    def test_changes_the_fingerprint(self) -> None:
        from trilogy.core.fingerprint import _Canonicalizer

        def digest(trailer: str) -> str:
            env = Environment()
            env.parse(
                HEADER
                + f"datasource plain (id: id, lat: lat) grain (id) file `p.parquet` {trailer};"
            )
            return _Canonicalizer(env, deep=False).datasource_hash(
                env.datasources["plain"]
            )

        assert (
            len({digest(""), digest("order by lat"), digest("order by lat desc")}) == 3
        )
        assert digest("storage (row_group_size = 1)") != digest(
            "storage (row_group_size = 2)"
        )


class TestHilbertIndex:
    def test_matches_the_reference_curve(self) -> None:
        points = [(-90.0, -180.0), (90.0, 180.0), (0.0, 0.0), (42.3, -71.1)] + [
            (((i * 7919) % 18000) / 100 - 90, ((i * 104729) % 36000) / 100 - 180)
            for i in range(300)
        ]
        values = ", ".join(f"({lat}, {lon})" for lat, lon in points)
        executor = Dialects.DUCK_DB.default_executor()
        executor.parse_text(
            HEADER
            + "datasource points (id: id, lat: lat, lon: lon) grain (id) query '''"
            + f"select row_number() over () as id, lat, lon from (values {values}) t(lat, lon)"
            + "''';"
        )
        rows = executor.execute_text(
            "select id, lat, lon, @hilbert_index(lat, lon) -> key;"
        )[-1].fetchall()
        assert len(rows) == len(points)
        assert [r.key for r in rows] == [hilbert(r.lat, r.lon) for r in rows]

    def test_a_missing_coordinate_is_null(self) -> None:
        executor = Dialects.DUCK_DB.default_executor()
        executor.parse_text(
            HEADER
            + "datasource points (id: id, lat: lat, lon: lon) grain (id) query '''"
            + "select 1 as id, null::double as lat, 1.0::double as lon'''"
            + ";"
        )
        rows = executor.execute_text("select id, @hilbert_index(lat, lon) -> key;")[
            -1
        ].fetchall()
        assert rows[0].key is None


class TestBuild:
    def test_sorted_into_small_row_groups(self, tmp_path: Path) -> None:
        target = tmp_path / "points.parquet"
        executor = _executor(tmp_path, target)
        executor.update_datasource(executor.environment.datasources["points"])
        _assert_sorted_and_pruneable(target)

    def test_through_the_local_stage(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("trilogy.executor.staged_write", _unstageable)
        target = tmp_path / "points.parquet"
        executor = _executor(tmp_path, target, "\nassert count(id) >= 1")
        executor.update_datasource(executor.environment.datasources["points"])
        _assert_sorted_and_pruneable(target)

    def test_an_append_sorts_the_whole_file(self, tmp_path: Path) -> None:
        target = tmp_path / "points.parquet"
        executor = _executor(tmp_path, target)
        executor.execute_text(
            f"append into points from where id < {ROWS // 2} select id, lat, lon;"
        )
        executor.execute_text(
            f"append into points from where id >= {ROWS // 2} select id, lat, lon;"
        )
        _assert_sorted_and_pruneable(target)

    def test_a_csv_target_is_sorted(self, tmp_path: Path) -> None:
        target = tmp_path / "points.csv"
        executor = Dialects.DUCK_DB.default_executor(
            environment=Environment(working_path=tmp_path)
        )
        executor.execute_raw_sql(RAW)
        executor.parse_text(
            HEADER
            + "root datasource raw (id: id, lat: lat, lon: lon) grain (id) address raw;\n"
            + "datasource points (id: id, lat: lat) grain (id)"
            + f" file `{target.as_posix()}` order by lat desc;"
        )
        executor.update_datasource(executor.environment.datasources["points"])
        lats = [
            r[0]
            for r in duckdb.sql(f"SELECT lat FROM '{target.as_posix()}'").fetchall()
        ]
        assert lats == sorted(lats, reverse=True)

    @pytest.mark.parametrize("preserve", [True, False])
    def test_a_csv_target_through_the_local_stage(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, preserve: bool
    ) -> None:
        monkeypatch.setattr("trilogy.executor.staged_write", _unstageable)
        target = tmp_path / "points.csv"
        executor = Dialects.DUCK_DB.default_executor(
            environment=Environment(working_path=tmp_path)
        )
        executor.execute_raw_sql(RAW)
        executor.execute_raw_sql(f"SET preserve_insertion_order = {preserve}")
        executor.parse_text(
            HEADER
            + "root datasource raw (id: id, lat: lat, lon: lon) grain (id) address raw;\n"
            + "datasource points (id: id, lat: lat) grain (id)"
            + f" file `{target.as_posix()}` order by lat desc assert count(id) >= 1;"
        )
        points = executor.environment.datasources["points"]
        if not preserve:
            with pytest.raises(ValueError, match="preserve_insertion_order"):
                executor.update_datasource(points)
            return
        executor.update_datasource(points)
        lats = [
            r[0]
            for r in duckdb.sql(f"SELECT lat FROM '{target.as_posix()}'").fetchall()
        ]
        assert lats == sorted(lats, reverse=True)


def test_storage_values_render_as_literals() -> None:
    from trilogy.executor import _copy_option_value
    from trilogy.parsing.render import _storage_literal

    assert [_storage_literal(v) for v in (True, 3, 1.5, "it's")] == [
        "true",
        "3",
        "1.5",
        "'it\\'s'",
    ]
    assert [_copy_option_value(v) for v in (False, 3, "it's")] == [
        "false",
        "3",
        "'it''s'",
    ]


def test_plan_ephemeral_wants_one_select() -> None:
    executor = Dialects.DUCK_DB.default_executor()
    with pytest.raises(ValueError, match="one select"):
        executor.plan_ephemeral("select 1 -> a; select 2 -> b;")


@contextmanager
def _unstageable(target: str, staging_root: str | None = None) -> Iterator[str]:
    yield target
