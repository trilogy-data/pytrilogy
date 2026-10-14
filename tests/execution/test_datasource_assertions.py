"""Datasource `assert` clauses: parsed on both backends, round-tripped by the
renderer, and enforced between a build's write and its swap into the asset."""

from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects, Executor
from trilogy.core.assertions import (
    AssertionSpecError,
    map_expression,
    plan_assertion,
    substitute,
)
from trilogy.core.enums import FunctionType
from trilogy.core.exceptions import InvalidSyntaxException
from trilogy.core.models.author import ConceptRef, Function
from trilogy.core.models.environment import Environment
from trilogy.execution.assertions import DatasourceAssertionError
from trilogy.execution.staged_write import STAGING_DIR
from trilogy.parsing.render import Renderer
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.model import HydrationError
from trilogy.parsing.v2.pest_backend import parse_pest

HEADER = """
key id int;
property id.source string;
"""

ASSERTS = """
assert count(id) >= 0.75 * prior(count(id))
assert count(id) by source >= 0.75 * prior(count(id) by source)
assert count(id ? source = 'A') >= 100
"""


def _model(target: str) -> str:
    return (
        HEADER
        + "root datasource raw (id: id, source: source) grain (id) address raw;\n"
        + "datasource built (id: id, source: source)\ngrain (id)\n"
        + target
        + "\nfreshness by id"
        + ASSERTS
        + ";"
    )


def _parse_datasource(trailer: str):
    env = Environment()
    env.parse(
        HEADER
        + "datasource plain (id: id, source: source) grain (id) address plain "
        + trailer
        + ";"
    )
    return env.datasources["plain"]


class TestSyntax:
    @pytest.mark.parametrize("backend", [parse_lark, parse_pest])
    def test_parses_on_both_backends(self, backend) -> None:
        backend(_model("address built"))

    def test_hydrates_in_order(self) -> None:
        ds = _parse_datasource(ASSERTS)
        rendered = [Renderer().to_string(a.condition) for a in ds.assertions]
        assert rendered == [
            "count(id) >= 0.75 * prior(count(id))",
            "count(id) by source >= 0.75 * prior(count(id) by source)",
            "count(id ? source = 'A') >= 100",
        ]
        assert ds.assertions[0].line_no is not None

    def test_round_trips(self) -> None:
        rendered = Renderer().to_string(_parse_datasource(ASSERTS))
        env = Environment()
        env.parse(HEADER + rendered)
        again = env.datasources["plain"]
        assert [str(a) for a in again.assertions] == [
            str(a) for a in _parse_datasource(ASSERTS).assertions
        ]

    def test_namespaced_import_keeps_assertions(self, tmp_path: Path) -> None:
        (tmp_path / "plain.preql").write_text(
            HEADER
            + "datasource plain (id: id, source: source) grain (id) address plain"
            + ASSERTS
            + ";",
            encoding="utf-8",
        )
        env = Environment(working_path=tmp_path)
        env.parse("import plain as p;")
        ds = env.datasources["p.plain"]
        assert len(ds.assertions) == 3
        assert "p.id" in {
            r.address for r in ds.assertions[0].condition.concept_arguments
        }

    @pytest.mark.parametrize(
        "trailer,message",
        [
            ("assert count(id) by source >= prior(count(id))", "no `by` grain"),
            ("assert count(id) >= id", "outside an aggregate"),
            ("assert prior(prior(count(id))) > 0", "cannot be nested"),
            ("assert id > 0", "measures nothing"),
            ("assert count(id) by source >= count(id) by id", "share one `by` grain"),
        ],
    )
    def test_rejects_shapes(self, trailer: str, message: str) -> None:
        with pytest.raises(HydrationError, match=message):
            _parse_datasource(trailer)

    def test_rejects_unstored_column(self) -> None:
        env = Environment()
        with pytest.raises(HydrationError, match="neither store nor derive"):
            env.parse(
                HEADER
                + "property id.extra int;\n"
                + "datasource plain (id: id) grain (id) address plain"
                + " assert count(extra) > 0;"
            )

    def test_prior_outside_assert_has_no_sql(self) -> None:
        executor = Dialects.DUCK_DB.default_executor(environment=Environment())
        executor.parse_text(HEADER + "datasource plain (id, source) address plain;")
        with pytest.raises(InvalidSyntaxException, match="only valid inside"):
            executor.generate_sql("select prior(count(id)) as x;")


class TestPlan:
    def test_terms_and_template(self) -> None:
        ds = _parse_datasource(
            "assert abs(count(id) - prior(count(id))) / prior(count(id)) < 0.25"
        )
        plan = plan_assertion(ds.assertions[0].condition)
        assert [t.placeholder for t in plan.new_terms] == ["__assert_new_0"]
        # the repeated prior term is read once
        assert [t.placeholder for t in plan.prior_terms] == ["__assert_prior_0"]
        assert plan.group_keys == []
        assert (
            Renderer().to_string(
                substitute(
                    plan.template, {"__assert_new_0": 80, "__assert_prior_0": 100}
                )
            )
            == "abs(80 - 100) / 100 < 0.25"
        )

    def test_group_keys(self) -> None:
        ds = _parse_datasource(ASSERTS)
        plan = plan_assertion(ds.assertions[1].condition)
        assert [k.address for k in plan.group_keys] == ["local.source"]

    def test_map_expression_reaches_leaves(self) -> None:
        tree = Function(
            operator=FunctionType.ADD,
            output_datatype="int",  # type: ignore[arg-type]
            output_purpose="property",  # type: ignore[arg-type]
            arguments=[ConceptRef(address="x.a"), 1],
            arg_count=2,
        )
        swapped = map_expression(
            tree, lambda n: 5 if isinstance(n, ConceptRef) else None
        )
        assert list(swapped.arguments) == [5, 1]

    def test_spec_error_type(self) -> None:
        with pytest.raises(AssertionSpecError):
            plan_assertion(ConceptRef(address="local.id"))


def _executor(tmp_path: Path, target: str) -> tuple[Executor, object]:
    executor = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=tmp_path)
    )
    executor.execute_raw_sql(
        "CREATE TABLE raw AS SELECT i AS id,"
        " CASE WHEN i % 10 = 0 THEN 'B' ELSE 'A' END AS source FROM range(1000) t(i)"
    )
    executor.parse_text(_model(target))
    return executor, executor.environment.datasources["built"]


class TestFileTarget:
    def test_failed_build_keeps_asset(self, tmp_path: Path, caplog) -> None:
        target = tmp_path / "built.parquet"
        executor, built = _executor(tmp_path, f"file `{target.as_posix()}`")
        with caplog.at_level("WARNING", logger="trilogy"):
            executor.update_datasource(built)
        assert "no previous asset" in caplog.text
        before = target.read_bytes()

        executor.execute_raw_sql("DELETE FROM raw WHERE id >= 10")
        with pytest.raises(DatasourceAssertionError) as info:
            executor.update_datasource(built)
        message = str(info.value)
        assert "count(id) = 10, prior(count(id)) = 1000" in message
        assert "for source = 'B': count(id) by source = 1" in message
        assert "count(id ? source = 'A') = 9" in message
        assert target.read_bytes() == before
        assert not (tmp_path / STAGING_DIR).exists()
        assert duckdb.sql(f"SELECT count(*) FROM '{target.as_posix()}'").fetchall() == [
            (1000,)
        ]

    def test_modest_drop_and_new_group_pass(self, tmp_path: Path, caplog) -> None:
        target = tmp_path / "built.parquet"
        executor, built = _executor(tmp_path, f"file `{target.as_posix()}`")
        executor.update_datasource(built)
        executor.execute_raw_sql("DELETE FROM raw WHERE id > 800 AND source = 'A'")
        executor.execute_raw_sql(
            "INSERT INTO raw SELECT i, 'C' FROM range(5000, 5100) t(i)"
        )
        with caplog.at_level("WARNING", logger="trilogy"):
            executor.update_datasource(built)
        assert "no previous value for source = 'C'" in caplog.text
        assert duckdb.sql(
            f"SELECT source, count(*) FROM '{target.as_posix()}' GROUP BY 1 ORDER BY 1"
        ).fetchall() == [("A", 720), ("B", 100), ("C", 100)]


class TestTableTarget:
    def test_failed_build_rolls_back(self, tmp_path: Path) -> None:
        executor, built = _executor(tmp_path, "address built")
        executor.update_datasource(built)
        executor.execute_raw_sql("DELETE FROM raw WHERE source = 'B' OR id > 800")
        with pytest.raises(DatasourceAssertionError) as info:
            executor.update_datasource(built)
        assert "for source = 'B': group has no rows in the new build" in str(info.value)
        assert executor.execute_raw_sql("SELECT count(*) FROM built").fetchall() == [
            (1000,)
        ]

    def test_first_build_against_empty_table(self, tmp_path: Path, caplog) -> None:
        executor, built = _executor(tmp_path, "address built")
        with caplog.at_level("WARNING", logger="trilogy"):
            executor.update_datasource(built)
        assert "previous asset has no rows" in caplog.text
        assert executor.execute_raw_sql("SELECT count(*) FROM built").fetchall() == [
            (1000,)
        ]

    def test_persist_statement_is_checked(self, tmp_path: Path) -> None:
        executor, built = _executor(tmp_path, "address built")
        executor.update_datasource(built)
        executor.execute_raw_sql("DELETE FROM raw WHERE id >= 10")
        with pytest.raises(DatasourceAssertionError):
            executor.execute_text("persist into built from select id, source;")
