"""`datasource child extends base, ...`: every part of the bases is added to
the child's own, and an address-less declaration is a reusable template.
Assertions may be `def` functions, so a shared guard is declared once."""

from pathlib import Path

import duckdb
import pytest

from trilogy import Dialects
from trilogy.core.models.environment import Environment
from trilogy.execution.assertions import DatasourceAssertionError
from trilogy.parsing.render import Renderer
from trilogy.parsing.v2.lark_backend import parse_lark
from trilogy.parsing.v2.model import HydrationError
from trilogy.parsing.v2.pest_backend import parse_pest

SHARED = """
key id int;
property id.source string;
property id.amount float;

def row_drop(key, pct) -> count(key) >= pct * prior(count(key));
def grouped_drop(key, dim, pct) -> count(key) by dim >= pct * prior(count(key) by dim);

template datasource guarded (id, source)
grain (id)
assert @row_drop(id, 0.75)
assert @grouped_drop(id, source, 0.75)
;

template datasource positive (amount)
assert min(amount) >= 0
;
"""

CHILD = """
datasource built extends guarded, positive (id, source, amount)
address built
assert count(id ? source = 'A') >= 5
;
"""


def _rendered(ds) -> list[str]:
    return [Renderer().to_string(a.condition) for a in ds.assertions]


class TestComposition:
    @pytest.mark.parametrize("backend", [parse_lark, parse_pest])
    def test_parses_on_both_backends(self, backend) -> None:
        backend(SHARED + CHILD)

    def test_template_is_not_a_datasource(self) -> None:
        env = Environment()
        env.parse(SHARED)
        assert set(env.datasource_templates) == {"guarded", "positive"}
        assert not env.datasources

    def test_child_gets_every_part(self) -> None:
        env = Environment()
        env.parse(SHARED + CHILD)
        built = env.datasources["built"]
        assert [c.concept.address for c in built.columns] == [
            "local.id",
            "local.source",
            "local.amount",
        ]
        assert built.grain.components == {"local.id"}
        assert _rendered(built) == [
            "@row_drop(id, 0.75)",
            "@grouped_drop(id, source, 0.75)",
            "min(amount) >= 0",
            "count(id ? source = 'A') >= 5",
        ]
        assert built.extends == ["guarded", "positive"]

    def test_child_inherits_columns_it_does_not_repeat(self) -> None:
        env = Environment()
        env.parse(SHARED + "datasource built extends guarded () address built;")
        built = env.datasources["built"]
        assert [c.concept.address for c in built.columns] == [
            "local.id",
            "local.source",
        ]

    def test_renders_as_written(self) -> None:
        env = Environment()
        env.parse(SHARED + CHILD)
        child = Renderer().to_string(env.datasources["built"])
        assert child.startswith("datasource built extends guarded, positive (")
        assert "min(amount)" not in child
        assert "assert count(id ? source = 'A') >= 5" in child
        template = Renderer().to_string(env.datasource_templates["guarded"])
        assert template.startswith("template datasource guarded (")
        assert "address" not in template
        assert "assert @row_drop(id, 0.75)" in template
        again = Environment()
        again.parse(SHARED + child)
        assert _rendered(again.datasources["built"]) == _rendered(
            env.datasources["built"]
        )

    def test_template_chain(self) -> None:
        env = Environment()
        env.parse(
            SHARED
            + "template datasource both extends guarded, positive ();\n"
            + "datasource built extends both () address built;"
        )
        assert len(env.datasources["built"].assertions) == 3
        assert "both" in env.datasource_templates

    def test_imported_template(self, tmp_path: Path) -> None:
        (tmp_path / "shared.preql").write_text(SHARED, encoding="utf-8")
        env = Environment(working_path=tmp_path)
        env.parse(
            "import shared as s;\n"
            "datasource built extends s.guarded () address built;"
        )
        built = env.datasources["built"]
        assert [c.concept.address for c in built.columns] == ["s.id", "s.source"]
        assert _rendered(built) == [
            "@row_drop(s.id, 0.75)",
            "@grouped_drop(s.id, s.source, 0.75)",
        ]

    def test_concrete_base_shares_its_address(self) -> None:
        env = Environment()
        env.parse(
            SHARED
            + "datasource base (id, source) grain (id) address base_table;\n"
            + "datasource checked extends base () assert count(id) >= 1;"
        )
        checked = env.datasources["checked"]
        assert checked.safe_address == "base_table"
        assert checked.grain.components == {"local.id"}
        assert len(checked.assertions) == 1

    @pytest.mark.parametrize(
        "text,message",
        [
            ("datasource built extends nope () address built;", "not a datasource"),
            (
                (
                    "template datasource a (id) grain (id);\n"
                    "template datasource b (source) grain (source);\n"
                    "datasource built extends a, b () address built;"
                ),
                "`grain` differs",
            ),
            ("datasource forgot (id);", "Declare it `template datasource`"),
            ("root template datasource t (id);", "both `root` and `template`"),
            ("template datasource t (id) address t;", "names a location"),
            ("datasource loop extends loop () address x;", "extends itself"),
        ],
    )
    def test_rejects(self, text: str, message: str) -> None:
        env = Environment()
        with pytest.raises(HydrationError, match=message):
            env.parse(SHARED + text)


def _model(target: str) -> str:
    return (
        SHARED
        + "root datasource raw (id: id, source: source, amount: amount) grain (id)"
        " address raw;\n"
        + f"datasource built extends guarded, positive (id, source, amount)\n"
        f"file `{target}`\nfreshness by id\nassert count(id ? source = 'A') >= 5;"
    )


class TestInheritedAssertionsRun:
    def test_function_assertions_guard_the_build(self, tmp_path: Path) -> None:
        target = tmp_path / "built.parquet"
        executor = Dialects.DUCK_DB.default_executor(
            environment=Environment(working_path=tmp_path)
        )
        executor.execute_raw_sql(
            "CREATE TABLE raw AS SELECT i AS id,"
            " CASE WHEN i % 10 = 0 THEN 'B' ELSE 'A' END AS source,"
            " i * 1.5 AS amount FROM range(1000) t(i)"
        )
        executor.parse_text(_model(target.as_posix()))
        built = executor.environment.datasources["built"]
        executor.update_datasource(built)
        executor.execute_raw_sql("DELETE FROM raw WHERE source = 'B' OR id > 500")
        with pytest.raises(DatasourceAssertionError) as info:
            executor.update_datasource(built)
        message = str(info.value)
        assert (
            "`@row_drop(id, 0.75)`: count(id) = 450, prior(count(id)) = 1000" in message
        )
        assert (
            "`@grouped_drop(id, source, 0.75)` for source = 'B': group has no rows"
            in message
        )
        assert duckdb.sql(f"SELECT count(*) FROM '{target.as_posix()}'").fetchall() == [
            (1000,)
        ]
