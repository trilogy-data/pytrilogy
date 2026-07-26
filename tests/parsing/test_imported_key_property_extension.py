"""A module can extend an imported dimension with one-to-one properties by
mapping its datasource primary key straight to the imported key. The property
lands in the imported key's namespace (`property dim.col_id.col_bytes_in` ->
`dim.col_bytes_in`), so the datasource must bind that address. The symbol table
used to record the file-local address instead, which turned a bare reference
into a dangling `local.` placeholder that only surfaced downstream as
NoDatasourceException."""

from __future__ import annotations

import pytest

from trilogy import Dialects
from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models.environment import Environment

DIM = """key col_id int;
property col_id.col_name string;
datasource loadbalancers (
    ID: col_id,
    NAME: col_name,
)
grain (col_id)
query '''select 1 as ID, 'a' as NAME union all select 2, 'b' ''';
"""

STATS_TEMPLATE = """import lbaas_loadbalancers as lbaas_loadbalancers;

property lbaas_loadbalancers.col_id.col_bytes_in int;

datasource source (
    LOADBALANCER_ID: lbaas_loadbalancers.col_id,
    BYTES_IN: {binding},
)
grain (lbaas_loadbalancers.col_id)
query '''select 1 as LOADBALANCER_ID, 10 as BYTES_IN union all select 2, 20 ''';
"""


@pytest.fixture
def model_root(tmp_path):
    (tmp_path / "lbaas_loadbalancers.preql").write_text(DIM, encoding="utf-8")
    return tmp_path


def write_stats(root, binding: str) -> None:
    (root / "lbaas_loadbalancer_statistics.preql").write_text(
        STATS_TEMPLATE.format(binding=binding), encoding="utf-8"
    )


def test_property_declared_on_imported_key_takes_that_namespace(model_root):
    write_stats(model_root, "lbaas_loadbalancers.col_bytes_in")
    env = Environment(working_path=model_root)
    env.parse("import lbaas_loadbalancer_statistics as stats;")
    concept = env.concepts["stats.lbaas_loadbalancers.col_bytes_in"]
    assert concept.keys == {"stats.lbaas_loadbalancers.col_id"}
    columns = {c.concept.address for c in env.datasources["stats.source"].columns}
    assert columns == {
        "stats.lbaas_loadbalancers.col_id",
        "stats.lbaas_loadbalancers.col_bytes_in",
    }


def test_two_level_import_resolves_extension_property(model_root):
    write_stats(model_root, "lbaas_loadbalancers.col_bytes_in")
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_root)
    )
    results = exec.execute_query(
        "import lbaas_loadbalancer_statistics as stats;\n"
        "select stats.lbaas_loadbalancers.col_id, "
        "stats.lbaas_loadbalancers.col_bytes_in "
        "order by stats.lbaas_loadbalancers.col_id asc;"
    ).fetchall()
    assert [tuple(r) for r in results] == [(1, 10), (2, 20)]


def test_two_level_import_joins_extension_to_dimension(model_root):
    write_stats(model_root, "lbaas_loadbalancers.col_bytes_in")
    exec = Dialects.DUCK_DB.default_executor(
        environment=Environment(working_path=model_root)
    )
    results = exec.execute_query(
        "import lbaas_loadbalancer_statistics as stats;\n"
        "select stats.lbaas_loadbalancers.col_name, "
        "stats.lbaas_loadbalancers.col_bytes_in "
        "order by stats.lbaas_loadbalancers.col_name asc;"
    ).fetchall()
    assert [tuple(r) for r in results] == [("a", 10), ("b", 20)]


def test_unqualified_binding_raises_at_parse(model_root):
    env = Environment(working_path=model_root)
    with pytest.raises(
        UndefinedConceptException, match=r"lbaas_loadbalancers\.col_bytes_in"
    ):
        env.parse(STATS_TEMPLATE.format(binding="col_bytes_in"))


def test_unqualified_binding_raises_through_import(model_root):
    write_stats(model_root, "col_bytes_in")
    env = Environment(working_path=model_root)
    with pytest.raises(ImportError, match=r"lbaas_loadbalancers\.col_bytes_in"):
        env.parse("import lbaas_loadbalancer_statistics as stats;")
