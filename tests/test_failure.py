from pytest import raises

from trilogy import Dialects
from trilogy.core.exceptions import UnresolvableQueryException


def test_cannot_find():
    x = Dialects.DUCK_DB.default_executor()

    with raises(UnresolvableQueryException):
        x.generate_sql("""
    key x int;
    key y int;

    datasource fun (
    y: y,
        )
    address abc;


    select x;

    """)


def test_cannot_find_complex():
    x = Dialects.DUCK_DB.default_executor()
    with raises(UnresolvableQueryException):
        x.generate_sql("""
    key x int;
    key y int;

    auto sum <- x+y;

    datasource fun (
    y: y,
        )
    address abc;


    select sum(y) by x as fun;

    """)


def test_disjoint_source_models_grouping():
    from trilogy.core.processing.concept_strategies_v3 import _disjoint_source_models

    class _C:
        def __init__(self, address, namespace, sources=()):
            self.address = address
            self.namespace = namespace
            self.sources = list(sources)

    store_name = _C("ss.store.store_name", "ss.store")
    web_rev = _C("local.web_rev", "local", sources=[_C("ws.net_paid", "ws")])

    models = _disjoint_source_models([store_name, web_rev])
    assert set(models) == {"ss", "ws"}
    assert "ss.store.store_name" in models["ss"]
    assert "local.web_rev" in models["ws"]
