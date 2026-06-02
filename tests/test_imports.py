from pathlib import Path

from trilogy import Dialects
from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    EnvironmentConfig,
    LazyEnvironment,
)


def test_multi_environment():
    basic = Environment()

    basic.parse(
        """
const pi <- 3.14;
                     

""",
        namespace="math",
    )

    basic.parse("""
                
            select math.pi;
                """)

    assert basic.concepts["math.pi"].name == "pi"


def test_test_alias_free_import():
    basic = Environment(working_path=Path(__file__).parent)

    basic.parse(
        """
import test_env;

key id2 int;


""",
    )

    assert basic.concepts["id"].name == "id"
    assert basic.concepts["id2"].name == "id2"
    assert basic.concepts["id"].namespace == basic.concepts["id2"].namespace


def test_import_concept_resolution():
    basic = LazyEnvironment(
        load_path=Path(__file__).parent / "test_lazy_env.preql",
        working_path=Path(__file__).parent,
        setup_queries=[],
    )
    materialized = basic.materialize_for_select()
    assert "one.two.import_key" in materialized.materialized_concepts
    assert "two.two.import_key" in materialized.materialized_concepts


def test_import_basics():
    basic = Environment(working_path=Path(__file__).parent)

    basic.parse(
        """
import test_env;

key id2 int;


""",
    )

    assert len(basic.imports["local"]) == 1, basic.imports
    importz = basic.imports["local"][0]
    assert importz.path == Path("test_env")
    expected = Path(__file__).parent / "test_env.preql"
    assert importz.input_path == expected


def test_recursive():
    basic = Environment(working_path=Path(__file__).parent)
    exec = Dialects.DUCK_DB.default_executor(environment=basic)
    from trilogy.hooks import DebuggingHook

    DebuggingHook()
    results = exec.execute_text(
        """
import recursive_env;

where id = 1
select id, name, parent.name, child.name
order by id asc;
""",
    )[-1].fetchall()

    row = results[0]
    assert row[0] == 1
    assert row[1] == "fun"
    assert row[2] == "greatest"
    assert row[3] == "times"


def test_property_block_namespace_uses_local_not_grain_key():
    """A `properties <imported.key, local_key> (...)` block declares its
    properties in the declaring file's namespace, not the namespace of the
    first grain key. Ingest-generated fact files key properties on an
    imported date dimension, so a grain-derived namespace would orphan the
    property from the datasource binding (which uses the local name)."""
    date_content = "key date_sk int;"
    sales_content = """
import date_dim as date_dim;
key amount numeric;
properties <date_dim.date_sk, amount> (
    ext_price numeric,
);
datasource sales (
    d: date_dim.date_sk,
    a: amount,
    e: ext_price,
)
grain (date_dim.date_sk, amount)
address sales;
"""
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(
            content={"date_dim": date_content, "sales": sales_content}
        )
    )
    env = Environment(config=config)
    env.parse("import sales as sales;")

    assert "sales.ext_price" in env.concepts
    assert "sales.date_dim.ext_price" not in env.concepts
    # the datasource binding and the property declaration agree on the address
    ds = env.datasources["sales.sales"]
    bound = {c.concept.address for c in ds.columns}
    assert "sales.ext_price" in bound


def test_property_derivation_namespace_uses_local_not_grain_key():
    """`property <imported.key>.name <- expr` declares the derived concept in
    the declaring file's namespace, consistent with the other `<...>` property
    forms — never pushed up into the grain key's (imported) namespace."""
    date_content = "key date_sk int;\nproperty date_sk.day_offset int;"
    sales_content = """
import date_dim as date_dim;
property <date_dim.date_sk>.shifted <- date_dim.day_offset + 1;
"""
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(
            content={"date_dim": date_content, "sales": sales_content}
        )
    )
    env = Environment(config=config)
    env.parse("import sales as sales;")

    assert "sales.shifted" in env.concepts
    assert "sales.date_dim.shifted" not in env.concepts


def test_selective_import():
    lib_content = """
key id int;
property id.name string;
property id.internal_score int;
"""
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(content={"mylib": lib_content})
    )
    env = Environment(config=config)
    env.parse("from mylib as lib import id, name;")

    # public concepts are accessible
    assert "lib.id" in env.concepts
    assert "lib.name" in env.concepts
    # internal_score is excluded from public view
    assert "lib.internal_score" not in env.concepts
    # but present for build-time lineage resolution via hidden set
    assert "lib.internal_score" in env.concepts.hidden
    # public concepts are not hidden
    assert "lib.id" not in env.concepts.hidden
    assert "lib.name" not in env.concepts.hidden


def test_selective_import_with_alias():
    lib_content = """
key id int;
property id.name string;
property id.score int;
"""
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(content={"store_returns": lib_content})
    )
    env = Environment(config=config)
    env.parse("from store_returns as returns import id, name;")

    assert "returns.id" in env.concepts
    assert "returns.name" in env.concepts
    assert "returns.score" not in env.concepts
    assert "returns.score" in env.concepts.hidden


def test_selective_import_propagates_hidden():
    lib_content = """
key id int;
property id.name string;
property id.internal_score int;
"""
    consumer_content = """
from mylib as lib import id, name;
"""
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(
            content={"mylib": lib_content, "consumer": consumer_content}
        )
    )
    env = Environment(config=config)
    env.parse("import consumer as c;")

    # public re-exports are accessible
    assert "c.lib.id" in env.concepts
    assert "c.lib.name" in env.concepts
    # hidden concept propagated, not publicly visible
    assert "c.lib.internal_score" not in env.concepts
    assert "c.lib.internal_score" in env.concepts.hidden


def test_imported_file_parsed_once_via_multiple_paths(monkeypatch):
    """A file reached through several import paths is parsed exactly once.

    Regression guard: the import cache must key on the resolved file, not the
    alias chain, otherwise a diamond import re-parses shared dependencies.
    """
    import trilogy.parsing.parse_engine_v2 as engine

    leaf = "key leaf_id int;"
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(
            content={
                "leaf": leaf,
                "mid_a": "import leaf as leaf;",
                "mid_b": "import leaf as leaf;",
            }
        )
    )
    real_parse_syntax = engine.parse_syntax
    parse_counts: dict[str, int] = {}

    def counting_parse_syntax(text):
        parse_counts[text] = parse_counts.get(text, 0) + 1
        return real_parse_syntax(text)

    monkeypatch.setattr(engine, "parse_syntax", counting_parse_syntax)

    env = Environment(config=config)
    env.parse("import mid_a as a; import mid_b as b;")

    assert "a.leaf.leaf_id" in env.concepts
    assert "b.leaf.leaf_id" in env.concepts
    assert parse_counts[leaf] == 1, parse_counts


def test_missing_import_suggests_nested_match(tmp_path):
    """When `import store_sales` fails at workspace root but `raw/store_sales.preql`
    exists, the parser surfaces a `Did you mean: raw.store_sales?` hint instead
    of leaking a raw OSError. This was the dominant agent dead-end in the
    tpc_ds_agent eval (~7 wasted calls/run)."""
    import pytest

    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "store_sales.preql").write_text("key id int;", encoding="utf-8")
    env = Environment(working_path=tmp_path)
    with pytest.raises(ImportError, match="Did you mean: raw.store_sales"):
        env.parse("import store_sales as store_sales; select 1 -> x;")


def test_missing_import_no_match_omits_hint(tmp_path):
    """No matching `.preql` anywhere under working_path → no `Did you mean` line."""
    import pytest

    env = Environment(working_path=tmp_path)
    with pytest.raises(ImportError) as exc_info:
        env.parse("import nonexistent as nx; select 1 -> x;")
    assert "Did you mean" not in str(exc_info.value)


def test_missing_import_skips_hidden_and_worker_dirs(tmp_path):
    """Suggestions skip `_worker_*`, `__pycache__`, `.git`, etc. — without
    this the eval workspace's parallel-worker copies dominated the hint list."""
    import pytest

    (tmp_path / "_worker_0" / "raw").mkdir(parents=True)
    (tmp_path / "_worker_0" / "raw" / "store_sales.preql").write_text(
        "key id int;", encoding="utf-8"
    )
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "store_sales.preql").write_text("key id int;", encoding="utf-8")
    env = Environment(working_path=tmp_path)
    with pytest.raises(ImportError) as exc_info:
        env.parse("import store_sales as store_sales; select 1 -> x;")
    msg = str(exc_info.value)
    assert "raw.store_sales" in msg
    assert "_worker_0" not in msg


def test_suggest_paths_returns_empty_when_root_missing(tmp_path):
    """_suggest_import_paths bails when the working path doesn't exist or
    isn't a directory, rather than crashing with FileNotFoundError."""
    from trilogy.parsing.v2.import_service import _suggest_import_paths

    assert _suggest_import_paths("foo", tmp_path / "missing") == []
    f = tmp_path / "file"
    f.write_text("x", encoding="utf-8")
    assert _suggest_import_paths("foo", f) == []


def test_suggest_paths_respects_max_depth(tmp_path):
    from trilogy.parsing.v2.import_service import _suggest_import_paths

    deep = tmp_path / "a" / "b" / "c" / "d" / "e"
    deep.mkdir(parents=True)
    (deep / "target.preql").write_text("x", encoding="utf-8")
    # max_depth=4 means rel.parts longer than 4 is dropped — this hit is 5.
    assert _suggest_import_paths("target", tmp_path, max_depth=4) == []
    assert _suggest_import_paths("target", tmp_path, max_depth=10) == [
        "a.b.c.d.e.target"
    ]


def test_suggest_paths_caps_hits_and_swallows_oserror(monkeypatch, tmp_path):
    from trilogy.parsing.v2 import import_service

    for i in range(30):
        sub = tmp_path / f"d{i}"
        sub.mkdir()
        (sub / "store.preql").write_text("x", encoding="utf-8")
    hits = import_service._suggest_import_paths("store", tmp_path, max_hits=2)
    assert len(hits) == 2

    def boom(*a, **kw):
        raise OSError("io error")

    monkeypatch.setattr(import_service.Path, "rglob", boom)
    assert import_service._suggest_import_paths("store", tmp_path) == []


def test_read_import_text_stdlib_raises_directly():
    """``is_stdlib=True`` skips the suggestion fallback and propagates the
    underlying OSError — stdlib lookups are absolute, suggestions don't apply."""
    import pytest

    from trilogy.parsing.v2.import_service import _read_import_text

    env = Environment()
    with pytest.raises(OSError):
        _read_import_text("/no/such/stdlib.preql", env, is_stdlib=True)


def test_read_import_text_dict_resolver_missing_key():
    import pytest

    from trilogy.parsing.v2.import_service import _read_import_text

    config = EnvironmentConfig(import_resolver=DictImportResolver(content={}))
    env = Environment(config=config)
    with pytest.raises(ImportError, match="not resolvable"):
        _read_import_text("nope", env)


def test_read_import_text_unsupported_resolver():
    import pytest

    from trilogy.parsing.v2.import_service import _read_import_text

    class _StubResolver:
        pass

    config = EnvironmentConfig(import_resolver=_StubResolver())  # type: ignore[arg-type]
    env = Environment(config=config)
    with pytest.raises(ImportError, match="not supported"):
        _read_import_text("foo", env)


def test_cyclic_import_returns_stub_import_statement():
    """When two files import each other, the second entry into a target on
    the in-flight stack returns a stub ImportStatement rather than recursing
    until max_parse_depth."""
    a_content = "import b as b;\nkey a int;"
    b_content = "import a as a;\nkey b int;"
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(content={"a": a_content, "b": b_content})
    )
    env = Environment(config=config)
    # No raise — both sides resolve via cycle break.
    env.parse("import a as a;")
    assert "a.a" in env.concepts
    assert "a.b.b" in env.concepts


def test_max_parse_depth_returns_stub(monkeypatch):
    """Going past ``max_parse_depth`` returns a stub instead of an unbounded
    recursive parse."""
    # Build a chain longer than the default depth (10).
    files = {f"f{i}": f"import f{i + 1} as nxt;\nkey k{i} int;" for i in range(15)}
    files["f15"] = "key terminal int;"
    config = EnvironmentConfig(import_resolver=DictImportResolver(content=files))
    env = Environment(config=config)
    # Doesn't crash even though depth exceeds the limit.
    env.parse("import f0 as f0;")


def test_import_hydration_service_wraps_parse_error():
    """A child-document parse failure surfaces as an ImportError with the
    underlying message — never a raw exception bubble."""
    import pytest

    config = EnvironmentConfig(
        import_resolver=DictImportResolver(content={"broken": "this is not trilogy"})
    )
    env = Environment(config=config)
    with pytest.raises(ImportError, match="parsing error"):
        env.parse("import broken as b;")


def test_self_import_dict_resolver():
    self_content = """
self import as parent;
self import as child;

key id int;
property id.name string;

datasource id_source (
    id,
    name,
    parent.id,
    child.id
)
grain (id)
query '''
select 1 as id, 'fun' as name, 0 as parent_id, 2 as child_id
union all
select 2 as id, 'times' as name, 1 as parent_id, null as child_id
union all
select 0 as id, 'greatest' as name, null as parent_id, 1 as child_id
''';
"""
    config = EnvironmentConfig(
        import_resolver=DictImportResolver(content={".": self_content})
    )
    env = Environment(config=config)
    env.parse(self_content)

    exec = Dialects.DUCK_DB.default_executor(environment=env)
    results = exec.execute_text("""
where id = 1
select id, name, parent.name, child.name
order by id asc;
""")[-1].fetchall()

    row = results[0]
    assert row[0] == 1
    assert row[1] == "fun"
    assert row[2] == "greatest"
    assert row[3] == "times"


def test_stdlib_sibling_import_under_dict_resolver():
    content = {
        "lineitem": "import std.money;\nkey id int;\nproperty id.amount float::usd;"
    }
    env = Environment(
        config=EnvironmentConfig(import_resolver=DictImportResolver(content=content))
    )
    env.parse("import lineitem;")
    assert "usd" in env.data_types
