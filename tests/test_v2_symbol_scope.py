"""Tests for the v2 SymbolTable and scoped symbol isolation."""

from __future__ import annotations

from trilogy.core.models.author import UndefinedConceptFull
from trilogy.core.models.environment import Environment
from trilogy.parsing.parse_engine_v2 import parse_text
from trilogy.parsing.v2.semantic_scope import ScopeKind, SymbolTable


class TestSymbolTableBasics:
    def test_global_scope_declare_and_lookup(self):
        env = Environment()
        table = SymbolTable(env)
        sym = table.declare("local.x", "x", "local")
        assert sym.scope.kind is ScopeKind.GLOBAL
        assert table.lookup("local.x") is sym
        assert sym.materialized is False

    def test_push_scope_adds_parent_link(self):
        env = Environment()
        table = SymbolTable(env)
        with table.push_scope(ScopeKind.FUNCTION) as scope:
            assert scope.parent is table.global_scope
            assert table.current is scope
        assert table.current is table.global_scope


class TestFunctionScope:
    def test_parameters_resolve_inside_body_only(self):
        env = Environment()
        table = SymbolTable(env)
        namespace = env.namespace or "local"

        assert table.lookup(f"{namespace}.a") is None
        with table.function_scope(["a", "b"]):
            found = table.lookup(f"{namespace}.a")
            assert found is not None
            assert found.scope.kind is ScopeKind.FUNCTION
            assert table.lookup(f"{namespace}.b") is not None

        assert table.lookup(f"{namespace}.a") is None
        assert table.lookup(f"{namespace}.b") is None

    def test_parameter_placeholder_materialized_then_removed(self):
        env = Environment()
        table = SymbolTable(env)
        namespace = env.namespace or "local"
        addr = f"{namespace}.p"

        assert addr not in env.concepts.data
        with table.function_scope(["p"]):
            placeholder = env.concepts.data.get(addr)
            assert isinstance(placeholder, UndefinedConceptFull)
        assert addr not in env.concepts.data

    def test_global_symbols_visible_from_function_scope(self):
        env = Environment()
        table = SymbolTable(env)
        table.declare("local.outer", "outer", "local")
        with table.function_scope(["x"]):
            assert table.lookup("local.outer") is not None
            assert table.lookup("local.x") is not None


class TestRowsetScope:
    def test_forward_refs_only_inside_scope(self):
        env = Environment()
        table = SymbolTable(env)
        with table.rowset_scope(["local.rs.qoh1"]):
            assert table.lookup("local.rs.qoh1") is not None
        assert table.lookup("local.rs.qoh1") is None

    def test_placeholder_replaced_by_real_concept_is_preserved(self):
        from trilogy.core.enums import Purpose
        from trilogy.core.models.author import Concept
        from trilogy.core.models.core import DataType

        env = Environment()
        table = SymbolTable(env)
        addr = "local.rs.qoh1"

        with table.rowset_scope([addr]):
            real = Concept(
                name="qoh1",
                namespace="local.rs",
                datatype=DataType.INTEGER,
                purpose=Purpose.PROPERTY,
            )
            env.concepts.data[addr] = real
        assert env.concepts.data.get(addr) is real


class TestHydrationIntegration:
    def test_function_definition_does_not_leak_parameter(self):
        text = "def square(x) -> x * x;"
        env = Environment()
        parse_text(text, environment=env)
        assert "local.x" not in env.concepts.data

    def test_function_call_resolves(self):
        text = """
key value int;
def double(x) -> x * 2;
select @double(value) -> doubled;
"""
        env = Environment()
        parse_text(text, environment=env)
        assert "local.doubled" in env.concepts.data
        assert "local.x" not in env.concepts.data
