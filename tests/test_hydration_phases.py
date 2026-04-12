"""Tests for the phased hydration lifecycle in the v2 parser.

Covers COLLECT_SYMBOLS, BIND, HYDRATE phases and topological ordering.
"""

from __future__ import annotations

import pytest

from trilogy.core.enums import Purpose
from trilogy.core.models.author import Concept
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.statements.author import (
    MultiSelectStatement,
    RowsetDerivationStatement,
    SelectStatement,
)
from trilogy.parsing.parse_engine_v2 import parse_syntax, parse_text
from trilogy.parsing.v2.hydration import (
    ConceptStatementPlan,
    HydrationContext,
    HydrationPhase,
    NativeHydrator,
    extract_concept_name_from_literal,
    extract_dependencies,
    find_concept_literals,
    topological_sort_plans,
)
from trilogy.parsing.v2.model import ConceptUpdate, ConceptUpdateKind
from trilogy.parsing.v2.semantic_state import ConceptLookup, SemanticState


def _semantic_state(env: Environment) -> SemanticState:
    return SemanticState(environment=env)


def _make_probe(name: str) -> Concept:
    return Concept(
        name=name,
        namespace="local",
        datatype=DataType.INTEGER,
        purpose=Purpose.KEY,
    )


def _hydrator_for(text: str) -> NativeHydrator:
    """Create a hydrator with plans from text."""
    env = Environment()
    ctx = HydrationContext(environment=env)
    hydrator = NativeHydrator(ctx)
    document = parse_syntax(text)
    hydrator.set_text(document.text)
    from trilogy.parsing.v2.rules_context import RuleContext

    hydrator._cached_rule_context = RuleContext(
        environment=hydrator.environment,
        function_factory=hydrator.function_factory,
        symbol_table=hydrator.symbol_table,
        semantic_state=hydrator.semantic_state,
        source_text=document.text,
    )
    hydrator.plans = hydrator.plan(document.forms)
    return hydrator


class TestCollectSymbols:
    def test_declaration_extracts_address(self):
        hydrator = _hydrator_for("key id int;")
        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        assert len(concept_plans) == 1

        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)

        assert concept_plans[0].address == "local.id"

    def test_derivation_extracts_address(self):
        hydrator = _hydrator_for("key x int;\nauto y <- x + 1;")
        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        assert len(concept_plans) == 2

        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)

        addresses = {p.address for p in concept_plans}
        assert "local.x" in addresses
        assert "local.y" in addresses

    def test_constant_extracts_address(self):
        hydrator = _hydrator_for("const pi <- 3.14;")
        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]

        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)

        assert concept_plans[0].address == "local.pi"

    def test_property_extracts_address(self):
        hydrator = _hydrator_for("key id int;\nproperty id.name string;")
        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]

        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)

        addresses = {p.address for p in concept_plans}
        assert "local.id" in addresses
        assert "local.name" in addresses

    def test_does_not_modify_environment(self):
        hydrator = _hydrator_for("key id int;\nauto y <- id + 1;")
        initial_keys = set(hydrator.environment.concepts.keys())

        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)

        assert set(hydrator.environment.concepts.keys()) == initial_keys

    def test_non_concept_plans_are_noop(self):
        hydrator = _hydrator_for("show concepts;")
        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)

    def test_properties_block_returns_none(self):
        hydrator = _hydrator_for("key id int;\nproperties <id> (name string, age int);")
        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)
        # properties block has no single address
        props_plan = next(p for p in concept_plans if p.address is None)
        assert props_plan is not None


class TestBind:
    def test_no_dependencies_for_declaration(self):
        hydrator = _hydrator_for("key id int;")
        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(HydrationPhase.BIND)

        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        assert concept_plans[0].dependencies == []

    def test_derivation_finds_dependency(self):
        hydrator = _hydrator_for("key x int;\nauto y <- x + 1;")
        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(HydrationPhase.BIND)

        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        y_plan = next(p for p in concept_plans if p.address == "local.y")
        assert "local.x" in y_plan.dependencies

    def test_multiple_dependencies(self):
        text = "key a int;\nkey b int;\nauto c <- a + b;"
        hydrator = _hydrator_for(text)
        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(HydrationPhase.BIND)

        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        c_plan = next(p for p in concept_plans if p.address == "local.c")
        assert "local.a" in c_plan.dependencies
        assert "local.b" in c_plan.dependencies

    def test_constant_no_dependencies(self):
        hydrator = _hydrator_for("const pi <- 3.14;")
        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(HydrationPhase.BIND)

        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        assert concept_plans[0].dependencies == []


class TestTopologicalSort:
    def test_preserves_order_when_no_deps(self):
        text = "key a int;\nkey b int;\nkey c int;"
        hydrator = _hydrator_for(text)
        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(HydrationPhase.BIND)

        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        sorted_plans = topological_sort_plans(concept_plans, hydrator.environment)
        assert [p.address for p in sorted_plans] == [p.address for p in concept_plans]

    def test_reorders_forward_reference(self):
        text = "auto y <- x + 1;\nkey x int;"
        hydrator = _hydrator_for(text)
        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(HydrationPhase.BIND)

        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        sorted_plans = topological_sort_plans(concept_plans, hydrator.environment)

        addresses = [p.address for p in sorted_plans]
        assert addresses.index("local.x") < addresses.index("local.y")

    def test_chain_dependencies(self):
        text = "auto c <- b + 1;\nauto b <- a + 1;\nkey a int;"
        hydrator = _hydrator_for(text)
        hydrator._run_phase(HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(HydrationPhase.BIND)

        concept_plans = [
            p for p in hydrator.plans if isinstance(p, ConceptStatementPlan)
        ]
        sorted_plans = topological_sort_plans(concept_plans, hydrator.environment)

        addresses = [p.address for p in sorted_plans]
        assert addresses.index("local.a") < addresses.index("local.b")
        assert addresses.index("local.b") < addresses.index("local.c")


class TestHydrate:
    def test_declaration_gets_correct_datatype(self):
        _, output = parse_text("key id int;", Environment())
        assert output[0].concept.datatype == DataType.INTEGER

    def test_derivation_resolves_lineage(self):
        env, output = parse_text("key x int;\nauto y <- x + 1;", Environment())
        assert env.concepts["local.y"].datatype == DataType.INTEGER
        assert env.concepts["local.y"].lineage is not None

    def test_forward_reference_resolved(self):
        env, output = parse_text("auto y <- x + 1;\nkey x int;", Environment())
        assert env.concepts["local.y"].datatype == DataType.INTEGER
        assert env.concepts["local.y"].lineage is not None

    def test_property_hydrates_fully(self):
        env, _ = parse_text("key id int;\nproperty id.name string;", Environment())
        assert env.concepts["local.name"].datatype == DataType.STRING

    def test_constant_hydrates_fully(self):
        env, _ = parse_text("const pi <- 3.14;", Environment())
        assert env.concepts["local.pi"].datatype == DataType.FLOAT

    def test_chained_forward_references(self):
        text = "auto c <- b + 1;\nauto b <- a + 1;\nkey a int;"
        env, _ = parse_text(text, Environment())
        assert env.concepts["local.a"].datatype == DataType.INTEGER
        assert env.concepts["local.b"].datatype == DataType.INTEGER
        assert env.concepts["local.c"].datatype == DataType.INTEGER

    def test_properties_block_hydrates(self):
        text = "key id int;\nproperties <id> (name string, age int);"
        env, _ = parse_text(text, Environment())
        assert env.concepts["local.name"].datatype == DataType.STRING
        assert env.concepts["local.age"].datatype == DataType.INTEGER


class TestFindConceptLiterals:
    def test_finds_in_simple_expression(self):
        doc = parse_syntax("auto y <- x + 1;")
        from trilogy.parsing.v2.syntax import SyntaxNodeKind

        block = doc.forms[0]
        literals = find_concept_literals(block)
        assert len(literals) >= 1
        assert all(lit.kind == SyntaxNodeKind.CONCEPT_LITERAL for lit in literals)

    def test_extracts_name(self):
        doc = parse_syntax("auto y <- x + 1;")
        block = doc.forms[0]
        literals = find_concept_literals(block)
        names = [extract_concept_name_from_literal(lit, "local") for lit in literals]
        assert "local.x" in names


class TestExtractDependencies:
    def test_no_deps_for_declaration(self):
        doc = parse_syntax("key id int;")
        env = Environment()
        block = doc.forms[0]
        deps = extract_dependencies(block, env)
        assert deps == []

    def test_finds_deps_in_derivation(self):
        doc = parse_syntax("auto y <- x + 1;")
        env = Environment()
        block = doc.forms[0]
        deps = extract_dependencies(block, env)
        assert "local.x" in deps


def _fully_parse(text: str) -> NativeHydrator:
    env = Environment()
    ctx = HydrationContext(environment=env)
    hydrator = NativeHydrator(ctx)
    document = parse_syntax(text)
    hydrator.parse(document)
    return hydrator


def _kinds_for(updates: list[ConceptUpdate], address: str) -> list[ConceptUpdateKind]:
    return [u.kind for u in updates if u.concept.address == address]


class TestConceptUpdateKinds:
    def test_top_level_declaration(self):
        hydrator = _fully_parse("key id int;")
        assert ConceptUpdateKind.TOP_LEVEL_DECLARATION in _kinds_for(
            hydrator.semantic_state.concepts, "local.id"
        )

    def test_property_declaration(self):
        hydrator = _fully_parse("key id int;\nproperty id.name string;")
        assert ConceptUpdateKind.PROPERTY_DECLARATION in _kinds_for(
            hydrator.semantic_state.concepts, "local.name"
        )

    def test_select_local(self):
        hydrator = _fully_parse("key x int;\nselect x + 1 -> y;")
        assert ConceptUpdateKind.SELECT_LOCAL in _kinds_for(
            hydrator.semantic_state.concepts, "local.y"
        )

    def test_multiselect_output(self):
        text = """
key one int;
key other_one int;
datasource num_one (
    one:one
) grain (one) address num_one;
datasource num_other (
    other_one:other_one
) grain (other_one) address num_other;

SELECT one
MERGE
SELECT other_one
ALIGN one_key:one,other_one;
"""
        hydrator = _fully_parse(text)
        kinds = [u.kind for u in hydrator.semantic_state.concepts]
        assert ConceptUpdateKind.MULTISELECT_OUTPUT in kinds

    def test_rowset_output(self):
        text = "key x int;\nrowset r <- select x;"
        hydrator = _fully_parse(text)
        kinds = [u.kind for u in hydrator.semantic_state.concepts]
        assert ConceptUpdateKind.ROWSET_OUTPUT in kinds


class TestSemanticStateTransaction:
    def test_successful_parse_commits_concepts(self):
        env = Environment()
        _, _ = parse_text("key id int;\nauto derived <- id + 1;", env)
        assert "local.id" in env.concepts.data
        assert "local.derived" in env.concepts.data

    def test_failed_parse_does_not_leak_concepts(self):
        env = Environment()
        baseline = set(env.concepts.data.keys())
        with pytest.raises(Exception):
            parse_text("key leaked int;\nselect undefined_col;", env)
        assert "local.leaked" not in env.concepts.data
        assert set(env.concepts.data.keys()) == baseline

    def test_select_local_resolves_during_hydration(self):
        env, _ = parse_text("key x int;\nselect x + 1 -> y;", Environment())
        assert "local.y" in env.concepts.data

    def test_multiselect_output_resolves_during_hydration(self):
        text = """
key one int;
key other_one int;
datasource num_one (
    one:one
) grain (one) address num_one;
datasource num_other (
    other_one:other_one
) grain (other_one) address num_other;

SELECT one
MERGE
SELECT other_one
ALIGN one_key:one,other_one;
"""
        env, _ = parse_text(text, Environment())
        assert "local.one_key" in env.concepts.data

    def test_rowset_output_resolves_during_hydration(self):
        env, _ = parse_text("key x int;\nrowset r <- select x;", Environment())
        assert "r.x" in env.concepts.data

    def test_semantic_state_unit_rollback_does_not_leak(self):
        env = Environment()
        state = _semantic_state(env)
        concept = _make_probe("probe")
        assert "local.probe" not in env.concepts.data
        state.add(concept, ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        assert "local.probe" not in env.concepts.data
        assert state.pending_lookup("local.probe") is concept
        state.rollback()
        assert "local.probe" not in env.concepts.data
        assert state.pending_lookup("local.probe") is None

    def test_semantic_state_unit_commit_advances_boundary(self):
        env = Environment()
        state = _semantic_state(env)
        state.add(_make_probe("first"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        state.commit()
        state.add(_make_probe("second"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        state.rollback()
        assert "local.first" in env.concepts.data
        assert "local.second" not in env.concepts.data
        assert any(u.concept.address == "local.first" for u in state.concepts)
        assert all(u.concept.address != "local.second" for u in state.concepts)


class TestSemanticStateStaged:
    def test_add_does_not_mutate_environment(self):
        env = Environment()
        state = SemanticState(environment=env)
        state.add(_make_probe("quiet"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        assert "local.quiet" not in env.concepts.data
        assert state.pending_lookup("local.quiet") is not None

    def test_commit_applies_pending_to_environment(self):
        env = Environment()
        state = SemanticState(environment=env)
        state.add(_make_probe("deferred"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        state.commit()
        assert "local.deferred" in env.concepts.data

    def test_rollback_does_not_touch_environment(self):
        env = Environment()
        baseline = set(env.concepts.data.keys())
        state = SemanticState(environment=env)
        state.add(_make_probe("disposable"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        state.rollback()
        assert set(env.concepts.data.keys()) == baseline
        assert state.pending_lookup("local.disposable") is None

    def test_lookup_facade_sees_pending(self):
        env = Environment()
        state = SemanticState(environment=env)
        lookup = ConceptLookup(state)
        state.add(_make_probe("offstage"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        assert lookup.require("local.offstage").name == "offstage"
        assert "local.offstage" not in env.concepts.data

    def test_replace_concept_uses_overlay(self):
        env = Environment()
        state = SemanticState(environment=env)
        state.add(_make_probe("morph"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        replacement = Concept(
            name="morph",
            namespace="local",
            datatype=DataType.STRING,
            purpose=Purpose.KEY,
        )
        with state.pending_overlay_scope():
            state.replace_concept(
                "local.morph",
                replacement,
                ConceptUpdateKind.TOP_LEVEL_DECLARATION,
            )
            assert env.concepts["local.morph"].datatype == DataType.STRING
            assert "local.morph" not in env.concepts.data
        assert "local.morph" not in env.concepts.data
        state.commit()
        assert env.concepts.data["local.morph"].datatype == DataType.STRING


class TestPendingOverlayScope:
    def test_overlay_exposes_pending_to_env_read(self):
        env = Environment()
        state = SemanticState(environment=env)
        state.add(_make_probe("staged"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        with state.pending_overlay_scope():
            resolved = env.concepts["local.staged"]
            assert resolved.name == "staged"
            assert "local.staged" not in env.concepts.data
            assert "local.staged" in env.concepts
        assert "local.staged" not in env.concepts.data

    def test_overlay_is_popped_on_exception(self):
        env = Environment()
        state = SemanticState(environment=env)
        state.add(_make_probe("boom"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        with pytest.raises(RuntimeError):
            with state.pending_overlay_scope():
                assert env.concepts["local.boom"].name == "boom"
                raise RuntimeError("parse failure")
        assert env.concepts._overlay_stack == []
        with pytest.raises(Exception):
            env.concepts["local.boom"]

    def test_overlay_sees_concepts_added_mid_scope(self):
        env = Environment()
        state = SemanticState(environment=env)
        with state.pending_overlay_scope():
            assert "local.late" not in env.concepts
            state.add(_make_probe("late"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
            assert env.concepts["local.late"].name == "late"
            assert "local.late" not in env.concepts.data

    def test_overlay_namespace_fallback(self):
        env = Environment()
        state = SemanticState(environment=env)
        state.add(_make_probe("bare"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        with state.pending_overlay_scope():
            assert env.concepts["bare"].name == "bare"
            assert env.concepts["local.bare"].name == "bare"

    def test_overlay_does_not_leak_between_scopes(self):
        env = Environment()
        state_a = SemanticState(environment=env)
        state_b = SemanticState(environment=env)
        state_a.add(_make_probe("alpha"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        state_b.add(_make_probe("beta"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        with state_a.pending_overlay_scope():
            assert env.concepts["local.alpha"].name == "alpha"
            with pytest.raises(Exception):
                env.concepts["local.beta"]
        with state_b.pending_overlay_scope():
            assert env.concepts["local.beta"].name == "beta"
            with pytest.raises(Exception):
                env.concepts["local.alpha"]

    def test_nested_overlays_read_both(self):
        env = Environment()
        state = SemanticState(environment=env)
        state.add(_make_probe("outer"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        with state.pending_overlay_scope():
            inner_view = {"local.inner": _make_probe("inner")}
            with env.concepts.push_overlay(inner_view):
                assert env.concepts["local.inner"].name == "inner"
                assert env.concepts["local.outer"].name == "outer"
            with pytest.raises(Exception):
                env.concepts["local.inner"]
            assert env.concepts["local.outer"].name == "outer"
        assert env.concepts._overlay_stack == []

    def test_overlay_is_read_only(self):
        from types import MappingProxyType

        env = Environment()
        state = SemanticState(environment=env)
        with state.pending_overlay_scope():
            assert env.concepts._overlay_stack, "overlay not installed"
            top = env.concepts._overlay_stack[-1]
            assert isinstance(top, MappingProxyType)
            with pytest.raises(TypeError):
                top["local.sneaky"] = _make_probe("sneaky")  # type: ignore[index]

    def test_commit_after_overlay_persists(self):
        env = Environment()
        state = SemanticState(environment=env)
        state.add(_make_probe("keep"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        with state.pending_overlay_scope():
            pass
        assert "local.keep" not in env.concepts.data
        state.commit()
        assert "local.keep" in env.concepts.data


class TestConceptLookupFacade:
    def test_pending_concept_resolves_before_commit(self):
        env = Environment()
        state = _semantic_state(env)
        lookup = ConceptLookup(state)
        state.add(_make_probe("pending"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        assert lookup.require("local.pending").name == "pending"
        assert lookup.get("local.pending") is not None
        assert lookup.contains("local.pending")
        assert "local.pending" in lookup

    def test_base_environment_concept_resolves(self):
        env = Environment()
        env.add_concept(_make_probe("base"))
        state = _semantic_state(env)
        lookup = ConceptLookup(state)
        assert lookup.require("local.base").name == "base"
        assert lookup.contains("local.base")
        assert "local.base" in [c.address for c in lookup.values()]

    def test_pending_overrides_base(self):
        env = Environment()
        base = _make_probe("shared")
        base.metadata = None
        env.add_concept(base)
        state = _semantic_state(env)
        lookup = ConceptLookup(state)
        replacement = Concept(
            name="shared",
            namespace="local",
            datatype=DataType.STRING,
            purpose=Purpose.KEY,
        )
        state.add(replacement, ConceptUpdateKind.TOP_LEVEL_DECLARATION, force=True)
        resolved = lookup.require("local.shared")
        assert resolved.datatype == DataType.STRING

    def test_pending_resolves_without_environment_write(self):
        env = Environment()
        state = _semantic_state(env)
        lookup = ConceptLookup(state)
        state.add(_make_probe("isolated"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        assert "local.isolated" not in env.concepts.data
        assert lookup.require("local.isolated").name == "isolated"
        assert lookup.get("local.isolated") is not None
        assert lookup.contains("local.isolated")

    def test_reference_returns_concept_ref(self):
        env = Environment()
        state = _semantic_state(env)
        lookup = ConceptLookup(state)
        state.add(_make_probe("ref_me"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        ref = lookup.reference("local.ref_me")
        assert ref.address == "local.ref_me"

    def test_missing_require_raises(self):
        env = Environment()
        state = _semantic_state(env)
        lookup = ConceptLookup(state)
        with pytest.raises(Exception):
            lookup.require("local.nope")

    def test_missing_get_returns_none(self):
        env = Environment()
        state = _semantic_state(env)
        lookup = ConceptLookup(state)
        assert lookup.get("local.nope") is None
        assert not lookup.contains("local.nope")

    def test_unqualified_resolves_pending_local(self):
        env = Environment()
        state = SemanticState(environment=env)
        lookup = ConceptLookup(state)
        state.add(_make_probe("id"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        assert lookup.require("id").address == "local.id"
        assert lookup.get("id") is not None
        assert lookup.contains("id")
        assert "id" in lookup
        assert lookup.reference("id").address == "local.id"

    def test_local_qualified_resolves_stripped(self):
        env = Environment()
        state = SemanticState(environment=env)
        lookup = ConceptLookup(state)
        # Simulate a pending concept stored under a stripped name variant.
        probe = _make_probe("id")
        state._pending_by_address["id"] = probe
        assert lookup.require("local.id") is probe
        assert lookup.get("local.id") is probe
        assert lookup.contains("local.id")


def _parse_staged(text: str) -> tuple[Environment, list]:
    env = Environment()
    state = SemanticState(environment=env)
    ctx = HydrationContext(environment=env, semantic_state=state)
    hydrator = NativeHydrator(ctx)
    document = parse_syntax(text)
    output = hydrator.parse(document)
    return env, output


class TestStagedParser:
    def test_key_then_select_ref(self):
        env, output = _parse_staged("key x int;\nselect x;")
        assert "local.x" in env.concepts.data
        select = next(o for o in output if isinstance(o, SelectStatement))
        assert [x.concept.address for x in select.selection] == ["local.x"]

    def test_key_then_inline_derivation(self):
        env, output = _parse_staged("key x int;\nselect x + 1 -> y;")
        assert "local.x" in env.concepts.data
        assert "local.y" in env.concepts.data
        assert env.concepts["local.y"].datatype == DataType.INTEGER

    def test_multiselect_align(self):
        text = """
key one int;
key other_one int;
datasource num_one (
    one:one
) grain (one) address num_one;
datasource num_other (
    other_one:other_one
) grain (other_one) address num_other;

SELECT one
MERGE
SELECT other_one
ALIGN one_key:one,other_one;
"""
        env, output = _parse_staged(text)
        assert "local.one_key" in env.concepts.data
        multi = next(o for o in output if isinstance(o, MultiSelectStatement))
        assert multi is not None

    def test_rowset_output(self):
        env, output = _parse_staged("key x int;\nrowset r <- select x;")
        assert "local.x" in env.concepts.data
        assert "r.x" in env.concepts.data
        rowset = next(o for o in output if isinstance(o, RowsetDerivationStatement))
        assert rowset.name == "r"


class TestDefaultStagedParser:
    def test_key_then_property(self):
        env, _ = parse_text("key id int; property id.name string;", Environment())
        assert "local.id" in env.concepts.data
        assert "local.name" in env.concepts.data

    def test_key_then_auto_derivation(self):
        env, _ = parse_text("key x int; auto y <- x + 1;", Environment())
        assert env.concepts["local.y"].datatype == DataType.INTEGER

    def test_auto_before_key(self):
        env, _ = parse_text("auto y <- x + 1; key x int;", Environment())
        assert env.concepts["local.y"].datatype == DataType.INTEGER
        assert env.concepts["local.x"].datatype == DataType.INTEGER

    def test_datasource_resolves_pending_key(self):
        env, _ = parse_text(
            "key id int; datasource test (id:id) grain (id) address memory.test;",
            Environment(),
        )
        assert "test" in env.datasources
        assert "local.id" in env.concepts.data

    def test_select_derivation_roundtrip(self):
        env, output = parse_text("key x int;\nselect x + 1 -> y;", Environment())
        assert "local.y" in env.concepts.data
        select = next(o for o in output if isinstance(o, SelectStatement))
        assert any(s.concept.address == "local.y" for s in select.selection)
