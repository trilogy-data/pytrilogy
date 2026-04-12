"""Tests for the phased hydration lifecycle in the v2 parser.

Covers COLLECT_SYMBOLS, BIND, HYDRATE phases and topological ordering.
"""

from __future__ import annotations

import pytest

from trilogy.core.enums import Purpose
from trilogy.core.models.author import Concept
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
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

    def test_semantic_state_unit_rollback_restores_prior(self):
        env = Environment()
        state = _semantic_state(env)
        concept = _make_probe("probe")
        assert "local.probe" not in env.concepts.data
        state.add(concept, ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        assert "local.probe" in env.concepts.data
        state.rollback()
        assert "local.probe" not in env.concepts.data

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

    def test_pending_resolves_independent_of_environment_mirror(self):
        env = Environment()
        state = _semantic_state(env)
        lookup = ConceptLookup(state)
        state.add(_make_probe("isolated"), ConceptUpdateKind.TOP_LEVEL_DECLARATION)
        # Simulate disabled mirroring: drop the env copy but keep SemanticState.
        env.concepts.data.pop("local.isolated", None)
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
