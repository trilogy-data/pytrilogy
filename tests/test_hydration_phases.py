"""Tests for the phased hydration lifecycle in the v2 parser.

Covers COLLECT_SYMBOLS, BIND, HYDRATE phases and topological ordering.
"""

from __future__ import annotations

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


def _hydrator_for(text: str) -> tuple[NativeHydrator, list]:
    """Create a hydrator and plan list from text."""
    env = Environment()
    ctx = HydrationContext(environment=env)
    hydrator = NativeHydrator(ctx)
    document = parse_syntax(text)
    hydrator.set_text(document.text)
    from trilogy.parsing.v2.model import RecordingEnvironmentUpdate
    from trilogy.parsing.v2.rules_context import RuleContext

    hydrator._cached_rule_context = RuleContext(
        environment=hydrator.environment,
        function_factory=hydrator.function_factory,
        source_text=document.text,
        update=hydrator.update,
    )
    plans = hydrator.plan(document.forms)
    return hydrator, plans


class TestCollectSymbols:
    def test_declaration_registers_name(self):
        hydrator, plans = _hydrator_for("key id int;")
        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        assert len(concept_plans) == 1

        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)

        assert concept_plans[0].address == "local.id"
        assert "local.id" in hydrator.environment.concepts

    def test_derivation_registers_name(self):
        hydrator, plans = _hydrator_for("key x int;\nauto y <- x + 1;")
        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        assert len(concept_plans) == 2

        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)

        addresses = {p.address for p in concept_plans}
        assert "local.x" in addresses
        assert "local.y" in addresses
        assert "local.y" in hydrator.environment.concepts

    def test_constant_registers_name(self):
        hydrator, plans = _hydrator_for("const pi <- 3.14;")
        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]

        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)

        assert concept_plans[0].address == "local.pi"
        assert "local.pi" in hydrator.environment.concepts

    def test_property_registers_name(self):
        hydrator, plans = _hydrator_for("key id int;\nproperty id.name string;")
        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]

        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)

        addresses = {p.address for p in concept_plans}
        assert "local.id" in addresses
        assert "local.name" in addresses

    def test_skeleton_has_unknown_datatype(self):
        hydrator, plans = _hydrator_for("key id int;")
        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)

        concept = hydrator.environment.concepts["local.id"]
        assert concept.datatype == DataType.UNKNOWN

    def test_non_concept_plans_are_noop(self):
        hydrator, plans = _hydrator_for("show concepts;")
        # Should not raise
        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)


class TestBind:
    def test_no_dependencies_for_declaration(self):
        hydrator, plans = _hydrator_for("key id int;")
        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(plans, HydrationPhase.BIND)

        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        assert concept_plans[0].dependencies == []

    def test_derivation_finds_dependency(self):
        hydrator, plans = _hydrator_for("key x int;\nauto y <- x + 1;")
        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(plans, HydrationPhase.BIND)

        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        y_plan = next(p for p in concept_plans if p.address == "local.y")
        assert "local.x" in y_plan.dependencies

    def test_multiple_dependencies(self):
        text = "key a int;\nkey b int;\nauto c <- a + b;"
        hydrator, plans = _hydrator_for(text)
        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(plans, HydrationPhase.BIND)

        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        c_plan = next(p for p in concept_plans if p.address == "local.c")
        assert "local.a" in c_plan.dependencies
        assert "local.b" in c_plan.dependencies

    def test_constant_no_dependencies(self):
        hydrator, plans = _hydrator_for("const pi <- 3.14;")
        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(plans, HydrationPhase.BIND)

        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        assert concept_plans[0].dependencies == []


class TestTopologicalSort:
    def test_preserves_order_when_no_deps(self):
        text = "key a int;\nkey b int;\nkey c int;"
        hydrator, plans = _hydrator_for(text)
        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(plans, HydrationPhase.BIND)

        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        sorted_plans = topological_sort_plans(concept_plans, hydrator.environment)
        assert [p.address for p in sorted_plans] == [p.address for p in concept_plans]

    def test_reorders_forward_reference(self):
        # y references x, but y is declared first
        text = "auto y <- x + 1;\nkey x int;"
        hydrator, plans = _hydrator_for(text)
        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(plans, HydrationPhase.BIND)

        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        sorted_plans = topological_sort_plans(concept_plans, hydrator.environment)

        addresses = [p.address for p in sorted_plans]
        assert addresses.index("local.x") < addresses.index("local.y")

    def test_chain_dependencies(self):
        # c depends on b, b depends on a
        text = "auto c <- b + 1;\nauto b <- a + 1;\nkey a int;"
        hydrator, plans = _hydrator_for(text)
        hydrator._run_phase(plans, HydrationPhase.COLLECT_SYMBOLS)
        hydrator._run_phase(plans, HydrationPhase.BIND)

        concept_plans = [p for p in plans if isinstance(p, ConceptStatementPlan)]
        sorted_plans = topological_sort_plans(concept_plans, hydrator.environment)

        addresses = [p.address for p in sorted_plans]
        assert addresses.index("local.a") < addresses.index("local.b")
        assert addresses.index("local.b") < addresses.index("local.c")


class TestHydrate:
    def test_declaration_gets_correct_datatype(self):
        _, output = parse_text("key id int;", Environment())
        assert output[0].concept.datatype == DataType.INTEGER

    def test_derivation_resolves_lineage(self):
        env, output = parse_text(
            "key x int;\nauto y <- x + 1;", Environment()
        )
        assert env.concepts["local.y"].datatype == DataType.INTEGER
        assert env.concepts["local.y"].lineage is not None

    def test_forward_reference_resolved(self):
        env, output = parse_text(
            "auto y <- x + 1;\nkey x int;", Environment()
        )
        assert env.concepts["local.y"].datatype == DataType.INTEGER
        assert env.concepts["local.y"].lineage is not None

    def test_property_hydrates_fully(self):
        env, _ = parse_text(
            "key id int;\nproperty id.name string;", Environment()
        )
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


class TestFindConceptLiterals:
    def test_finds_in_simple_expression(self):
        doc = parse_syntax("auto y <- x + 1;")
        from trilogy.parsing.v2.syntax import SyntaxNodeKind

        # Walk into the derivation source
        block = doc.forms[0]
        literals = find_concept_literals(block)
        assert len(literals) >= 1
        assert all(
            lit.kind == SyntaxNodeKind.CONCEPT_LITERAL for lit in literals
        )

    def test_extracts_name(self):
        doc = parse_syntax("auto y <- x + 1;")
        block = doc.forms[0]
        literals = find_concept_literals(block)
        names = [
            extract_concept_name_from_literal(lit, "local") for lit in literals
        ]
        # Should contain x (and possibly y from the declaration part)
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
