"""Unit tests for the discovery loop's condition-completion validation.

Two related bugs in this area both surfaced as the INCOMPLETE_CONDITION
guardrail at concept_strategies_v3.py raising SyntaxError:

1. Merge-expanded ROOT nodes were having their condition row arguments
   stripped off by `_handle_expanded_node`. `gen_merge_node` deliberately
   includes those concepts (and intentionally does *not* set
   preexisting_conditions on the returned node) so the discovery loop can
   apply the WHERE itself; stripping them broke that handoff.

2. Stacks containing a scalar/single-row CTE reference (e.g. `max_total.cmax`)
   were failing the conditions-equality check because the scalar's
   preexisting_conditions reflect the CTE's own WHERE, which can be stricter
   than the consumer's WHERE — but the scalar is cross-joined as a constant,
   so its filter is independent of the outer query's row-level filter.
"""

from types import SimpleNamespace

from trilogy.core.enums import (
    BooleanOperator,
    ComparisonOperator,
    Derivation,
    Granularity,
    Purpose,
)
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.core import DataType
from trilogy.core.models.environment import Environment
from trilogy.core.processing.discovery_node_factory import (
    NodeGenerationContext,
    RootNodeHandler,
)
from trilogy.core.processing.discovery_validation import (
    ValidationResult,
    _is_scalar_only,
    validate_stack,
)
from trilogy.core.processing.nodes import History


def _concept(
    name: str,
    granularity: Granularity = Granularity.MULTI_ROW,
    derivation: Derivation = Derivation.ROOT,
) -> BuildConcept:
    return BuildConcept(
        name=name,
        canonical_name=name,
        datatype=DataType.STRING,
        purpose=Purpose.KEY,
        build_is_aggregate=False,
        derivation=derivation,
        granularity=granularity,
        grain=BuildGrain(),
    )


def _eq(concept: BuildConcept, value: str) -> BuildComparison:
    return BuildComparison(
        left=concept,
        right=value,
        operator=ComparisonOperator.EQ,
    )


def _and(*atoms) -> BuildConditional | BuildComparison:
    head = atoms[0]
    for atom in atoms[1:]:
        head = BuildConditional(left=head, right=atom, operator=BooleanOperator.AND)
    return head


class _StackNode:
    """Minimal stand-in for StrategyNode that satisfies validate_stack and
    _is_scalar_only. Real StrategyNodes require a fully-built environment plus
    parent resolution; we just need the attributes those two functions touch."""

    def __init__(
        self,
        output_concepts,
        preexisting_conditions=None,
        hidden_concepts=None,
        partial_concepts=None,
        virtual_output_concepts=None,
        label="node",
    ):
        self.output_concepts = list(output_concepts)
        self.preexisting_conditions = preexisting_conditions
        self.hidden_concepts = set(hidden_concepts or set())
        self.partial_concepts = list(partial_concepts or [])
        self.virtual_output_concepts = list(virtual_output_concepts or [])
        self._label = label

    def resolve(self):
        return SimpleNamespace(
            output_concepts=self.output_concepts,
            hidden_concepts=self.hidden_concepts,
        )

    def set_output_concepts(self, concepts, rebuild=True, change_visibility=True):
        self.output_concepts = list(concepts)
        return self

    def __str__(self):
        return f"_StackNode<{self._label}>"


# ---------------------------------------------------------------------------
# _is_scalar_only
# ---------------------------------------------------------------------------


class TestIsScalarOnly:
    def test_single_row_only_outputs(self):
        scalar = _concept("cmax", granularity=Granularity.SINGLE_ROW)
        node = _StackNode([scalar])
        assert _is_scalar_only(node) is True

    def test_multi_row_outputs(self):
        key = _concept("customer_id")
        node = _StackNode([key])
        assert _is_scalar_only(node) is False

    def test_mixed_outputs(self):
        scalar = _concept("cmax", granularity=Granularity.SINGLE_ROW)
        key = _concept("customer_id")
        node = _StackNode([scalar, key])
        assert _is_scalar_only(node) is False

    def test_no_visible_outputs(self):
        scalar = _concept("cmax", granularity=Granularity.SINGLE_ROW)
        node = _StackNode([scalar], hidden_concepts={scalar.address})
        # hidden-only outputs aren't a usable scalar provider — guard against
        # silently treating an empty visible set as "scalar".
        assert _is_scalar_only(node) is False

    def test_hidden_multi_row_not_counted(self):
        scalar = _concept("cmax", granularity=Granularity.SINGLE_ROW)
        hidden_multi = _concept("customer_id")
        node = _StackNode(
            [scalar, hidden_multi], hidden_concepts={hidden_multi.address}
        )
        assert _is_scalar_only(node) is True


# ---------------------------------------------------------------------------
# validate_stack — condition completion behavior with scalar nodes
# ---------------------------------------------------------------------------


class TestValidateStackScalarConditions:
    def test_scalar_with_stricter_preexisting_passes(self):
        """The bug: validation rejected this stack because the scalar's
        preexisting_conditions (CTE-internal WHERE) didn't equal the outer
        query's WHERE, even though the scalar is cross-joined as a constant."""
        cmax = _concept("cmax", granularity=Granularity.SINGLE_ROW)
        customer_id = _concept("customer_id")
        sales_channel = _concept("sales_channel")
        target_conditional = _and(
            _eq(customer_id, "x"),
            _eq(sales_channel, "STORE"),
        )
        # scalar's CTE filter is stricter — adds a date.year-style atom
        scalar_node = _StackNode(
            [cmax],
            preexisting_conditions=_and(
                _eq(customer_id, "x"),
                _eq(sales_channel, "STORE"),
                _eq(_concept("year"), "2000"),
            ),
            label="scalar",
        )
        multi_node = _StackNode(
            [customer_id],
            preexisting_conditions=target_conditional,
            label="multi",
        )
        result, _, _, _, _ = validate_stack(
            environment=BuildEnvironment(),
            stack=[scalar_node, multi_node],
            concepts=[cmax, customer_id],
            mandatory_with_filter=[cmax, customer_id, sales_channel],
            conditions=BuildWhereClause(conditional=target_conditional),
            accept_partial=False,
        )
        assert result == ValidationResult.COMPLETE

    def test_scalar_with_matching_preexisting_passes(self):
        cmax = _concept("cmax", granularity=Granularity.SINGLE_ROW)
        customer_id = _concept("customer_id")
        sales_channel = _concept("sales_channel")
        target_conditional = _and(
            _eq(customer_id, "x"),
            _eq(sales_channel, "STORE"),
        )
        scalar_node = _StackNode(
            [cmax],
            preexisting_conditions=target_conditional,
            label="scalar",
        )
        multi_node = _StackNode(
            [customer_id],
            preexisting_conditions=target_conditional,
            label="multi",
        )
        result, _, _, _, _ = validate_stack(
            environment=BuildEnvironment(),
            stack=[scalar_node, multi_node],
            concepts=[cmax, customer_id],
            mandatory_with_filter=[cmax, customer_id, sales_channel],
            conditions=BuildWhereClause(conditional=target_conditional),
            accept_partial=False,
        )
        assert result == ValidationResult.COMPLETE

    def test_multi_row_with_stricter_preexisting_does_not_pass(self):
        """Regression guard: the scalar exemption must not extend to multi-row
        nodes. A row-level node with a stricter pre-filter has *fewer* rows
        than the outer WHERE would produce, so we cannot accept it as
        already-satisfied."""
        cmax = _concept("cmax")  # NOT single-row
        customer_id = _concept("customer_id")
        sales_channel = _concept("sales_channel")
        target_conditional = _and(
            _eq(customer_id, "x"),
            _eq(sales_channel, "STORE"),
        )
        stricter = _and(
            _eq(customer_id, "x"),
            _eq(sales_channel, "STORE"),
            _eq(_concept("year"), "2000"),
        )
        node_a = _StackNode([cmax], preexisting_conditions=stricter, label="multi_a")
        node_b = _StackNode(
            [customer_id],
            preexisting_conditions=target_conditional,
            label="multi_b",
        )
        result, _, _, _, _ = validate_stack(
            environment=BuildEnvironment(),
            stack=[node_a, node_b],
            concepts=[cmax, customer_id],
            mandatory_with_filter=[cmax, customer_id, sales_channel],
            conditions=BuildWhereClause(conditional=target_conditional),
            accept_partial=False,
        )
        assert result == ValidationResult.INCOMPLETE_CONDITION

    def test_row_args_present_still_passes(self):
        """Existing fallback path: when condition row args are accessible at
        this level, validation passes regardless of preexisting_conditions."""
        cmax = _concept("cmax", granularity=Granularity.SINGLE_ROW)
        customer_id = _concept("customer_id")
        sales_channel = _concept("sales_channel")
        target_conditional = _and(
            _eq(customer_id, "x"),
            _eq(sales_channel, "STORE"),
        )
        # Neither node has preexisting_conditions, but both row args are in
        # the merged outputs so the loop can apply the WHERE here.
        node_a = _StackNode([cmax], label="a")
        node_b = _StackNode([customer_id, sales_channel], label="b")
        result, _, _, _, _ = validate_stack(
            environment=BuildEnvironment(),
            stack=[node_a, node_b],
            concepts=[cmax, customer_id, sales_channel],
            mandatory_with_filter=[cmax, customer_id, sales_channel],
            conditions=BuildWhereClause(conditional=target_conditional),
            accept_partial=False,
        )
        assert result == ValidationResult.COMPLETE


# ---------------------------------------------------------------------------
# RootNodeHandler._handle_expanded_node — preserves condition row args
# ---------------------------------------------------------------------------


class TestHandleExpandedNodePreservesRowArgs:
    def _context(self, conditions, concept, local_optional):
        return NodeGenerationContext(
            concept=concept,
            local_optional=local_optional,
            environment=BuildEnvironment(),
            g=None,  # type: ignore[arg-type]
            depth=0,
            source_concepts=lambda *a, **kw: None,  # type: ignore[arg-type]
            history=History(base_environment=Environment()),
            accept_partial=False,
            conditions=conditions,
        )

    def test_row_args_kept_when_conditions_present(self):
        """Bug 1: gen_merge_node returns a node containing the condition row
        arguments so the discovery loop can apply the WHERE on top.
        _handle_expanded_node must not strip those out — otherwise the next
        validate_stack call sees a node with no row args and no
        preexisting_conditions and trips the INCOMPLETE_CONDITION guardrail."""
        target = _concept("date")  # the ROOT concept being sourced
        optional = _concept("item_id")
        row_arg_a = _concept("sales_channel")
        row_arg_b = _concept("year")

        ctx = self._context(
            conditions=BuildWhereClause(
                conditional=_and(_eq(row_arg_a, "STORE"), _eq(row_arg_b, "2000"))
            ),
            concept=target,
            local_optional=[optional],
        )
        # Expanded node mirrors what gen_merge_node returns: target, optional,
        # plus the condition row args (and an unrelated extra that *should*
        # still get stripped).
        unrelated_extra = _concept("date_id")
        expanded = _StackNode(
            [target, optional, row_arg_a, row_arg_b, unrelated_extra],
            label="expanded",
        )

        RootNodeHandler(ctx)._handle_expanded_node(expanded, [target, optional])

        kept = {c.address for c in expanded.output_concepts}
        assert target.address in kept
        assert optional.address in kept
        assert row_arg_a.address in kept, (
            "condition row arg sales_channel must be preserved so the "
            "discovery loop can apply the WHERE"
        )
        assert row_arg_b.address in kept, (
            "condition row arg year must be preserved so the discovery loop "
            "can apply the WHERE"
        )
        assert (
            unrelated_extra.address not in kept
        ), "non-target, non-row-arg extras should still be stripped"

    def test_no_conditions_strips_everything_extra(self):
        """When no conditions are present, the original strip-down behavior
        applies — anything not in root_targets is removed."""
        target = _concept("date")
        optional = _concept("item_id")
        unrelated = _concept("unrelated")

        ctx = self._context(
            conditions=None,
            concept=target,
            local_optional=[optional],
        )
        expanded = _StackNode([target, optional, unrelated], label="expanded")

        RootNodeHandler(ctx)._handle_expanded_node(expanded, [target, optional])

        kept = {c.address for c in expanded.output_concepts}
        assert kept == {target.address, optional.address}
