"""Shared v4 helpers for fixed-point condition injection."""

from dataclasses import dataclass, field

from trilogy.core.enums import BooleanOperator
from trilogy.core.models.build import (
    BoolExpr,
    BuildConcept,
    BuildConditional,
    BuildGrain,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    decompose_condition,
)
from trilogy.core.processing.nodes import MergeNode, SelectNode, StrategyNode
from trilogy.utility import unique


@dataclass
class ConditionSources:
    row_concepts: list[BuildConcept] = field(default_factory=list)
    row_parents: list[StrategyNode] = field(default_factory=list)
    existence_concepts: list[BuildConcept] = field(default_factory=list)
    existence_parents: list[StrategyNode] = field(default_factory=list)


def has_existence_args(atom: BoolExpr) -> bool:
    return any(arg for group in atom.existence_arguments for arg in group)


def split_existence_atoms(
    conditions: BuildWhereClause | None,
) -> tuple[BuildWhereClause | None, BuildWhereClause | None]:
    if conditions is None:
        return None, None
    row_atoms: list[BoolExpr] = []
    existence_atoms: list[BoolExpr] = []
    for atom in decompose_condition(conditions.conditional):
        if has_existence_args(atom):
            existence_atoms.append(atom)
        else:
            row_atoms.append(atom)
    row_combined = combine_condition_atoms(row_atoms) if row_atoms else None
    ex_combined = combine_condition_atoms(existence_atoms) if existence_atoms else None
    return (
        (
            BuildWhereClause(conditional=row_combined)
            if row_combined is not None
            else None
        ),
        BuildWhereClause(conditional=ex_combined) if ex_combined is not None else None,
    )


def condition_row_args(conditions: BuildWhereClause | None) -> list[BuildConcept]:
    if conditions is None:
        return []
    return unique(list(conditions.row_arguments), "address")


def inject_condition_at_node(
    node: StrategyNode,
    condition: BuildWhereClause,
    output_concepts: list[BuildConcept],
    environment: BuildEnvironment,
    sources: ConditionSources,
    *,
    partial_concepts: list[BuildConcept] | None = None,
    grain: BuildGrain | None = None,
    hidden_concepts: set[str] | None = None,
    input_concepts: list[BuildConcept] | None = None,
    condition_on_merge: bool = False,
    combine_existing: bool = True,
) -> StrategyNode:
    if condition_on_merge:
        combined = (
            BuildConditional(
                left=node.conditions,
                right=condition.conditional,
                operator=BooleanOperator.AND,
            )
            if combine_existing and node.conditions
            else condition.conditional
        )
        return MergeNode(
            input_concepts=unique(
                list(
                    input_concepts
                    if input_concepts is not None
                    else node.usable_outputs
                )
                + sources.row_concepts,
                "address",
            ),
            output_concepts=output_concepts,
            environment=environment,
            parents=[node, *sources.row_parents, *sources.existence_parents],
            partial_concepts=(
                partial_concepts
                if partial_concepts is not None
                else list(node.partial_concepts)
            ),
            grain=grain,
            conditions=combined,
            hidden_concepts=hidden_concepts,
            existence_concepts=sources.existence_concepts,
        )

    base = node
    if sources.row_parents:
        merged_outputs = unique(
            list(base.output_concepts)
            + [
                output
                for parent in sources.row_parents
                for output in parent.output_concepts
            ],
            "address",
        )
        base = MergeNode(
            input_concepts=merged_outputs,
            output_concepts=merged_outputs,
            environment=environment,
            parents=[base, *sources.row_parents],
        )

    combined = (
        BuildConditional(
            left=base.conditions,
            right=condition.conditional,
            operator=BooleanOperator.AND,
        )
        if combine_existing and base.conditions
        else condition.conditional
    )
    input_concepts = unique(
        list(input_concepts if input_concepts is not None else base.usable_outputs)
        + sources.row_concepts,
        "address",
    )
    node_partials = (
        partial_concepts
        if partial_concepts is not None
        else list(base.partial_concepts)
    )
    if sources.existence_parents:
        return MergeNode(
            input_concepts=input_concepts,
            output_concepts=output_concepts,
            environment=environment,
            parents=[base, *sources.existence_parents],
            partial_concepts=node_partials,
            grain=grain,
            conditions=combined,
            hidden_concepts=hidden_concepts,
            existence_concepts=sources.existence_concepts,
        )
    return SelectNode(
        output_concepts=output_concepts,
        input_concepts=input_concepts,
        parents=[base],
        environment=environment,
        partial_concepts=node_partials,
        conditions=combined,
        grain=grain,
        hidden_concepts=hidden_concepts,
    )
