from trilogy.core.enums import BooleanOperator, Derivation
from trilogy.core.models.build import (
    BuildComparison,
    BuildConditional,
    BuildDatasource,
    BuildParenthetical,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    condition_implies,
    decompose_condition,
    flatten_conditions,
    is_scalar_condition,
    merge_conditions_and_dedup,
)

ConditionExpression = BuildComparison | BuildConditional | BuildParenthetical


def condition_atom_addresses(atom: ConditionExpression) -> set[str]:
    return {
        c.canonical_address
        for c in atom.row_arguments
        if c.derivation != Derivation.CONSTANT
    }


def datasource_conditions(
    datasource: BuildDatasource,
    conditions: BuildWhereClause | None,
    injected_conditions: ConditionExpression | None,
    partial_is_full: bool,
) -> ConditionExpression | None:
    datasource_conditions = datasource.where.conditional if datasource.where else None
    if injected_conditions and datasource_conditions:
        datasource_conditions = datasource_conditions + injected_conditions
    elif injected_conditions:
        datasource_conditions = injected_conditions

    if not conditions:
        return datasource_conditions

    ds_outputs = {c.canonical_address for c in datasource.output_concepts}
    covered_atoms: set[str] = set()
    if partial_is_full and datasource.non_partial_for:
        covered_atoms = {
            str(atom)
            for atom in decompose_condition(datasource.non_partial_for.conditional)
        }
    for atom in decompose_condition(conditions.conditional):
        if (
            str(atom) in covered_atoms
            or atom.existence_arguments
            or not is_scalar_condition(atom)
        ):
            continue
        if condition_atom_addresses(atom).issubset(ds_outputs):
            datasource_conditions = (
                merge_conditions_and_dedup(atom, datasource_conditions)
                if datasource_conditions
                else atom
            )
    return datasource_conditions


def preexisting_conditions(
    datasource: BuildDatasource,
    conditions: BuildWhereClause | None,
    partial_is_full: bool,
    satisfies_conditions: bool,
) -> ConditionExpression | None:
    if not conditions:
        return None
    # partial_is_full only means non_partial_for conditions are satisfied;
    # any extra conditions in the query must still be applied externally.
    if partial_is_full and datasource.non_partial_for:
        return datasource.non_partial_for.conditional
    if satisfies_conditions:
        return conditions.conditional
    return None


def covered_conditions(
    conditions: BuildWhereClause, environment: BuildEnvironment
) -> BuildWhereClause | None:
    """Return condition atoms covered by a datasource's complete_where."""
    query_condition = flatten_conditions(conditions.conditional)
    atoms = [flatten_conditions(atom) for atom in decompose_condition(query_condition)]
    atom_str_map = {str(a): a for a in atoms}
    preserved = []
    seen: set[str] = set()
    for ds in environment.datasources.values():
        if not isinstance(ds, BuildDatasource) or not ds.non_partial_for:
            continue
        if not condition_implies(query_condition, ds.non_partial_for.conditional):
            continue
        for np_atom in decompose_condition(ds.non_partial_for.conditional):
            key = str(flatten_conditions(np_atom))
            if key in atom_str_map and key not in seen:
                preserved.append(atom_str_map[key])
                seen.add(key)
    if not preserved:
        return None
    cond = preserved[0]
    for a in preserved[1:]:
        cond = BuildConditional(left=cond, right=a, operator=BooleanOperator.AND)
    return BuildWhereClause(conditional=cond)
