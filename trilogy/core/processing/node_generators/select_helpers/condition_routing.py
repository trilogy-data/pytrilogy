from enum import Enum, auto

from trilogy.core.enums import ComparisonOperator
from trilogy.core.models.build import (
    BoolExpr,
    BuildComparison,
    BuildConcept,
    BuildDatasource,
    BuildWhereClause,
)
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.processing.condition_utility import (
    combine_condition_atoms,
    condition_implies,
    condition_required_addresses,
    decompose_condition,
    flatten_conditions,
    is_null_literal,
    is_scalar_condition,
    merge_conditions_and_dedup,
    strip_condition_atoms,
)


class DatasourceConditionAtomState(Enum):
    KEEP = auto()
    ALWAYS_TRUE = auto()
    ALWAYS_FALSE = auto()


def datasource_condition_atom_state(
    datasource: BuildDatasource,
    atom: BoolExpr,
) -> DatasourceConditionAtomState:
    if not isinstance(atom, BuildComparison):
        return DatasourceConditionAtomState.KEEP
    if atom.operator not in (ComparisonOperator.IS, ComparisonOperator.IS_NOT):
        return DatasourceConditionAtomState.KEEP
    if isinstance(atom.left, BuildConcept) and is_null_literal(atom.right):
        concept = atom.left
    elif isinstance(atom.right, BuildConcept) and is_null_literal(atom.left):
        concept = atom.right
    else:
        return DatasourceConditionAtomState.KEEP

    non_nullable = {
        address
        for column in datasource.columns
        if not column.is_nullable
        for address in (column.concept.address, column.concept.canonical_address)
    }
    if (
        concept.address not in non_nullable
        and concept.canonical_address not in non_nullable
    ):
        return DatasourceConditionAtomState.KEEP
    if atom.operator == ComparisonOperator.IS_NOT:
        return DatasourceConditionAtomState.ALWAYS_TRUE
    return DatasourceConditionAtomState.ALWAYS_FALSE


def _is_null_test(atom: BoolExpr) -> BuildConcept | None:
    """The concept of a plain ``X IS NULL`` atom, else None."""
    if not isinstance(atom, BuildComparison) or atom.operator != ComparisonOperator.IS:
        return None
    if isinstance(atom.left, BuildConcept) and is_null_literal(atom.right):
        return atom.left
    if isinstance(atom.right, BuildConcept) and is_null_literal(atom.left):
        return atom.right
    return None


def absence_atoms(datasource: BuildDatasource, condition: BoolExpr) -> list[BoolExpr]:
    """Atoms of ``condition`` that test a column of an extension-licensed
    ``datasource`` for NULL.

    A scan binding a key ``~`` is the side an outer merge can leave absent, so
    ``x is null`` on its own column is a merge-level test: a row the scan
    filters out is indistinguishable from a row it never had, and pushing the
    atom in turns "x present" rows into "x absent" ones (a raw ``true``
    returns flag renders ``true is null`` and empties the scan, and every line
    looks unreturned). On a scan without ``~`` the atom is a plain value test
    and routes as before.
    """
    if not datasource.column_level_partial_addresses:
        return []
    bound = {
        address
        for column in datasource.columns
        for address in (column.concept.address, column.concept.canonical_address)
    }
    out: list[BoolExpr] = []
    for atom in decompose_condition(condition):
        concept = _is_null_test(atom)
        if concept is not None and (
            concept.address in bound or concept.canonical_address in bound
        ):
            out.append(atom)
    return out


def strip_atoms(condition: BoolExpr, atoms: list[BoolExpr]) -> BoolExpr | None:
    if not atoms:
        return condition
    combined = combine_condition_atoms(atoms)
    assert combined is not None
    return strip_condition_atoms(condition, combined)


def datasource_conditions(
    datasource: BuildDatasource,
    conditions: BuildWhereClause | None,
    injected_conditions: BoolExpr | None,
    partial_is_full: bool,
) -> BoolExpr | None:
    datasource_conditions = datasource.where.conditional if datasource.where else None
    if injected_conditions:
        injected_conditions = strip_atoms(
            injected_conditions, absence_atoms(datasource, injected_conditions)
        )
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
    absent = absence_atoms(datasource, conditions.conditional)
    for atom in decompose_condition(conditions.conditional):
        if (
            datasource_condition_atom_state(datasource, atom)
            == DatasourceConditionAtomState.ALWAYS_TRUE
            or atom in absent
        ):
            continue
        if (
            str(atom) in covered_atoms
            or any(arg for group in atom.existence_arguments for arg in group)
            or not is_scalar_condition(atom)
        ):
            continue
        if condition_required_addresses(atom).issubset(ds_outputs):
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
) -> BoolExpr | None:
    if not conditions:
        return None
    # partial_is_full only means non_partial_for conditions are satisfied;
    # any extra conditions in the query must still be applied externally.
    if partial_is_full and datasource.non_partial_for:
        return datasource.non_partial_for.conditional
    if satisfies_conditions:
        return strip_atoms(
            conditions.conditional, absence_atoms(datasource, conditions.conditional)
        )
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
    cond = combine_condition_atoms(preserved)
    if cond is None:
        return None
    return BuildWhereClause(conditional=cond)
