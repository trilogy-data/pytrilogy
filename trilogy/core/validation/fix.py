from dataclasses import dataclass
from pathlib import Path
from typing import Any

from trilogy import Environment, Executor
from trilogy.authoring import ConceptDeclarationStatement, Datasource
from trilogy.core.enums import Modifier
from trilogy.core.exceptions import DatasourceColumnBindingError
from trilogy.core.models.author import ConceptRef
from trilogy.core.models.core import (
    ArrayType,
    DataType,
    MapType,
    NumericType,
    StructType,
    TraitDataType,
)
from trilogy.core.models.datasource import ColumnAssignment
from trilogy.core.validation.environment import validate_environment
from trilogy.parsing.render import Renderer, safe_address


@dataclass
class DatasourceColumnFix:
    """Represents a fix to apply to a datasource column."""

    datasource_identifier: str
    column_address: str
    new_modifiers: list[Modifier] | None = None


@dataclass
class DatasourceReferenceFix:
    """Represents a fix to merge a datasource column with an imported reference."""

    datasource_identifier: str
    column_address: str
    column_alias: str
    reference_concept: ConceptRef


@dataclass
class ConceptTypeFix:
    """Represents a fix to update a concept's data type."""

    concept_address: str
    new_type: DataType | ArrayType | StructType | MapType | NumericType | TraitDataType


def update_datasource_column_modifiers(
    datasource: Datasource, column_address: str, new_modifiers: list[Modifier]
) -> None:
    """Generic method to update column modifiers in a datasource."""
    for col in datasource.columns:
        if col.concept.address == column_address:
            col.modifiers = list(set(col.modifiers + new_modifiers))


def process_validation_errors(
    errors: list[DatasourceColumnBindingError],
) -> tuple[
    list[DatasourceColumnFix], list[ConceptTypeFix], list[DatasourceReferenceFix]
]:
    """Process validation errors and generate a list of fixes to apply."""
    column_fixes: list[DatasourceColumnFix] = []
    concept_fixes: list[ConceptTypeFix] = []
    reference_fixes: list[DatasourceReferenceFix] = []

    for error in errors:
        if isinstance(error, DatasourceColumnBindingError):
            for x in error.errors:
                if x.is_modifier_issue():
                    column_fixes.append(
                        DatasourceColumnFix(
                            datasource_identifier=error.dataset_address,
                            column_address=x.address,
                            new_modifiers=x.value_modifiers,
                        )
                    )
                if x.is_type_issue():
                    concept_fixes.append(
                        ConceptTypeFix(
                            concept_address=x.address,
                            new_type=x.value_type,
                        )
                    )

    return column_fixes, concept_fixes, reference_fixes


def update_datasource_column_reference(
    datasource: Datasource, column_address: str, new_concept: ConceptRef
) -> None:
    """Update a datasource column to reference a different concept."""

    for i, col in enumerate(datasource.columns):
        if col.concept.address == column_address:
            # Create a new ColumnAssignment with the new concept reference
            new_col = ColumnAssignment(
                alias=col.alias,
                concept=new_concept,
                modifiers=col.modifiers,
            )
            datasource.columns[i] = new_col
            break


def apply_fixes_to_statements(
    statements: list[Any],
    column_fixes: list[DatasourceColumnFix],
    concept_fixes: list[ConceptTypeFix],
    reference_fixes: list[DatasourceReferenceFix],
) -> list[Any]:
    """Apply the generated fixes to the statement list."""
    output = []

    # Track which concept addresses are being replaced by references
    replaced_concept_addresses = {
        fix.column_address: fix.reference_concept for fix in reference_fixes
    }

    for statement in statements:
        if isinstance(statement, Datasource):
            for col_fix in column_fixes:
                if (
                    statement.identifier == col_fix.datasource_identifier
                    and col_fix.new_modifiers
                ):
                    update_datasource_column_modifiers(
                        statement, col_fix.column_address, col_fix.new_modifiers
                    )

            for ref_fix in reference_fixes:
                if statement.identifier == ref_fix.datasource_identifier:
                    update_datasource_column_reference(
                        statement,
                        ref_fix.column_address,
                        ref_fix.reference_concept,
                    )
                new_grain = set()
                for x in statement.grain.components:
                    if safe_address(x) in replaced_concept_addresses:
                        new_grain.add(
                            replaced_concept_addresses[safe_address(x)].address
                        )
                    else:
                        new_grain.add(x)
                statement.grain.components = new_grain

        elif isinstance(statement, ConceptDeclarationStatement):
            # Skip concept declarations that are being replaced by references
            if statement.concept.address in replaced_concept_addresses:
                continue
            new_keys = set()
            replace_keys = False

            for x in statement.concept.keys or set():
                if safe_address(x) in replaced_concept_addresses:
                    replace_keys = True
                    new_keys.add(replaced_concept_addresses[safe_address(x)].address)
                else:
                    new_keys.add(x)
            if replace_keys:
                statement.concept.keys = new_keys
            for concept_fix in concept_fixes:
                if statement.concept.address == concept_fix.concept_address:
                    statement.concept.datatype = concept_fix.new_type

        output.append(statement)

    return output


def rewrite_file_with_errors(
    statements: list[Any],
    errors: list[DatasourceColumnBindingError],
    additional_reference_fixes: list[DatasourceReferenceFix] | None = None,
):
    renderer = Renderer()

    column_fixes, concept_fixes, reference_fixes = process_validation_errors(errors)

    # Add any additional reference fixes provided
    if additional_reference_fixes:
        reference_fixes.extend(additional_reference_fixes)

    output = apply_fixes_to_statements(
        statements, column_fixes, concept_fixes, reference_fixes
    )

    return renderer.render_statement_string(output)


def rewrite_file_with_reference_merges(
    statements: list[Any], reference_fixes: list[DatasourceReferenceFix]
) -> str:
    renderer = Renderer()

    output = apply_fixes_to_statements(statements, [], [], reference_fixes)

    return renderer.render_statement_string(output)


DEPTH_CUTOFF = 3


def validate_and_rewrite(
    input: Path | str, exec: Executor | None = None, depth: int = 0
) -> str | None:
    if depth > DEPTH_CUTOFF:
        print(f"Reached depth cutoff of {DEPTH_CUTOFF}, stopping.")
        return None
    if isinstance(input, str):
        raw = input
        env = Environment()
    else:
        with open(input, "r") as f:
            raw = f.read()
        env = Environment(working_path=input.parent)
    if exec:
        env = exec.environment
    env, statements = env.parse(raw)

    validation_results = validate_environment(env, exec=exec, generate_only=True)

    errors = [
        x.result
        for x in validation_results
        if isinstance(x.result, DatasourceColumnBindingError)
    ]

    if not errors:
        print("No validation errors found")
        return None
    print(
        f"Found {len(errors)} validation errors, attempting to fix, current depth: {depth}..."
    )
    for error in errors:
        for item in error.errors:
            print(f"- {item.format_failure()}")

    new_text = rewrite_file_with_errors(statements, errors)

    while iteration := validate_and_rewrite(new_text, exec=exec, depth=depth + 1):
        depth = depth + 1
        if depth >= DEPTH_CUTOFF:
            break
        if iteration:
            new_text = iteration
        depth += 1
    if isinstance(input, Path):
        with open(input, "w") as f:
            f.write(new_text)
        return None
    else:
        return new_text
