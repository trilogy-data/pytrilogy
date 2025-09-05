from collections import defaultdict
from pathlib import Path
from typing import Any

from trilogy import Environment, Executor
from trilogy.authoring import ConceptDeclarationStatement, Datasource
from trilogy.core.exceptions import (
    DatasourceColumnBindingData,
    DatasourceColumnBindingError,
)
from trilogy.core.validation.environment import validate_environment
from trilogy.parsing.render import Renderer


def rewrite_file_with_errors(
    statements: list[Any], errors: list[DatasourceColumnBindingError]
):
    renderer = Renderer()
    output = []
    ds_error_map: dict[str, list[DatasourceColumnBindingData]] = defaultdict(list)
    concept_error_map: dict[str, list[DatasourceColumnBindingData]] = defaultdict(list)
    for error in errors:
        if isinstance(error, DatasourceColumnBindingError):
            for x in error.errors:
                if error.dataset_address not in ds_error_map:
                    ds_error_map[error.dataset_address] = []
                # this is by dataset address
                if x.is_modifier_issue():
                    ds_error_map[error.dataset_address].append(x)
                # this is by column
                if x.is_type_issue():
                    concept_error_map[x.address].append(x)
    for statement in statements:
        if isinstance(statement, Datasource):
            if statement.identifier in ds_error_map:
                error_cols = ds_error_map[statement.identifier]
                for col in statement.columns:
                    if col.concept.address in [x.address for x in error_cols]:
                        error_col = [
                            x for x in error_cols if x.address == col.concept.address
                        ][0]
                        col.modifiers = list(
                            set(col.modifiers + error_col.value_modifiers)
                        )
        elif isinstance(statement, ConceptDeclarationStatement):
            if statement.concept.address in concept_error_map:
                error_cols = concept_error_map[statement.concept.address]
                statement.concept.datatype = error_cols[0].value_type
        output.append(statement)

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
