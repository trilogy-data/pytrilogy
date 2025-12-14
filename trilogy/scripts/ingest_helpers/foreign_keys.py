from pathlib import Path

from trilogy.authoring import (
    Comment,
    ConceptDeclarationStatement,
    Datasource,
    ImportStatement,
)
from trilogy.core.validation.fix import (
    DatasourceReferenceFix,
    rewrite_file_with_reference_merges,
)
from trilogy.scripts.display import print_error, print_info


def parse_foreign_keys(fks_str: str | None) -> dict[str, dict[str, str]]:
    if not fks_str:
        return {}

    fk_map: dict[str, dict[str, str]] = {}

    for fk_spec in fks_str.split(","):
        fk_spec = fk_spec.strip()
        if not fk_spec:
            continue

        try:
            source_part, target_part = fk_spec.split(":")
            source_table, source_column = source_part.rsplit(".", 1)
            target_table, target_column = target_part.rsplit(".", 1)

            if source_table not in fk_map:
                fk_map[source_table] = {}

            # Store as column -> table.column mapping
            fk_map[source_table][source_column] = f"{target_table}.{target_column}"

        except ValueError:
            from click.exceptions import Exit

            print_error(f"Invalid FK specification: {fk_spec}")
            print_error("Expected format: source_table.column:target_table.column")
            raise Exit(1)

    return fk_map


def apply_foreign_key_references(
    table_name: str,
    datasource: Datasource,
    datasources: dict[str, Datasource],
    script_content: list[
        Datasource | Comment | ConceptDeclarationStatement | ImportStatement
    ],
    column_mappings: dict[str, str],
) -> str:
    fk_imports: set[str] = set()
    reference_fixes: list[DatasourceReferenceFix] = []

    for source_column, target_ref in column_mappings.items():
        # Parse target reference: table.column
        target_table, _ = target_ref.rsplit(".", 1)
        target_datasource = datasources.get(target_table)
        target_concept = None
        if not target_datasource:
            continue
        # Find the concept for the target column
        for col_assign in target_datasource.columns:
            if col_assign.alias == target_ref.rsplit(".", 1)[1]:
                target_concept = col_assign.concept
                break

        # Find the source column's concept address
        source_concept = None
        for col_assign in datasource.columns:
            if col_assign.alias == source_column:
                source_concept = col_assign.concept.address
                break

        if not source_concept:
            print_error(f"Could not find column {source_column} in {table_name}")
            continue

        # Create the reference fix
        if target_concept:
            reference_fixes.append(
                DatasourceReferenceFix(
                    datasource_identifier=datasource.identifier,
                    column_address=source_concept,
                    column_alias=source_column,
                    reference_concept=target_concept.reference.with_namespace(
                        target_table
                    ),
                )
            )

            fk_imports.add(target_table)
            print_info(f"Linking {table_name}.{source_column} -> {target_ref}")

    # Add FK imports at the beginning (after comments)
    if fk_imports:
        # Find where to insert (after existing imports/comments)
        insert_pos = 0
        for i, stmt in enumerate(script_content):
            if isinstance(stmt, (Comment, ImportStatement)):
                insert_pos = i + 1
            else:
                break

        # Add FK imports
        for fk_import in sorted(fk_imports):
            script_content.insert(
                insert_pos,
                ImportStatement(
                    input_path=fk_import,
                    alias=fk_import,
                    path=Path(fk_import),
                ),
            )
            insert_pos += 1

    # Apply reference fixes to update datasource
    return rewrite_file_with_reference_merges(script_content, reference_fixes)
