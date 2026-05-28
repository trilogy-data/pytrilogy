from pathlib import Path

from trilogy.authoring import (
    Comment,
    ConceptDeclarationStatement,
    Datasource,
    ImportStatement,
    PropertiesDeclarationStatement,
)
from trilogy.core.enums import Modifier
from trilogy.core.validation.fix import (
    DatasourceReferenceFix,
    rewrite_file_with_reference_merges,
)
from trilogy.scripts.display import print_error, print_info
from trilogy.scripts.ingest_helpers.fk_inference import FKBinding


def parse_foreign_keys(fks_str: str | None) -> dict[str, dict[str, FKBinding]]:
    """Parse the ``--fks`` CLI argument into per-table column->FKBinding maps.

    Explicit FKs arrive with no data evidence, so each binding defaults to
    ``partial=True`` (the safe assumption: the child may not cover every
    parent key). ``enrich_explicit_fks_partial`` may later flip individual
    bindings to ``partial=False`` once it has run reverse-coverage sniffs.
    """
    if not fks_str:
        return {}

    fk_map: dict[str, dict[str, FKBinding]] = {}

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

            fk_map[source_table][source_column] = FKBinding(
                target_ref=f"{target_table}.{target_column}", partial=True
            )

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
        Datasource
        | Comment
        | ConceptDeclarationStatement
        | PropertiesDeclarationStatement
        | ImportStatement
    ],
    column_mappings: dict[str, FKBinding],
) -> str:
    # (input_path, import_alias) — multiple aliases of the same dim become
    # separate imports (role-playing dimensions).
    fk_imports: set[tuple[str, str]] = set()
    reference_fixes: list[DatasourceReferenceFix] = []

    for source_column, binding in column_mappings.items():
        # Parse target reference: "table.column" or "table.column@alias".
        # The optional ``@alias`` is set by inference for role-playing dims;
        # explicit --fks entries never carry it.
        target_ref = binding.target_ref
        role_alias: str | None = None
        if "@" in target_ref:
            target_ref, role_alias = target_ref.rsplit("@", 1)
        target_table, target_col = target_ref.rsplit(".", 1)
        import_alias = role_alias or target_table

        target_datasource = datasources.get(target_table)
        target_concept = None
        if not target_datasource:
            continue
        # Find the concept for the target column
        for col_assign in target_datasource.columns:
            if col_assign.alias == target_col:
                target_concept = col_assign.concept
                break

        # Find the source column's concept address. Mark it partial only when
        # the binding says so — reverse-coverage sniffing has shown that some
        # parent keys are missing from this column, so the source datasource
        # cannot stand in as a complete source of the parent concept.
        source_concept = None
        for col_assign in datasource.columns:
            if col_assign.alias == source_column:
                source_concept = col_assign.concept.address
                if binding.partial and Modifier.PARTIAL not in col_assign.modifiers:
                    col_assign.modifiers.append(Modifier.PARTIAL)
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
                        import_alias
                    ),
                )
            )

            fk_imports.add((target_table, import_alias))
            parts = []
            if role_alias:
                parts.append(f"as `{role_alias}`")
            parts.append("partial" if binding.partial else "complete")
            suffix = f" ({', '.join(parts)})"
            print_info(f"Linking {table_name}.{source_column} -> {target_ref}{suffix}")

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
        for input_path, alias in sorted(fk_imports):
            script_content.insert(
                insert_pos,
                ImportStatement(
                    input_path=input_path,
                    alias=alias,
                    path=Path(input_path),
                ),
            )
            insert_pos += 1

    # Apply reference fixes to update datasource
    return rewrite_file_with_reference_merges(script_content, reference_fixes)
