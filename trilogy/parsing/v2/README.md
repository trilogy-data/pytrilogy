# Parser v2 Design

Parser v2 separates syntax parsing from semantic enrichment. The parser should produce
`SyntaxDocument` only; it should not read or mutate an `Environment`.

## Layers

1. `parse_engine_v2.py`
   - Owns the current parser adapter.
   - Today this adapter uses Lark to produce a neutral `SyntaxDocument`.
   - A future Rust parser should be able to produce the same syntax objects.

2. `syntax.py`
   - Defines `SyntaxDocument`, `SyntaxNode`, `SyntaxToken`, `SyntaxMeta`, and
     parser-neutral syntax kind enums.
   - These types are parser output, not semantic objects.
   - They must not know about concepts, datasources, imports, or environments.
   - Parser adapters translate their native labels into `SyntaxNodeKind` and
     `SyntaxTokenKind`. Keep raw parser names only as provenance/debug metadata.
   - Anonymous Lark tokens should get generic token kinds when v2 code needs to
     branch on them.

3. `hydration.py`
   - Coordinator for phase-based hydration of a `SyntaxDocument` against an
     `Environment`. Owns `NativeHydrator`, `HydrationContext`, `HydrationPhase`,
     the `NODE_HYDRATORS` / `TOKEN_HYDRATORS` registries, and `hydrate_rule` /
     `hydrate_token` dispatch.
   - Delegates plan construction to `statement_planner.py` and holds no plan
     classes or syntax-walking helpers of its own.
   - Phase-based so each top-level statement can be migrated without preserving
     the old transformer replay model.

4. `statement_planner.py`
   - Maps top-level `SyntaxElement`s to `StatementPlan` instances.
   - `StatementPlanner.plan(forms)` is the only dispatch surface; it replaces the
     large syntax-kind branch that previously lived on `NativeHydrator`.
   - Also exports `require_block_statement`, the shared BLOCK-unwrap helper.

5. `statement_plans.py`
   - Home of the explicit `StatementPlan` classes (`CommentStatementPlan`,
     `ConceptStatementPlan`, `ShowStatementPlan`, `ImportStatementPlan`,
     `SelectStatementPlan`, `FunctionDefinitionPlan`, `DatasourceStatementPlan`,
     `MergeStatementPlan`, `RowsetStatementPlan`, `PersistStatementPlan`,
     `RawSQLStatementPlan`, `UnsupportedStatementPlan`).
   - Also defines `StatementPlan`, `StatementPlanBase`, and
     `UnsupportedSyntaxError`. Unported statement families receive
     `UnsupportedStatementPlan`; they are not hydrated by recursively replaying
     syntax rules.

6. `symbols.py`
   - Syntax-walking helpers for concept discovery and dependency resolution:
     `find_concept_literals`, `extract_concept_name_from_literal`,
     `collect_inline_concept_addresses`, `collect_concept_address`,
     `collect_properties_addresses`, `extract_dependencies`, and
     `topological_sort_plans`.
   - Used by the concept/rowset/select plans during `collect_symbols` and `bind`.

7. `statements.py`
   - Contains syntax-node-first statement hydrators.
   - New top-level statement families should start here instead of adding another
     generic child-args transformer handler.

8. `function_syntax.py`
   - Typed view over `FUNCTION` / `RAW_FUNCTION` / `TABLE_FUNCTION` syntax nodes.
     `FunctionDefinitionPlan` uses it to extract parameter names for scope
     management.

9. `semantic_scope.py`
   - Defines `SemanticScope`, `SymbolDefinition`, `SymbolReference`, and
     `SymbolTable`, the stacked scope used by plans during `collect_symbols`
     and `bind`.
   - `SymbolTable.function_scope` and `SymbolTable.rowset_scope` replace the
     old environment-mutating `temporary_function_scope` / `temporary_rowset_scope`
     bridge; function parameters and rowset forward references now live in
     dedicated scopes instead of the global environment.

10. `import_service.py`
   - Owns recursive import parsing and the `ImportHydrationService` caches
     (`parsed_environments`, `text_lookup`, `import_keys`) that keep imports
     idempotent across nested parses.
   - Defines `ImportRequest`, the syntax-agnostic payload produced by
     `import_rules.py`. `ImportStatementPlan.load_imports` hands the request
     to `ImportHydrationService.execute`, which recursively parses the target,
     caches the result, and calls `environment.add_import(...)`.
   - Keeping this out of `import_rules.py` means the rule layer stays pure
     syntax-to-`ImportRequest` and never imports `NativeHydrator`.

11. Rule modules
   - `concept_rules.py`, `expression_rules.py`, and `token_rules.py` contain the
     currently ported rule-level semantic builders.
   - Each rule module owns its local `SyntaxNodeKind` or `SyntaxTokenKind` registry;
     `hydration.py` composes those registries directly.
   - Node rule functions take the source `SyntaxNode`, `RuleContext`, and a targeted
     child hydration callback. They should read the syntax shape they own rather than
     accepting generic pre-hydrated child args.

12. `model.py` / `rules_context.py`
   - `model.py` defines v2 diagnostics and the `RecordingEnvironmentUpdate` shape.
   - `rules_context.py` defines `RuleContext`, including the environment, function
     factory, source text, and current update.

## Hydration Phases

Each top-level syntax form becomes a `StatementPlan`. Plans run through these phases:

0. `load_imports`
   - Materialize imports before any other statement touches the environment.
   - Only `ImportStatementPlan` does work here; every other plan no-ops.
   - Imports run early because later statements reference imported concepts,
     functions, and datasources during `collect_symbols`, `bind`, and `hydrate`.
     Hiding this inside a generic `commit` (or `RecordingEnvironmentUpdate`
     replay) would make the ordering an implementation accident; naming the
     phase makes the materialization boundary explicit.

1. `collect_symbols`
   - Register declarations that other statements may refer to.
   - Examples: concept names, type names, function signatures, datasource names.
   - This should be a shallow declaration pass, not full expression hydration.

2. `bind`
   - Resolve names against the symbol table and import namespace.
   - Missing names should become explicit diagnostics or deliberate placeholders.

3. `hydrate`
   - Build semantic authoring objects such as `Concept`, `Function`, and statements.
   - This phase may use the `Environment`, but syntax parsing never should.

4. `validate`
   - Check types, purposes, grains, duplicate names, and statement-specific invariants.
   - Prefer dependency/worklist validation over replaying the syntax tree.

5. `commit`
   - Apply the final semantic result to the environment and return top-level output.
   - Long term this should behave like a small transaction boundary so failed hydration
     does not leave partial environment state behind.
   - Today `RecordingEnvironmentUpdate` records concept additions while still
     applying them immediately to preserve same-parse references. Moving those
     updates to true commit-time application is a planned follow-up once
     collect/bind are richer.

## Current Coverage

Current native coverage:

- comments
- concept declarations
- property declarations
- parameter declarations
- constant and simple concept derivations
- literals, arithmetic, and comparison expressions
- function definitions
- datasource declarations
- select / multi-select / persist / rowset / raw SQL statements
- merge statements
- imports (plain, selective, and self), resolved via `ImportHydrationService`
- `show concepts` / `show datasources`

Unported forms raise `UnsupportedSyntaxError`. That is deliberate. It prevents new v2
work from silently falling back to the old Lark transformer or relying on a `to_lark`
compatibility bridge.

The current expression and declaration helpers still recurse into child syntax where
their grammar requires it, such as ordered arithmetic chains. That recursion is
targeted by rule family and should stay below the statement boundary.

## Migration Rule

Port one top-level statement family at a time. Add or specialize a `StatementPlan`
for that family, then move the minimum expression helpers needed by that statement.
Keep `parse_engine.py` available for comparison tests until v2 coverage is complete.
