from __future__ import annotations

from trilogy.parsing.v2.statement_plans import (
    ChartStatementPlan,
    CommentStatementPlan,
    ConceptStatementPlan,
    CopyStatementPlan,
    CreateStatementPlan,
    DatasourceStatementPlan,
    FunctionDefinitionPlan,
    ImportStatementPlan,
    MergeStatementPlan,
    MockStatementPlan,
    MultiSelectStatementPlan,
    PersistStatementPlan,
    PublishStatementPlan,
    RawSQLStatementPlan,
    RowsetStatementPlan,
    SelectStatementPlan,
    ShowStatementPlan,
    StatementPlan,
    TypeDeclarationPlan,
    UnsupportedStatementPlan,
    UnsupportedSyntaxError,
    ValidateStatementPlan,
)
from trilogy.parsing.v2.syntax import (
    SyntaxElement,
    SyntaxNode,
    SyntaxNodeKind,
    SyntaxToken,
    SyntaxTokenKind,
    syntax_name,
)

_IMPORT_KINDS = {
    SyntaxNodeKind.IMPORT_STATEMENT,
    SyntaxNodeKind.SELECTIVE_IMPORT_STATEMENT,
    SyntaxNodeKind.SELF_IMPORT_STATEMENT,
}

_FUNCTION_INNER_KINDS = {
    SyntaxNodeKind.RAW_FUNCTION,
    SyntaxNodeKind.TABLE_FUNCTION,
}

_PERSIST_KINDS = {
    SyntaxNodeKind.PERSIST_STATEMENT,
    SyntaxNodeKind.AUTO_PERSIST,
    SyntaxNodeKind.FULL_PERSIST,
}


def require_block_statement(block: SyntaxNode) -> SyntaxNode:
    """Return the statement node inside a BLOCK, raising on malformed blocks."""
    if not isinstance(block, SyntaxNode) or block.kind != SyntaxNodeKind.BLOCK:
        raise UnsupportedSyntaxError.from_syntax(
            f"Expected syntax node 'block', got '{syntax_name(block)}'",
            block,
        )
    statement = block.children[0]
    if not isinstance(statement, SyntaxNode):
        raise UnsupportedSyntaxError.from_syntax(
            f"Expected statement node in block, got token '{syntax_name(statement)}'",
            statement,
        )
    return statement


class StatementPlanner:
    def plan(self, forms: list[SyntaxElement]) -> list[StatementPlan]:
        return [self._plan_form(form) for form in forms]

    def _plan_form(self, form: SyntaxElement) -> StatementPlan:
        if isinstance(form, SyntaxToken):
            if form.kind == SyntaxTokenKind.COMMENT:
                return CommentStatementPlan(form)
            return UnsupportedStatementPlan(form)
        if form.kind == SyntaxNodeKind.SHOW_STATEMENT:
            return ShowStatementPlan(form)
        if form.kind == SyntaxNodeKind.SELECT_STATEMENT:
            return SelectStatementPlan(form)
        if form.kind == SyntaxNodeKind.MULTI_SELECT_STATEMENT:
            return MultiSelectStatementPlan(form)
        if form.kind in _IMPORT_KINDS:
            return ImportStatementPlan(form)
        if form.kind == SyntaxNodeKind.BLOCK:
            statement = require_block_statement(form)
            if statement.kind == SyntaxNodeKind.CONCEPT:
                return ConceptStatementPlan(syntax=form)
            if statement.kind == SyntaxNodeKind.FUNCTION:
                return self._plan_function_block(statement)
            if statement.kind in _IMPORT_KINDS:
                return ImportStatementPlan(statement)
            if statement.kind == SyntaxNodeKind.SELECT_STATEMENT:
                return SelectStatementPlan(statement)
            if statement.kind == SyntaxNodeKind.MULTI_SELECT_STATEMENT:
                return MultiSelectStatementPlan(statement)
            return self._plan_block_statement(statement)
        return UnsupportedStatementPlan(form)

    def _plan_function_block(self, statement: SyntaxNode) -> StatementPlan:
        inner = statement.children[0]
        if isinstance(inner, SyntaxNode) and inner.kind in _FUNCTION_INNER_KINDS:
            return FunctionDefinitionPlan(inner)
        return UnsupportedStatementPlan(statement)

    def _plan_block_statement(self, statement: SyntaxNode) -> StatementPlan:
        kind = statement.kind
        if kind == SyntaxNodeKind.DATASOURCE:
            return DatasourceStatementPlan(statement)
        if kind == SyntaxNodeKind.MERGE_STATEMENT:
            return MergeStatementPlan(statement)
        if kind == SyntaxNodeKind.ROWSET_DERIVATION_STATEMENT:
            return RowsetStatementPlan(statement)
        if kind in _PERSIST_KINDS:
            return PersistStatementPlan(statement)
        if kind == SyntaxNodeKind.RAWSQL_STATEMENT:
            return RawSQLStatementPlan(statement)
        if kind == SyntaxNodeKind.TYPE_DECLARATION:
            return TypeDeclarationPlan(statement)
        if kind == SyntaxNodeKind.CREATE_STATEMENT:
            return CreateStatementPlan(statement)
        if kind == SyntaxNodeKind.VALIDATE_STATEMENT:
            return ValidateStatementPlan(statement)
        if kind == SyntaxNodeKind.MOCK_STATEMENT:
            return MockStatementPlan(statement)
        if kind == SyntaxNodeKind.PUBLISH_STATEMENT:
            return PublishStatementPlan(statement)
        if kind == SyntaxNodeKind.COPY_STATEMENT:
            return CopyStatementPlan(statement)
        if kind == SyntaxNodeKind.CHART_STATEMENT:
            return ChartStatementPlan(statement)
        return UnsupportedStatementPlan(statement)
