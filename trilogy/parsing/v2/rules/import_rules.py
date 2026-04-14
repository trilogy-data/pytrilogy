from __future__ import annotations

from logging import getLogger
from os.path import join
from pathlib import Path

from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    FileSystemImportResolver,
)
from trilogy.core.statements.author import ImportStatement
from trilogy.parsing.v2.import_service import ImportRequest
from trilogy.parsing.v2.rules_context import (
    HydrateFunction,
    NodeHydrator,
    RuleContext,
    hydrated_children,
)
from trilogy.parsing.v2.syntax import SyntaxNode, SyntaxNodeKind

perf_logger = getLogger("trilogy.parse.performance")

STDLIB_ROOT = Path(__file__).parent.parent.parent.parent


def _resolve_import_path(
    raw_args: list[str],
    environment: Environment,
) -> tuple[str, str, str, str, Path | str, bool]:
    parent_dirs = -1
    parsed_args: list[str] = []
    for x in raw_args:
        if x == ".":
            parent_dirs += 1
        else:
            parsed_args.append(str(x))
    parent_dirs = max(parent_dirs, 0)
    if len(parsed_args) == 2:
        alias = parsed_args[-1]
        cache_key = parsed_args[-1]
    else:
        alias = environment.namespace
        cache_key = parsed_args[0]
    input_path = parsed_args[0]
    path = input_path.split(".")
    is_stdlib = path[0] == "std"
    if is_stdlib:
        target = join(STDLIB_ROOT, *path) + ".preql"
        token_lookup: Path | str = Path(target)
    elif isinstance(environment.config.import_resolver, FileSystemImportResolver):
        troot = Path(environment.working_path)
        for _ in range(parent_dirs):
            troot = troot.parent
        target = join(troot, *path) + ".preql"
        token_lookup = Path(target)
    elif isinstance(environment.config.import_resolver, DictImportResolver):
        target = ".".join(path)
        token_lookup = target
    else:
        raise NotImplementedError
    return alias, cache_key, input_path, target, token_lookup, is_stdlib


def import_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ImportRequest:
    args = [str(hydrate(child)) for child in node.children]
    alias, cache_key, input_path, target, token_lookup, is_stdlib = (
        _resolve_import_path(args, context.environment)
    )
    return ImportRequest(
        alias=alias,
        cache_key=cache_key,
        input_path=input_path,
        target=target,
        token_lookup=token_lookup,
        is_stdlib=is_stdlib,
    )


def selective_import_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ImportRequest:
    args = hydrated_children(node, hydrate)
    concepts_list: list[str] = next(a for a in args if isinstance(a, list))
    path_args = [str(a) for a in args if not isinstance(a, list)]
    alias, cache_key, input_path, target, token_lookup, is_stdlib = (
        _resolve_import_path(path_args, context.environment)
    )
    return ImportRequest(
        alias=alias,
        cache_key=cache_key,
        input_path=input_path,
        target=target,
        token_lookup=token_lookup,
        is_stdlib=is_stdlib,
        concepts=concepts_list,
    )


def self_import_statement(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> ImportStatement:
    args = [str(hydrate(child)) for child in node.children]
    alias = args[-1]
    environment = context.environment
    is_dict_resolver = isinstance(
        environment.config.import_resolver, DictImportResolver
    )
    path: Path
    if is_dict_resolver:
        input_path = "."
        path = Path(".")
    elif environment.env_file_path is not None:
        env_file_path = environment.env_file_path
        if isinstance(env_file_path, str):
            env_file_path = Path(env_file_path)
        input_path = str(env_file_path.stem)
        path = env_file_path
    else:
        raise ImportError("Cannot use 'self import' without a file path context.")
    # Register the alias for deferred-placeholder fallbacks during datasource /
    # concept hydration (``parent.id`` → UndefinedConceptFull) and stage the
    # self-import for materialization after the current parse's concepts and
    # datasources are durable. See SemanticState._pending_self_imports.
    context.semantic_state.add_deferred_import_alias(alias)
    context.semantic_state.add_pending_self_import(alias, path)
    return ImportStatement(
        alias=alias,
        input_path=input_path,
        path=path,
        is_self=True,
    )


def import_concepts(
    node: SyntaxNode,
    context: RuleContext,
    hydrate: HydrateFunction,
) -> list[str]:
    return [str(hydrate(child)) for child in node.children]


IMPORT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = {
    SyntaxNodeKind.IMPORT_CONCEPTS: import_concepts,
}
