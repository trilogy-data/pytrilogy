from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from os.path import dirname
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from trilogy.constants import Parsing
from trilogy.core.models.environment import (
    DEFAULT_NAMESPACE,
    DictImportResolver,
    Environment,
    FileSystemImportResolver,
    Import,
    NamespaceProjection,
    build_namespace_projection,
)
from trilogy.core.statements.author import ImportStatement
from trilogy.utility import safe_open

if TYPE_CHECKING:
    from trilogy.parsing.v2.semantic_state import SemanticState


@dataclass
class ImportRequest:
    alias: str
    cache_key: str
    input_path: str
    target: str
    token_lookup: Path | str
    is_stdlib: bool = False
    concepts: list[str] | None = None
    leading_dots: int = 0
    # same-line trailing comment on the import statement (set by the planner)
    description: str | None = None


def _suggest_import_paths(
    missing_basename: str, search_root: Path, max_depth: int = 4, max_hits: int = 5
) -> list[str]:
    """Return dotted import paths for .preql files matching ``missing_basename``
    under ``search_root``. Used to turn ``[Errno 2] No such file: store_sales.preql``
    into ``Did you mean: raw.store_sales?`` — a downward scan only, no `..`
    upward traversal (per agent-eval feedback, upward paths produced noisier
    suggestions than they were worth)."""
    if not search_root.exists() or not search_root.is_dir():
        return []
    target = f"{missing_basename}.preql"
    hits: list[str] = []
    try:
        for path in search_root.rglob(target):
            try:
                rel = path.relative_to(search_root).with_suffix("")
            except ValueError:
                continue
            if len(rel.parts) > max_depth:
                continue
            # Skip hidden / dunder / venv-ish dirs so the suggestion isn't
            # polluted by `__pycache__`, `.git`, eval worker copies, etc.
            if any(
                part.startswith((".", "_")) or part in ("node_modules", "venv")
                for part in rel.parts
            ):
                continue
            hits.append(".".join(rel.parts))
            if len(hits) >= max_hits * 4:  # bail before the disk burns
                break
    except OSError:
        return []
    # Stable sort + dedupe; closer (fewer path parts) first, then alphabetical.
    return sorted(set(hits), key=lambda p: (p.count("."), p))[:max_hits]


def _read_import_text(
    address: str, environment: Environment, is_stdlib: bool = False
) -> str:
    resolver = environment.config.import_resolver
    if isinstance(resolver, FileSystemImportResolver) or is_stdlib:
        try:
            with safe_open(address) as f:
                return f.read()
        except OSError as exc:
            if is_stdlib:
                raise
            missing_basename = Path(address).stem
            suggestions = _suggest_import_paths(
                missing_basename, Path(environment.working_path)
            )
            hint = f" Did you mean: {', '.join(suggestions)}?" if suggestions else ""
            raise ImportError(f"Unable to import '{address}': {exc}.{hint}") from exc
    if isinstance(resolver, DictImportResolver):
        key = resolver.resolve(address)
        if key is None:
            raise ImportError(
                f"Unable to import file {address}, not resolvable from provided source files."
            )
        return resolver.content[key]
    raise ImportError(
        f"Unable to import file {address}, resolver type "
        f"{type(resolver)} not supported"
    )


# Parse-local cache key for parsed import environments: (canonical target,
# config root). Filesystem targets are absolute paths; dict-resolver targets
# are the resolver's canonical `content` key, so a file reached relatively
# from one importer and absolutely from another parses once.
ImportEnvCacheKey = tuple[str, str | None]


class ClosureKey(NamedTuple):
    """One text an import environment depends on: an absolute path read off
    disk, or a dict resolver's canonical content key."""

    on_disk: bool
    target: str


# ---------------------------------------------------------------------------
# Cross-parse import environment store.
#
# A parsed import file's Environment is a pure function of the resolved
# target, the parse-relevant config (duplicate-declaration flag, import search
# paths, parameter values) and the CONTENT of the file plus everything it
# transitively imports. Entries are validated on reuse by re-hashing that text
# closure through the importing env's resolver and by an env-integrity stamp
# that catches post-parse mutation of the cached environment through shared
# objects (bare imports share objects with the importing environment; see
# Environment.add_import). Dict-resolver targets are not unique across
# resolvers, so their keys carry the target's own text hash: models that share
# an address coexist instead of evicting each other.
# ---------------------------------------------------------------------------


@dataclass
class _ClosureFrame:
    """Per-import-parse dependency recorder: closure key -> hash(text) for
    the file and everything parsed beneath it. `tainted` marks a parse whose
    result is not context-free (cycle/depth stub baked in) and must never
    enter the process-wide store."""

    deps: dict[ClosureKey, int] = field(default_factory=dict)
    tainted: bool = False


@dataclass
class _ImportEnvEntry:
    env: Environment
    closure: dict[ClosureKey, int]
    integrity: tuple
    # `with_namespace(alias)` products of `env`, keyed by alias. Only valid
    # while `env` itself is — they are dropped with the entry. Each is
    # re-stamped on reuse because an importer that received one can edit the
    # shared datasources in place (see NamespaceProjection.integrity).
    # Projection and stamp live in ONE dict so publishing them is a single
    # atomic write: this store is process-global and a directory run parses on
    # a thread pool, so two dicts tear — a reader sees the projection before
    # its stamp exists and raises KeyError(alias).
    projections: dict[str, tuple[NamespaceProjection, tuple]] = field(
        default_factory=dict
    )

    def projection_for(self, alias: str) -> NamespaceProjection:
        cached = self.projections.get(alias)
        if cached is not None and cached[1] == cached[0].integrity():
            return cached[0]
        built = build_namespace_projection(self.env, alias)
        self.projections[alias] = (built, built.integrity())
        return built


_IMPORT_ENV_STORE: OrderedDict[tuple, _ImportEnvEntry] = OrderedDict()
_IMPORT_ENV_STORE_MAX = 128
IMPORT_ENV_STORE_ENABLED = True
# A directory run parses on a thread pool against this one global store, so the
# LRU bookkeeping needs a lock: move_to_end/popitem raise KeyError on a key
# another thread just evicted. Guards only the OrderedDict ops, never the
# closure re-hash (which reads files).
_IMPORT_ENV_STORE_LOCK = threading.Lock()


def clear_import_env_store() -> None:
    with _IMPORT_ENV_STORE_LOCK:
        _IMPORT_ENV_STORE.clear()


def set_import_env_store_max(size: int) -> None:
    """Size the store for the deployment: each entry holds a whole parsed
    Environment plus its namespaced projections, and a multi-tenant service
    fills it with one entry per (model file, content)."""
    global _IMPORT_ENV_STORE_MAX
    with _IMPORT_ENV_STORE_LOCK:
        _IMPORT_ENV_STORE_MAX = size
        while len(_IMPORT_ENV_STORE) > size:
            _IMPORT_ENV_STORE.popitem(last=False)


def _params_fingerprint(parameters: dict) -> tuple:
    return tuple(sorted((k, repr(v)) for k, v in parameters.items()))


def _env_integrity(env: Environment) -> tuple:
    """Detects post-parse mutation of a cached child env: dict writes via the
    mutation counters, plus in-place datasource edits (status flips at
    publish/persist, column strips on persisted-concept redeclaration) that
    never touch a dict."""
    return (
        env.concepts.mutations,
        env.datasources.mutations,
        tuple(
            sorted(
                (k, d.status.value, len(d.columns)) for k, d in env.datasources.items()
            )
        ),
    )


def _store_lookup(
    key: tuple,
    own_key: ClosureKey,
    own_hash: int,
    read_text: Callable[[ClosureKey], str | None],
) -> _ImportEnvEntry | None:
    with _IMPORT_ENV_STORE_LOCK:
        entry = _IMPORT_ENV_STORE.get(key)
    if entry is None:
        return None
    valid = entry.closure.get(own_key) == own_hash
    if valid:
        for dep, expected in entry.closure.items():
            if dep == own_key:
                continue
            text = read_text(dep)
            if text is None or hash(text) != expected:
                valid = False
                break
    if valid and _env_integrity(entry.env) != entry.integrity:
        valid = False
    with _IMPORT_ENV_STORE_LOCK:
        if not valid:
            _IMPORT_ENV_STORE.pop(key, None)
            return None
        if key in _IMPORT_ENV_STORE:
            _IMPORT_ENV_STORE.move_to_end(key)
    return entry


def _store_fill(key: tuple, entry: _ImportEnvEntry) -> None:
    with _IMPORT_ENV_STORE_LOCK:
        _IMPORT_ENV_STORE[key] = entry
        _IMPORT_ENV_STORE.move_to_end(key)
        while len(_IMPORT_ENV_STORE) > _IMPORT_ENV_STORE_MAX:
            _IMPORT_ENV_STORE.popitem(last=False)


@dataclass
class ImportHydrationService:
    """Owns recursive import parsing and the shared caches that make it idempotent."""

    environment: Environment
    parse_config: Parsing | None = None
    max_parse_depth: int = 10
    parsed_environments: dict[ImportEnvCacheKey, Environment] = field(
        default_factory=dict
    )
    text_lookup: dict[Path | str, str] = field(default_factory=dict)
    import_keys: list[str] = field(default_factory=list)
    # Imports whose parse is currently on the call stack. Used to break
    # circular imports at re-entry rather than recursing until max_parse_depth.
    # Shared by reference across child ImportHydrationServices via
    # HydrationContext so cycle detection sees the full parse stack.
    in_flight_imports: set[ImportEnvCacheKey] = field(default_factory=set)
    # Closure recording for the cross-parse store: per-parse dependency frames
    # (shared across child services like in_flight_imports) plus the recorded
    # closure for each local parsed_environments entry.
    closure_stack: list[_ClosureFrame] = field(default_factory=list)
    local_closures: dict[ImportEnvCacheKey, _ClosureFrame] = field(default_factory=dict)
    # True while parsing inside a stdlib file; propagated to child parses so
    # nested sibling imports stay stdlib regardless of the import resolver.
    in_stdlib: bool = False
    # Parser-local SemanticState. When a cycle is detected, the broken
    # alias is registered here so ConceptLookup can generate narrow
    # UndefinedConceptFull placeholders for datasource columns referencing
    # concepts in that in-flight namespace. Optional for back-compat with
    # direct ImportHydrationService construction in tests.
    semantic_state: SemanticState | None = None

    def set_text(self, key: Path | str, text: str) -> None:
        self.text_lookup[key] = text

    def _read_closure_text(self, key: ClosureKey) -> str | None:
        if key.on_disk:
            path = Path(key.target)
            text = self.text_lookup.get(path)
            if text is None:
                try:
                    with safe_open(key.target) as f:
                        text = f.read()
                except OSError:
                    return None
                self.text_lookup[path] = text
            return text
        resolver = self.environment.config.import_resolver
        if not isinstance(resolver, DictImportResolver):
            return None
        return resolver.content.get(key.target)

    def execute(self, request: ImportRequest) -> ImportStatement:
        from trilogy.parsing.parse_engine_v2 import parse_syntax
        from trilogy.parsing.v2.hydration import HydrationContext, NativeHydrator

        environment = self.environment
        resolver = environment.config.import_resolver
        key_path = self.import_keys + [request.cache_key]
        cache_lookup = "-".join(key_path)
        is_dict = not request.is_stdlib and isinstance(resolver, DictImportResolver)
        # Cache parsed import environments by canonical target + config root
        # rather than the alias chain: the parsed env is namespace-neutral and
        # identical regardless of which alias imports it, so a file reachable
        # via multiple import paths parses exactly once. add_import still
        # applies the per-edge namespace downstream.
        target = str(request.target)
        if is_dict:
            assert isinstance(resolver, DictImportResolver)
            canonical = resolver.resolve(target)
            if canonical is None:
                raise ImportError(
                    f"Unable to import file {target}, not resolvable from provided source files."
                )
            target = canonical
        root = None
        if "." in target:
            root = target.rsplit(".", 1)[0]
        env_cache_key: ImportEnvCacheKey = (target, root)

        # Cycle detection: a parse currently on the stack re-encounters
        # itself. Break by returning a stub ImportStatement and registering
        # the alias as a deferred namespace; downstream concept lookups in
        # this parser produce partial placeholders via ConceptLookup rather
        # than recursing until max_parse_depth and failing.
        if env_cache_key in self.in_flight_imports:
            if self.semantic_state is not None:
                self.semantic_state.add_deferred_import_alias(request.alias)
            if self.closure_stack:
                self.closure_stack[-1].tainted = True
            return ImportStatement(
                alias=request.alias,
                input_path=request.input_path,
                path=Path(request.target),
            )

        if len(key_path) > self.max_parse_depth:
            if self.closure_stack:
                self.closure_stack[-1].tainted = True
            return ImportStatement(
                alias=request.alias,
                input_path=request.input_path,
                path=Path(request.target),
            )

        if is_dict:
            assert isinstance(resolver, DictImportResolver)
            text = resolver.content[target]
        elif request.token_lookup in self.text_lookup:
            text = self.text_lookup[request.token_lookup]
        else:
            text = _read_import_text(request.target, environment, request.is_stdlib)
            self.text_lookup[request.token_lookup] = text
        own_hash = hash(text)
        own_key = ClosureKey(not is_dict, target)

        use_store = IMPORT_ENV_STORE_ENABLED and (
            request.is_stdlib
            or isinstance(resolver, (FileSystemImportResolver, DictImportResolver))
        )
        # The active deployment env is part of the key: its transform rewrites
        # datasource Addresses in place, and those Address objects are shared
        # into every namespaced copy, so an env-prefixed parse must not hand
        # its entry to an unprefixed one.
        from trilogy.execution.envs import active_env

        activation = active_env()
        store_key = (
            target,
            root,
            own_hash if is_dict else None,
            environment.config.allow_duplicate_declaration,
            tuple(str(p) for p in environment.import_paths),
            _params_fingerprint(environment.parameters),
            activation.name if activation else None,
        )

        store_entry: _ImportEnvEntry | None = None
        if env_cache_key in self.parsed_environments:
            new_env = self.parsed_environments[env_cache_key]
            frame = self.local_closures.get(env_cache_key)
            if self.closure_stack and frame is not None:
                self.closure_stack[-1].deps.update(frame.deps)
                self.closure_stack[-1].tainted |= frame.tainted
            # Re-importing a file already parsed in THIS parse (a second alias
            # for it, or a diamond in the import graph). Recover the store entry
            # so the per-alias projections still cache — identity against the
            # env we already hold is the validation, so no closure re-hash.
            local = _IMPORT_ENV_STORE.get(store_key)
            if local is not None and local.env is new_env:
                store_entry = local
        elif use_store and (
            entry := _store_lookup(
                store_key, own_key, own_hash, self._read_closure_text
            )
        ):
            store_entry = entry
            new_env = entry.env
            self.parsed_environments[env_cache_key] = new_env
            self.local_closures[env_cache_key] = _ClosureFrame(deps=dict(entry.closure))
            if self.closure_stack:
                self.closure_stack[-1].deps.update(entry.closure)
        else:
            self.in_flight_imports.add(env_cache_key)
            frame = _ClosureFrame(tainted=not use_store)
            self.closure_stack.append(frame)
            try:
                document = parse_syntax(text)
                # Parameters are snapshotted: the importer's dict keeps being
                # written by set_parameters after the parse, and a stored child
                # env must not observe values its key never fingerprinted.
                new_env = Environment(
                    working_path=dirname(request.target),
                    import_paths=list(environment.import_paths),
                    env_file_path=request.token_lookup,
                    config=environment.config.copy_for_root(root=root),
                    parameters=dict(environment.parameters),
                )
                child_context = HydrationContext(
                    environment=new_env,
                    parse_address=cache_lookup,
                    token_address=request.token_lookup,
                    parse_config=self.parse_config,
                    max_parse_depth=self.max_parse_depth,
                    parsed_environments=self.parsed_environments,
                    text_lookup=self.text_lookup,
                    import_keys=key_path,
                    in_flight_imports=self.in_flight_imports,
                    closure_stack=self.closure_stack,
                    local_closures=self.local_closures,
                    in_stdlib=request.is_stdlib or self.in_stdlib,
                )
                NativeHydrator(child_context).parse(document)
                self.parsed_environments[env_cache_key] = new_env
            except Exception as e:
                raise ImportError(
                    f"Unable to import '{request.target}', parsing error: {e}"
                ) from e
            finally:
                self.in_flight_imports.discard(env_cache_key)
                self.closure_stack.pop()
            frame.deps[own_key] = own_hash
            self.local_closures[env_cache_key] = frame
            if use_store and not frame.tainted:
                store_entry = _ImportEnvEntry(
                    env=new_env,
                    closure=dict(frame.deps),
                    integrity=_env_integrity(new_env),
                )
                _store_fill(store_key, store_entry)
            if self.closure_stack:
                self.closure_stack[-1].deps.update(frame.deps)
                self.closure_stack[-1].tainted |= frame.tainted

        is_file_resolver = isinstance(resolver, FileSystemImportResolver)
        parsed_path = Path(request.input_path)
        # Aliased imports namespace-copy the whole source env; when it came from
        # the store it is validated-unchanged, so that copy is reusable across
        # every parse importing the same file under the same alias.
        projection = (
            store_entry.projection_for(request.alias)
            if store_entry is not None and request.alias != DEFAULT_NAMESPACE
            else None
        )
        environment.add_import(
            request.alias,
            new_env,
            Import(
                alias=request.alias,
                path=parsed_path,
                input_path=Path(request.target) if is_file_resolver else None,
                concepts=request.concepts,
                description=request.description,
                leading_dots=request.leading_dots,
            ),
            projection=projection,
        )
        return ImportStatement(
            alias=request.alias,
            input_path=request.input_path,
            path=parsed_path,
            concepts=request.concepts,
            leading_dots=request.leading_dots,
        )
