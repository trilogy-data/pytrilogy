from __future__ import annotations

from dataclasses import dataclass, field
from os.path import dirname
from pathlib import Path

from trilogy.constants import Parsing
from trilogy.core.models.environment import (
    DictImportResolver,
    Environment,
    FileSystemImportResolver,
    Import,
)
from trilogy.core.statements.author import ImportStatement


@dataclass
class ImportRequest:
    alias: str
    cache_key: str
    input_path: str
    target: str
    token_lookup: Path | str
    is_stdlib: bool = False
    concepts: list[str] | None = None


def _read_import_text(
    address: str, environment: Environment, is_stdlib: bool = False
) -> str:
    if (
        isinstance(environment.config.import_resolver, FileSystemImportResolver)
        or is_stdlib
    ):
        with open(address, "r", encoding="utf-8") as f:
            return f.read()
    if isinstance(environment.config.import_resolver, DictImportResolver):
        if address not in environment.config.import_resolver.content:
            raise ImportError(
                f"Unable to import file {address}, not resolvable from provided source files."
            )
        return environment.config.import_resolver.content[address]
    raise ImportError(
        f"Unable to import file {address}, resolver type "
        f"{type(environment.config.import_resolver)} not supported"
    )


@dataclass
class ImportHydrationService:
    """Owns recursive import parsing and the shared caches that make it idempotent."""

    environment: Environment
    parse_config: Parsing | None = None
    max_parse_depth: int = 10
    parsed_environments: dict[str, Environment] = field(default_factory=dict)
    text_lookup: dict[Path | str, str] = field(default_factory=dict)
    import_keys: list[str] = field(default_factory=list)

    def set_text(self, key: Path | str, text: str) -> None:
        self.text_lookup[key] = text

    def execute(self, request: ImportRequest) -> ImportStatement:
        from trilogy.parsing.parse_engine_v2 import parse_syntax
        from trilogy.parsing.v2.hydration import HydrationContext, NativeHydrator

        environment = self.environment
        key_path = self.import_keys + [request.cache_key]
        cache_lookup = "-".join(key_path)

        if len(key_path) > self.max_parse_depth:
            return ImportStatement(
                alias=request.alias,
                input_path=request.input_path,
                path=Path(request.target),
            )

        if request.token_lookup in self.text_lookup:
            text = self.text_lookup[request.token_lookup]
        else:
            text = _read_import_text(request.target, environment, request.is_stdlib)
            self.text_lookup[request.token_lookup] = text

        if cache_lookup in self.parsed_environments:
            new_env = self.parsed_environments[cache_lookup]
        else:
            root = None
            if "." in str(request.token_lookup):
                root = str(request.token_lookup).rsplit(".", 1)[0]
            try:
                document = parse_syntax(text)
                new_env = Environment(
                    working_path=dirname(request.target),
                    env_file_path=request.token_lookup,
                    config=environment.config.copy_for_root(root=root),
                    parameters=environment.parameters,
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
                )
                NativeHydrator(child_context).parse(document)
                self.parsed_environments[cache_lookup] = new_env
            except Exception as e:
                raise ImportError(
                    f"Unable to import file {request.target}, parsing error: {e}"
                ) from e

        is_file_resolver = isinstance(
            environment.config.import_resolver, FileSystemImportResolver
        )
        parsed_path = Path(request.input_path)
        environment.add_import(
            request.alias,
            new_env,
            Import(
                alias=request.alias,
                path=parsed_path,
                input_path=Path(request.target) if is_file_resolver else None,
                concepts=request.concepts,
            ),
        )
        return ImportStatement(
            alias=request.alias,
            input_path=request.input_path,
            path=parsed_path,
            concepts=request.concepts,
        )
