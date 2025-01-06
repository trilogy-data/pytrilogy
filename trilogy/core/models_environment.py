from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import Annotated, Dict, ItemsView, List, Never, Optional, Tuple, ValuesView

from lark.tree import Meta
from pydantic import BaseModel, ConfigDict, Field
from pydantic.functional_validators import PlainValidator

from trilogy.constants import DEFAULT_NAMESPACE, ENV_CACHE_NAME, logger
from trilogy.core.constants import INTERNAL_NAMESPACE, PERSISTED_CONCEPT_PREFIX
from trilogy.core.enums import (
    ConceptSource,
    Derivation,
    FunctionType,
    Modifier,
    Purpose,
)
from trilogy.core.exceptions import UndefinedConceptException
from trilogy.core.models_author import (
    Concept,
    Function,
    UndefinedConcept,
    address_with_namespace,
)
from trilogy.core.models_core import DataType
from trilogy.core.models_datasource import Datasource, EnvironmentDatasourceDict
from trilogy.core.statements_author import ImportStatement


class EnvironmentOptions(BaseModel):
    allow_duplicate_declaration: bool = True


class EnvironmentConceptDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)
        self.undefined: dict[str, UndefinedConcept] = {}
        self.fail_on_missing: bool = True
        self.populate_default_concepts()

    def duplicate(self) -> "EnvironmentConceptDict":
        new = EnvironmentConceptDict()
        new.update({k: v.duplicate() for k, v in self.items()})
        new.undefined = self.undefined
        new.fail_on_missing = self.fail_on_missing
        return new

    def populate_default_concepts(self):
        from trilogy.core.internal import DEFAULT_CONCEPTS

        for concept in DEFAULT_CONCEPTS.values():
            self[concept.address] = concept

    def values(self) -> ValuesView[Concept]:  # type: ignore
        return super().values()

    def get(self, key: str, default: Concept | None = None) -> Concept | None:  # type: ignore
        try:
            return self.__getitem__(key)
        except UndefinedConceptException:
            return default

    def raise_undefined(
        self, key: str, line_no: int | None = None, file: Path | str | None = None
    ) -> Never:

        matches = self._find_similar_concepts(key)
        message = f"Undefined concept: {key}."
        if matches:
            message += f" Suggestions: {matches}"

        if line_no:
            if file:
                raise UndefinedConceptException(
                    f"{file}: {line_no}: " + message, matches
                )
            raise UndefinedConceptException(f"line: {line_no}: " + message, matches)
        raise UndefinedConceptException(message, matches)

    def __getitem__(
        self, key: str, line_no: int | None = None, file: Path | None = None
    ) -> Concept | UndefinedConcept:
        try:
            return super(EnvironmentConceptDict, self).__getitem__(key)
        except KeyError:
            if "." in key and key.split(".", 1)[0] == DEFAULT_NAMESPACE:
                return self.__getitem__(key.split(".", 1)[1], line_no)
            if DEFAULT_NAMESPACE + "." + key in self:
                return self.__getitem__(DEFAULT_NAMESPACE + "." + key, line_no)
            if not self.fail_on_missing:
                if "." in key:
                    ns, rest = key.rsplit(".", 1)
                else:
                    ns = DEFAULT_NAMESPACE
                    rest = key
                if key in self.undefined:
                    return self.undefined[key]
                undefined = UndefinedConcept(
                    name=rest,
                    line_no=line_no,
                    datatype=DataType.UNKNOWN,
                    purpose=Purpose.UNKNOWN,
                    namespace=ns,
                )
                self.undefined[key] = undefined
                return undefined
        self.raise_undefined(key, line_no, file)

    def _find_similar_concepts(self, concept_name: str):
        def strip_local(input: str):
            if input.startswith(f"{DEFAULT_NAMESPACE}."):
                return input[len(DEFAULT_NAMESPACE) + 1 :]
            return input

        matches = difflib.get_close_matches(
            strip_local(concept_name), [strip_local(x) for x in self.keys()]
        )
        return matches

    def items(self) -> ItemsView[str, Concept]:  # type: ignore
        return super().items()


def validate_concepts(v) -> EnvironmentConceptDict:
    if isinstance(v, EnvironmentConceptDict):
        return v
    elif isinstance(v, dict):
        return EnvironmentConceptDict(
            **{x: Concept.model_validate(y) for x, y in v.items()}
        )
    raise ValueError


def validate_datasources(v) -> EnvironmentDatasourceDict:
    if isinstance(v, EnvironmentDatasourceDict):
        return v
    elif isinstance(v, dict):
        return EnvironmentDatasourceDict(
            **{x: Datasource.model_validate(y) for x, y in v.items()}
        )
    raise ValueError


def get_version():
    from trilogy import __version__

    return __version__


class Environment(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, strict=False)

    concepts: Annotated[EnvironmentConceptDict, PlainValidator(validate_concepts)] = (
        Field(default_factory=EnvironmentConceptDict)
    )
    datasources: Annotated[
        EnvironmentDatasourceDict, PlainValidator(validate_datasources)
    ] = Field(default_factory=EnvironmentDatasourceDict)
    functions: Dict[str, Function] = Field(default_factory=dict)
    data_types: Dict[str, DataType] = Field(default_factory=dict)
    imports: Dict[str, list[ImportStatement]] = Field(
        default_factory=lambda: defaultdict(list)  # type: ignore
    )
    namespace: str = DEFAULT_NAMESPACE
    working_path: str | Path = Field(default_factory=lambda: os.getcwd())
    environment_config: EnvironmentOptions = Field(default_factory=EnvironmentOptions)
    version: str = Field(default_factory=get_version)
    cte_name_map: Dict[str, str] = Field(default_factory=dict)
    materialized_concepts: set[str] = Field(default_factory=set)
    alias_origin_lookup: Dict[str, Concept] = Field(default_factory=dict)
    # TODO: support freezing environments to avoid mutation
    frozen: bool = False
    env_file_path: Path | None = None

    def freeze(self):
        self.frozen = True

    def thaw(self):
        self.frozen = False

    def duplicate(self):
        return Environment.model_construct(
            datasources=self.datasources.duplicate(),
            concepts=self.concepts.duplicate(),
            functions=dict(self.functions),
            data_types=dict(self.data_types),
            imports=dict(self.imports),
            namespace=self.namespace,
            working_path=self.working_path,
            environment_config=self.environment_config,
            version=self.version,
            cte_name_map=dict(self.cte_name_map),
            materialized_concepts=set(self.materialized_concepts),
            alias_origin_lookup={
                k: v.duplicate() for k, v in self.alias_origin_lookup.items()
            },
        )

    def __init__(self, **data):
        super().__init__(**data)
        concept = Concept(
            name="_env_working_path",
            namespace=self.namespace,
            lineage=Function(
                operator=FunctionType.CONSTANT,
                arguments=[str(self.working_path)],
                output_datatype=DataType.STRING,
                output_purpose=Purpose.CONSTANT,
            ),
            datatype=DataType.STRING,
            purpose=Purpose.CONSTANT,
        )
        self.add_concept(concept)

    # def freeze(self):
    #     self.frozen = True

    # def thaw(self):
    #     self.frozen = False

    @classmethod
    def from_file(cls, path: str | Path) -> "Environment":
        if isinstance(path, str):
            path = Path(path)
        with open(path, "r") as f:
            read = f.read()
        return Environment(working_path=path.parent, env_file_path=path).parse(read)[0]

    @classmethod
    def from_string(cls, input: str) -> "Environment":
        return Environment().parse(input)[0]

    @classmethod
    def from_cache(cls, path) -> Optional["Environment"]:
        with open(path, "r") as f:
            read = f.read()
        base = cls.model_validate_json(read)
        version = get_version()
        if base.version != version:
            return None
        return base

    def to_cache(self, path: Optional[str | Path] = None) -> Path:
        if not path:
            ppath = Path(self.working_path) / ENV_CACHE_NAME
        else:
            ppath = Path(path)
        with open(ppath, "w") as f:
            f.write(self.model_dump_json())
        return ppath

    def gen_concept_list_caches(self) -> None:
        concrete_addresses = set()
        for datasource in self.datasources.values():
            for concept in datasource.output_concepts:
                concrete_addresses.add(concept.address)
        self.materialized_concepts = set(
            [
                c.address
                for c in self.concepts.values()
                if c.address in concrete_addresses
            ]
            + [
                c.address
                for c in self.alias_origin_lookup.values()
                if c.address in concrete_addresses
            ],
        )

    def validate_concept(self, new_concept: Concept, meta: Meta | None = None):
        lookup = new_concept.address
        existing: Concept = self.concepts.get(lookup)  # type: ignore
        if not existing:
            return

        def handle_persist():
            deriv_lookup = (
                f"{existing.namespace}.{PERSISTED_CONCEPT_PREFIX}_{existing.name}"
            )

            alt_source = self.alias_origin_lookup.get(deriv_lookup)
            if not alt_source:
                return None
            # if the new concept binding has no lineage
            # nothing to cause us to think a persist binding
            # needs to be invalidated
            if not new_concept.lineage:
                return existing
            if str(alt_source.lineage) == str(new_concept.lineage):
                logger.info(
                    f"Persisted concept {existing.address} matched redeclaration, keeping current persistence binding."
                )
                return existing
            logger.warning(
                f"Persisted concept {existing.address} lineage {str(alt_source.lineage)} did not match redeclaration {str(new_concept.lineage)}, overwriting and invalidating persist binding."
            )
            for k, datasource in self.datasources.items():
                if existing.address in datasource.output_concepts:
                    datasource.columns = [
                        x
                        for x in datasource.columns
                        if x.concept.address != existing.address
                    ]
            return None

        if existing and self.environment_config.allow_duplicate_declaration:
            if existing.metadata.concept_source == ConceptSource.PERSIST_STATEMENT:
                return handle_persist()
            return
        elif existing.metadata:
            if existing.metadata.concept_source == ConceptSource.PERSIST_STATEMENT:
                return handle_persist()
            # if the existing concept is auto derived, we can overwrite it
            if existing.metadata.concept_source == ConceptSource.AUTO_DERIVED:
                return None
        elif meta and existing.metadata:
            raise ValueError(
                f"Assignment to concept '{lookup}' on line {meta.line} is a duplicate"
                f" declaration; '{lookup}' was originally defined on line"
                f" {existing.metadata.line_number}"
            )
        elif existing.metadata:
            raise ValueError(
                f"Assignment to concept '{lookup}'  is a duplicate declaration;"
                f" '{lookup}' was originally defined on line"
                f" {existing.metadata.line_number}"
            )
        raise ValueError(
            f"Assignment to concept '{lookup}'  is a duplicate declaration;"
        )

    def add_import(
        self, alias: str, source: Environment, imp_stm: ImportStatement | None = None
    ):
        from trilogy.core.statements_author import ImportStatement

        if self.frozen:
            raise ValueError("Environment is frozen, cannot add imports")
        exists = False
        existing = self.imports[alias]
        if imp_stm:
            if any(
                [x.path == imp_stm.path and x.alias == imp_stm.alias for x in existing]
            ):
                exists = True
        else:
            if any(
                [x.path == source.working_path and x.alias == alias for x in existing]
            ):
                exists = True
            imp_stm = ImportStatement(alias=alias, path=Path(source.working_path))
        same_namespace = alias == self.namespace

        if not exists:
            self.imports[alias].append(imp_stm)
        # we can't exit early
        # as there may be new concepts
        for k, concept in source.concepts.items():
            # skip internal namespace
            if INTERNAL_NAMESPACE in concept.address:
                continue
            if same_namespace:
                new = self.add_concept(concept, _ignore_cache=True)
            else:
                new = self.add_concept(
                    concept.with_namespace(alias), _ignore_cache=True
                )

                k = address_with_namespace(k, alias)
            # set this explicitly, to handle aliasing
            self.concepts[k] = new

        for _, datasource in source.datasources.items():
            if same_namespace:
                self.add_datasource(datasource, _ignore_cache=True)
            else:
                self.add_datasource(
                    datasource.with_namespace(alias), _ignore_cache=True
                )
        for key, val in source.alias_origin_lookup.items():
            if same_namespace:
                self.alias_origin_lookup[key] = val
            else:
                self.alias_origin_lookup[address_with_namespace(key, alias)] = (
                    val.with_namespace(alias)
                )

        self.gen_concept_list_caches()
        return self

    def add_file_import(
        self, path: str | Path, alias: str, env: "Environment" | None = None
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add imports")
        from trilogy.parsing.parse_engine import (
            PARSER,
            ParseToObjects,
            gen_cache_lookup,
        )

        if isinstance(path, str):
            if path.endswith(".preql"):
                path = path.rsplit(".", 1)[0]
            if "." not in path:
                target = Path(self.working_path, path)
            else:
                target = Path(self.working_path, *path.split("."))
            target = target.with_suffix(".preql")
        else:
            target = path
        if not env:
            parse_address = gen_cache_lookup(str(target), alias, str(self.working_path))
            try:
                with open(target, "r", encoding="utf-8") as f:
                    text = f.read()
                nenv = Environment(
                    working_path=target.parent,
                )
                nenv.concepts.fail_on_missing = False
                nparser = ParseToObjects(
                    environment=Environment(
                        working_path=target.parent,
                    ),
                    parse_address=parse_address,
                    token_address=target,
                )
                nparser.set_text(text)
                nparser.transform(PARSER.parse(text))
                nparser.hydrate_missing()

            except Exception as e:
                raise ImportError(
                    f"Unable to import file {target.parent}, parsing error: {e}"
                )
            env = nparser.environment
        imps = ImportStatement(alias=alias, path=target)
        self.add_import(alias, source=env, imp_stm=imps)
        return imps

    def parse(
        self, input: str, namespace: str | None = None, persist: bool = False
    ) -> Tuple["Environment", list]:
        from trilogy import parse
        from trilogy.core.query_processor import process_persist
        from trilogy.core.statements_author import (
            MultiSelectStatement,
            PersistStatement,
            SelectStatement,
            ShowStatement,
        )

        if namespace:
            new = Environment()
            _, queries = new.parse(input)
            self.add_import(namespace, new)
            return self, queries
        _, queries = parse(input, self)
        generatable = [
            x
            for x in queries
            if isinstance(
                x,
                (
                    SelectStatement,
                    PersistStatement,
                    MultiSelectStatement,
                    ShowStatement,
                ),
            )
        ]
        while generatable:
            t = generatable.pop(0)
            if isinstance(t, PersistStatement) and persist:
                processed = process_persist(self, t)
                self.add_datasource(processed.datasource)
        return self, queries

    def add_concept(
        self,
        concept: Concept,
        meta: Meta | None = None,
        force: bool = False,
        add_derived: bool = True,
        _ignore_cache: bool = False,
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add concepts")
        if not force:
            existing = self.validate_concept(concept, meta=meta)
            if existing:
                concept = existing
        self.concepts[concept.address] = concept
        from trilogy.core.environment_helpers import generate_related_concepts

        generate_related_concepts(concept, self, meta=meta, add_derived=add_derived)
        if not _ignore_cache:
            self.gen_concept_list_caches()
        return concept

    def add_datasource(
        self,
        datasource: Datasource,
        meta: Meta | None = None,
        _ignore_cache: bool = False,
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add datasource")
        self.datasources[datasource.identifier] = datasource

        eligible_to_promote_roots = datasource.non_partial_for is None
        # mark this as canonical source
        for current_concept in datasource.output_concepts:
            if not eligible_to_promote_roots:
                continue

            current_derivation = current_concept.derivation
            # TODO: refine this section;
            # too hacky for maintainability
            if current_derivation not in (Derivation.ROOT, Derivation.CONSTANT):
                persisted = f"{PERSISTED_CONCEPT_PREFIX}_" + current_concept.name
                # override the current concept source to reflect that it's now coming from a datasource
                if (
                    current_concept.metadata.concept_source
                    != ConceptSource.PERSIST_STATEMENT
                ):
                    new_concept = current_concept.model_copy(deep=True)
                    new_concept.set_name(persisted)
                    self.add_concept(
                        new_concept, meta=meta, force=True, _ignore_cache=True
                    )
                    current_concept.metadata.concept_source = (
                        ConceptSource.PERSIST_STATEMENT
                    )
                    # remove the associated lineage
                    # to make this a root for discovery purposes
                    # as it now "exists" in a table
                    current_concept.lineage = None
                    current_concept = current_concept.with_default_grain()
                    self.add_concept(
                        current_concept, meta=meta, force=True, _ignore_cache=True
                    )
                    self.merge_concept(new_concept, current_concept, [])
                else:
                    self.add_concept(current_concept, meta=meta, _ignore_cache=True)
        if not _ignore_cache:
            self.gen_concept_list_caches()
        return datasource

    def delete_datasource(
        self,
        address: str,
        meta: Meta | None = None,
    ) -> bool:
        if self.frozen:
            raise ValueError("Environment is frozen, cannot delete datsources")
        if address in self.datasources:
            del self.datasources[address]
            self.gen_concept_list_caches()
            return True
        return False

    def merge_concept(
        self,
        source: Concept,
        target: Concept,
        modifiers: List[Modifier],
        force: bool = False,
    ) -> bool:
        if self.frozen:
            raise ValueError("Environment is frozen, cannot merge concepts")
        replacements = {}

        # exit early if we've run this
        if source.address in self.alias_origin_lookup and not force:
            if self.concepts[source.address] == target:
                return False
        self.alias_origin_lookup[source.address] = source
        for k, v in self.concepts.items():
            if v.address == target.address:
                v.pseudonyms.add(source.address)

            if v.address == source.address:
                replacements[k] = target
                v.pseudonyms.add(target.address)
            # we need to update keys and grains of all concepts
            else:
                replacements[k] = v.with_merge(source, target, modifiers)
        self.concepts.update(replacements)

        for k, ds in self.datasources.items():
            if source.address in ds.output_lcl:
                ds.merge_concept(source, target, modifiers=modifiers)
        return True


class LazyEnvironment(Environment):
    """Variant of environment to defer parsing of a path
    until relevant attributes accessed."""

    load_path: Path
    loaded: bool = False

    def __getattribute__(self, name):
        if name in (
            "load_path",
            "loaded",
            "working_path",
            "model_config",
            "model_fields",
            "model_post_init",
        ) or name.startswith("_"):
            return super().__getattribute__(name)
        if not self.loaded:
            logger.info(
                f"lazily evaluating load path {self.load_path} to access {name}"
            )
            from trilogy import parse

            env = Environment(working_path=str(self.working_path))
            with open(self.load_path, "r") as f:
                parse(f.read(), env)
            self.loaded = True
            self.datasources = env.datasources
            self.concepts = env.concepts
            self.imports = env.imports
        return super().__getattribute__(name)


Environment.update_forward_refs()
