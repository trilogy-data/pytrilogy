from __future__ import annotations

import difflib
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Dict,
    ItemsView,
    List,
    Never,
    Optional,
    Self,
    Tuple,
    ValuesView,
)

from lark.tree import Meta
from pydantic import BaseModel, ConfigDict, Field
from pydantic.functional_validators import PlainValidator

from trilogy.constants import DEFAULT_NAMESPACE, ENV_CACHE_NAME, logger
from trilogy.core.constants import (
    INTERNAL_NAMESPACE,
    WORKING_PATH_CONCEPT,
)
from trilogy.core.enums import (
    ConceptSource,
    Derivation,
    FunctionType,
    Granularity,
    Modifier,
    Purpose,
)
from trilogy.core.exceptions import (
    FrozenEnvironmentException,
    UndefinedConceptException,
)
from trilogy.core.models.author import (
    Concept,
    ConceptRef,
    CustomFunctionFactory,
    CustomType,
    Function,
    SelectLineage,
    UndefinedConcept,
    UndefinedConceptFull,
    address_with_namespace,
)
from trilogy.core.models.core import DataType
from trilogy.core.models.datasource import Datasource, EnvironmentDatasourceDict

if TYPE_CHECKING:
    from trilogy.core.models.build import BuildConcept, BuildEnvironment


@dataclass
class Import:
    alias: str
    path: Path
    input_path: Path | None = (
        None  # filepath where the text came from (path is the import path, but may be resolved from a dictionary for some resolvers)
    )


class BaseImportResolver(BaseModel):
    pass


class FileSystemImportResolver(BaseImportResolver):
    pass


class DictImportResolver(BaseImportResolver):
    content: Dict[str, str]


class EnvironmentConfig(BaseModel):
    allow_duplicate_declaration: bool = True
    import_resolver: BaseImportResolver = Field(
        default_factory=FileSystemImportResolver
    )

    def copy_for_root(self, root: str | None) -> Self:
        new = self.model_copy(deep=True)
        if isinstance(new.import_resolver, DictImportResolver) and root:
            new.import_resolver = DictImportResolver(
                content={
                    k[len(root) + 1 :]: v
                    for k, v in new.import_resolver.content.items()
                    if k.startswith(f"{root}.")
                }
            )
        return new


class EnvironmentConceptDict(dict):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(self, *args, **kwargs)
        self.undefined: dict[str, UndefinedConceptFull] = {}
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
    ) -> Concept | UndefinedConceptFull:
        # fast access path
        if key in self.keys():
            return super(EnvironmentConceptDict, self).__getitem__(key)
        if isinstance(key, ConceptRef):
            return self.__getitem__(key.address, line_no=line_no, file=file)
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
                undefined = UndefinedConceptFull(
                    line_no=line_no,
                    datatype=DataType.UNKNOWN,
                    name=rest,
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
    functions: Dict[str, CustomFunctionFactory] = Field(default_factory=dict)
    data_types: Dict[str, CustomType] = Field(default_factory=dict)
    named_statements: Dict[str, SelectLineage] = Field(default_factory=dict)
    imports: defaultdict[str, list[Import]] = Field(
        default_factory=lambda: defaultdict(list)  # type: ignore
    )
    namespace: str = DEFAULT_NAMESPACE
    working_path: str | Path = Field(default_factory=lambda: os.getcwd())
    config: EnvironmentConfig = Field(default_factory=EnvironmentConfig)
    version: str = Field(default_factory=get_version)
    cte_name_map: Dict[str, str] = Field(default_factory=dict)
    alias_origin_lookup: Dict[str, Concept] = Field(default_factory=dict)
    # TODO: support freezing environments to avoid mutation
    frozen: bool = False
    env_file_path: Path | str | None = None
    parameters: Dict[str, Any] = Field(default_factory=dict)

    def freeze(self):
        self.frozen = True

    def thaw(self):
        self.frozen = False

    def set_parameters(self, **kwargs) -> Self:

        self.parameters.update(kwargs)
        return self

    def materialize_for_select(
        self, local_concepts: dict[str, "BuildConcept"] | None = None
    ) -> "BuildEnvironment":
        """helper method"""
        from trilogy.core.models.build import Factory

        return Factory(self, local_concepts=local_concepts).build(self)

    def add_rowset(self, name: str, lineage: SelectLineage):
        self.named_statements[name] = lineage

    def duplicate(self):
        return Environment.model_construct(
            datasources=self.datasources.duplicate(),
            concepts=self.concepts.duplicate(),
            functions=dict(self.functions),
            data_types=dict(self.data_types),
            imports=defaultdict(list, self.imports),
            namespace=self.namespace,
            working_path=self.working_path,
            environment_config=self.config.model_copy(deep=True),
            version=self.version,
            cte_name_map=dict(self.cte_name_map),
            alias_origin_lookup={
                k: v.duplicate() for k, v in self.alias_origin_lookup.items()
            },
            env_file_path=self.env_file_path,
        )

    def _add_path_concepts(self):
        concept = Concept(
            name=WORKING_PATH_CONCEPT,
            namespace=self.namespace,
            lineage=Function(
                operator=FunctionType.CONSTANT,
                arguments=[str(self.working_path)],
                output_datatype=DataType.STRING,
                output_purpose=Purpose.CONSTANT,
            ),
            datatype=DataType.STRING,
            granularity=Granularity.SINGLE_ROW,
            derivation=Derivation.CONSTANT,
            purpose=Purpose.CONSTANT,
        )
        self.add_concept(concept)

    def model_post_init(self, context: Any) -> None:
        self._add_path_concepts()

    @classmethod
    def from_file(cls, path: str | Path) -> "Environment":
        if isinstance(path, str):
            path = Path(path)
        with open(path, "r") as f:
            read = f.read()
        return Environment(working_path=path.parent, env_file_path=path).parse(read)[0]

    @classmethod
    def from_string(
        cls, input: str, config: EnvironmentConfig | None = None
    ) -> "Environment":
        config = config or EnvironmentConfig()
        return Environment(config=config).parse(input)[0]

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

    def validate_concept(
        self, new_concept: Concept, meta: Meta | None = None
    ) -> Concept | None:
        lookup = new_concept.address
        if lookup not in self.concepts:
            return None
        existing: Concept = self.concepts.get(lookup)  # type: ignore
        if isinstance(existing, UndefinedConcept):
            return None

        def handle_currently_bound_sources():
            if str(existing.lineage) == str(new_concept.lineage):
                return None

            invalidated = False
            for k, datasource in self.datasources.items():
                if existing.address in datasource.output_concepts:
                    logger.warning(
                        f"Removed concept for {existing} assignment from {k}"
                    )
                    clen = len(datasource.columns)
                    datasource.columns = [
                        x
                        for x in datasource.columns
                        if x.concept.address != existing.address
                    ]
                    assert len(datasource.columns) < clen
                    invalidated = len(datasource.columns) < clen
            if invalidated:
                logger.warning(
                    f"Persisted concept {existing.address} lineage {str(existing.lineage)} did not match redeclaration {str(new_concept.lineage)}, invalidated current bound datasource."
                )
            return None

        if existing and self.config.allow_duplicate_declaration:
            if (
                existing.metadata
                and existing.metadata.concept_source == ConceptSource.AUTO_DERIVED
            ):
                # auto derived concepts will not have sources nad do not need to be checked
                return None
            return handle_currently_bound_sources()
        elif (
            existing.metadata
            and existing.metadata.concept_source == ConceptSource.AUTO_DERIVED
        ):
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
        self, alias: str, source: Environment, imp_stm: Import | None = None
    ):
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
            imp_stm = Import(alias=alias, path=Path(source.working_path))
        same_namespace = alias == DEFAULT_NAMESPACE

        if not exists:
            self.imports[alias].append(imp_stm)
        # we can't exit early
        # as there may be new concepts
        iteration: list[tuple[str, Concept]] = list(source.concepts.items())
        for k, concept in iteration:
            # skip internal namespace
            if INTERNAL_NAMESPACE in concept.address:
                continue
            # don't overwrite working path
            if concept.name == WORKING_PATH_CONCEPT:
                continue
            if same_namespace:
                new = self.add_concept(concept, add_derived=False)
            else:
                new = self.add_concept(concept.with_namespace(alias), add_derived=False)

                k = address_with_namespace(k, alias)
            # set this explicitly, to handle aliasing
            self.concepts[k] = new

        for _, datasource in source.datasources.items():
            if same_namespace:
                self.add_datasource(datasource)
            else:
                self.add_datasource(datasource.with_namespace(alias))
        for key, val in source.alias_origin_lookup.items():

            if same_namespace:
                self.alias_origin_lookup[key] = val
            else:
                self.alias_origin_lookup[address_with_namespace(key, alias)] = (
                    val.with_namespace(alias)
                )

        for key, function in source.functions.items():
            if same_namespace:
                self.functions[key] = function
            else:
                self.functions[address_with_namespace(key, alias)] = (
                    function.with_namespace(alias)
                )
        for key, type in source.data_types.items():
            if same_namespace:
                self.data_types[key] = type
            else:
                self.data_types[address_with_namespace(key, alias)] = (
                    type.with_namespace(alias)
                )
        return self

    def add_file_import(
        self, path: str | Path, alias: str, env: "Environment" | None = None
    ):
        if self.frozen:
            raise ValueError("Environment is frozen, cannot add imports")
        from trilogy.parsing.parse_engine import (
            PARSER,
            ParseToObjects,
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
            import_keys = ["root", alias]
            parse_address = "-".join(import_keys)
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
                    import_keys=import_keys,
                )
                nparser.set_text(text)
                nparser.environment.concepts.fail_on_missing = False
                nparser.transform(PARSER.parse(text))
                nparser.run_second_parse_pass()
                nparser.environment.concepts.fail_on_missing = True

            except Exception as e:
                raise ImportError(
                    f"Unable to import file {target.parent}, parsing error: {e}"
                )
            env = nparser.environment
        imps = Import(alias=alias, path=target)
        self.add_import(alias, source=env, imp_stm=imps)
        return imps

    def parse(
        self, input: str, namespace: str | None = None, persist: bool = False
    ) -> Tuple["Environment", list]:
        from trilogy import parse
        from trilogy.core.query_processor import process_persist
        from trilogy.core.statements.author import (
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
    ):

        if self.frozen:
            raise FrozenEnvironmentException(
                "Environment is frozen, cannot add concepts"
            )
        if not force:
            existing = self.validate_concept(concept, meta=meta)
            if existing:
                concept = existing

        self.concepts[concept.address] = concept

        from trilogy.core.environment_helpers import generate_related_concepts

        generate_related_concepts(concept, self, meta=meta, add_derived=add_derived)

        return concept

    def remove_concept(
        self,
        concept: Concept | str,
    ) -> bool:
        if self.frozen:
            raise FrozenEnvironmentException(
                "Environment is frozen, cannot remove concepts"
            )
        if isinstance(concept, Concept):
            address = concept.address
            c_instance = concept
        else:
            address = concept
            c_instance_check = self.concepts.get(address)
            if not c_instance_check:
                return False
            c_instance = c_instance_check
        from trilogy.core.environment_helpers import remove_related_concepts

        remove_related_concepts(c_instance, self)
        if address in self.concepts:
            del self.concepts[address]
            return True
        if address in self.alias_origin_lookup:
            del self.alias_origin_lookup[address]

        return False

    def add_datasource(
        self,
        datasource: Datasource,
        meta: Meta | None = None,
    ):
        if self.frozen:
            raise FrozenEnvironmentException(
                "Environment is frozen, cannot add datasource"
            )
        self.datasources[datasource.identifier] = datasource
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
            # self.gen_concept_list_caches()
            return True
        return False

    def merge_concept(
        self,
        source: Concept,
        target: Concept,
        modifiers: List[Modifier],
        force: bool = False,
    ) -> bool:
        from trilogy.core.models.build import BuildConcept

        if isinstance(source, BuildConcept):
            raise SyntaxError(source)
        elif isinstance(target, BuildConcept):
            raise SyntaxError(target)
        if self.frozen:
            raise ValueError("Environment is frozen, cannot merge concepts")
        replacements = {}

        # exit early if we've run this
        if source.address in self.alias_origin_lookup and not force:
            if self.concepts[source.address] == target:
                return False

        self.alias_origin_lookup[source.address] = source
        self.alias_origin_lookup[source.address].pseudonyms.add(target.address)
        for k, v in self.concepts.items():

            if v.address == target.address:
                if source.address != target.address:
                    v.pseudonyms.add(source.address)

            if v.address == source.address:
                replacements[k] = target
            # we need to update keys and grains of all concepts
            else:
                if source.address in v.sources or source.address in v.grain.components:
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
    setup_queries: list[Any] = Field(default_factory=list)
    loaded: bool = False

    @property
    def setup_path(self) -> Path:
        return self.load_path.parent / "setup.preql"

    def __init__(self, **data):
        if not data.get("working_path"):
            data["working_path"] = data["load_path"].parent
        super().__init__(**data)
        assert self.working_path == self.load_path.parent

    def _add_path_concepts(self):
        pass

    def _load(self):
        if self.loaded:
            return
        from trilogy import parse

        env = Environment(working_path=self.load_path.parent)
        assert env.working_path == self.load_path.parent
        with open(self.load_path, "r") as f:
            env, _ = parse(f.read(), env)
        if self.setup_path.exists():
            with open(self.setup_path, "r") as f2:
                env, q = parse(f2.read(), env)
                for q in q:
                    self.setup_queries.append(q)
        self.loaded = True
        self.datasources = env.datasources
        self.concepts = env.concepts
        self.imports = env.imports
        self.alias_origin_lookup = env.alias_origin_lookup
        self.functions = env.functions
        self.data_types = env.data_types
        self.cte_name_map = env.cte_name_map

    def __getattr__(self, name):
        return self.__getattribute__(name)

    def __getattribute__(self, name):
        if name not in (
            "datasources",
            "concepts",
            "imports",
            "functions",
            "datatypes",
            "cte_name_map",
        ) or name.startswith("_"):
            return super().__getattribute__(name)
        if not self.loaded:
            self._load()
        return super().__getattribute__(name)


Environment.model_rebuild()
