from logging import getLogger
from dataclasses import dataclass, field
from enum import Enum

logger = getLogger("trilogy")

DEFAULT_NAMESPACE = "local"

VIRTUAL_CONCEPT_PREFIX = "_virtual"

ENV_CACHE_NAME = ".preql_cache.json"


class MagicConstants(Enum):
    NULL = "null"


NULL_VALUE = MagicConstants.NULL


@dataclass
class Optimizations:
    predicate_pushdown: bool = True
    datasource_inlining: bool = True
    direct_return: bool = True


# TODO: support loading from environments
@dataclass
class Config:
    strict_mode: bool = True
    human_identifiers: bool = True
    validate_missing: bool = True
    optimizations: Optimizations = field(default_factory=Optimizations)


CONFIG = Config()

CONFIG.strict_mode = True
