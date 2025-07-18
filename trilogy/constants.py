import random
from dataclasses import dataclass, field
from enum import Enum
from logging import getLogger

logger = getLogger("trilogy")

DEFAULT_NAMESPACE = "local"

RECURSIVE_GATING_CONCEPT = "_terminal"

VIRTUAL_CONCEPT_PREFIX = "_virt"

ENV_CACHE_NAME = ".preql_cache.json"


class MagicConstants(Enum):
    NULL = "null"


NULL_VALUE = MagicConstants.NULL


@dataclass
class Optimizations:
    predicate_pushdown: bool = True
    datasource_inlining: bool = True
    constant_inlining: bool = True
    constant_inline_cutoff: int = 10
    direct_return: bool = True


@dataclass
class Comments:
    """Control what is placed in CTE comments"""

    show: bool = False
    basic: bool = True
    joins: bool = True
    nullable: bool = True
    partial: bool = True


@dataclass
class Rendering:
    """Control how the SQL is rendered"""

    parameters: bool = True
    concise: bool = False


@dataclass
class Parsing:
    """Control Parsing"""

    strict_name_shadow_enforcement: bool = False
    select_as_definition: bool = True


# TODO: support loading from environments
@dataclass
class Config:
    strict_mode: bool = True
    human_identifiers: bool = True
    randomize_cte_names: bool = False
    validate_missing: bool = True
    comments: Comments = field(default_factory=Comments)
    optimizations: Optimizations = field(default_factory=Optimizations)
    rendering: Rendering = field(default_factory=Rendering)
    parsing: Parsing = field(default_factory=Parsing)

    @property
    def show_comments(self) -> bool:
        return self.comments.show

    def set_random_seed(self, seed: int):
        random.seed(seed)


CONFIG = Config()

CONFIG.set_random_seed(42)
