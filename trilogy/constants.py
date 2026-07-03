import random
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from logging import getLogger
from typing import Any

logger = getLogger("trilogy")

DEFAULT_NAMESPACE = "local"

RECURSIVE_GATING_CONCEPT = "_terminal"

VIRTUAL_CONCEPT_PREFIX = "_virt"

# Magic rowset name for an inline `from union(...) -> (...)` TVF; its outputs are
# exposed as bare select-local bindings, so the name never collides.
MAGIC_TVF_UNION_NAME = "_tvf_union"

ENV_CACHE_NAME = ".preql_cache.json"

REMOTE_PREFIXES = (
    "gs://",
    "gcs://",
    "s3://",
    "az://",
    "abfs://",
    "abfss://",
    "http://",
    "https://",
)


class MagicConstants(Enum):
    NULL = "null"
    LINE_SEPARATOR = "\n"


NULL_VALUE = MagicConstants.NULL


@dataclass
class Optimizations:
    predicate_pushdown: bool = True
    datasource_inlining: bool = True
    constant_inline_cutoff: int = 10
    direct_return: bool = True
    hide_unused_concepts: bool = True
    merge_aggregate: bool = True
    merge_irrelevant_group_by: bool = True
    upgrade_condition_joins: bool = True
    upgrade_outer_key_set_equivalence: bool = True
    # Trust EQUAL domain declarations (non-partial `merge a into b`) when
    # narrowing outer joins: the merged key's FULL join may upgrade to INNER
    # once both sides pass the completeness tests. Default OFF until the
    # SUBSET/UNION default flip (docs/subset_union_join_design.md) — on
    # declaration-violating data the narrowed join drops the violating rows.
    narrow_equal_domain_joins: bool = False
    simplify_null_safe_joins: bool = True
    strip_redundant_not_null: bool = True
    join_hoist: bool = True
    union_dim_pushdown: bool = True
    order_inner_joins_first: bool = True


@dataclass
class Generation:
    datasource_build_cache: bool = True


@dataclass
class Comments:
    """Control what is placed in CTE comments"""

    show: bool = False
    basic: bool = True
    joins: bool = False
    nullable: bool = False
    partial: bool = False
    source_map: bool = False


@dataclass
class Rendering:
    """Control how the SQL is rendered"""

    parameters: bool = True
    concise: bool = False

    @contextmanager
    def temporary(self, **kwargs: Any):
        """
        Context manager to temporarily set attributes and revert them afterwards.

        Usage:
            r = Rendering()
            with r.temporary(parameters=False, concise=True):
                # parameters is False, concise is True here
                do_something()
            # parameters and concise are back to their original values
        """
        # Store original values
        original_values = {key: getattr(self, key) for key in kwargs}

        # Set new values
        for key, value in kwargs.items():
            setattr(self, key, value)

        try:
            yield self
        finally:
            # Restore original values
            for key, value in original_values.items():
                setattr(self, key, value)


@dataclass
class Parsing:
    """Control Parsing"""

    strict_name_shadow_enforcement: bool = False
    select_as_definition: bool = True


class ParserBackend(Enum):
    LARK = "lark"
    PEST = "pest"


# TODO: support loading from environments
@dataclass
class Config:
    strict_mode: bool = True
    human_identifiers: bool = True
    randomize_cte_names: bool = False
    validate_missing: bool = True
    # Route discovery through the v4 planner instead of the v3 search. Opt-in
    # while v4 is stabilizing; v3 remains the default.
    use_v4_discovery: bool = False
    comments: Comments = field(default_factory=Comments)
    optimizations: Optimizations = field(default_factory=Optimizations)
    rendering: Rendering = field(default_factory=Rendering)
    parsing: Parsing = field(default_factory=Parsing)
    generation: Generation = field(default_factory=Generation)
    parser_backend: ParserBackend = ParserBackend.PEST

    @property
    def show_comments(self) -> bool:
        return self.comments.show

    def set_random_seed(self, seed: int):
        random.seed(seed)


CONFIG = Config()

CONFIG.set_random_seed(42)
