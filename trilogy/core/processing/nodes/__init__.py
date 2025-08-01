from pydantic import BaseModel, ConfigDict, Field

from trilogy.core.exceptions import UnresolvableQueryException
from trilogy.core.models.author import Concept
from trilogy.core.models.build import BuildConcept, BuildWhereClause
from trilogy.core.models.build_environment import BuildEnvironment
from trilogy.core.models.environment import Environment

from .base_node import NodeJoin, StrategyNode, WhereSafetyNode
from .filter_node import FilterNode
from .group_node import GroupNode
from .merge_node import MergeNode
from .recursive_node import RecursiveNode
from .select_node_v2 import ConstantNode, SelectNode
from .union_node import UnionNode
from .unnest_node import UnnestNode
from .window_node import WindowNode


class History(BaseModel):
    base_environment: Environment
    local_base_concepts: dict[str, Concept] = Field(default_factory=dict)
    history: dict[str, StrategyNode | None] = Field(default_factory=dict)
    select_history: dict[str, StrategyNode | None] = Field(default_factory=dict)
    started: dict[str, int] = Field(default_factory=dict)
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _concepts_to_lookup(
        self,
        search: list[BuildConcept],
        accept_partial: bool,
        conditions: BuildWhereClause | None = None,
    ) -> str:
        base = sorted([c.address for c in search])
        if conditions:
            return "-".join(base) + str(accept_partial) + str(conditions)
        return "-".join(base) + str(accept_partial)

    def search_to_history(
        self,
        search: list[BuildConcept],
        accept_partial: bool,
        output: StrategyNode | None,
        conditions: BuildWhereClause | None = None,
    ):
        self.history[
            self._concepts_to_lookup(search, accept_partial, conditions=conditions)
        ] = output
        self.log_end(
            search,
            accept_partial=accept_partial,
            conditions=conditions,
        )

    def get_history(
        self,
        search: list[BuildConcept],
        conditions: BuildWhereClause | None = None,
        accept_partial: bool = False,
        parent_key: str = "",
    ) -> StrategyNode | None | bool:
        key = self._concepts_to_lookup(
            search,
            accept_partial,
            conditions,
        )
        if parent_key and parent_key == key:
            raise ValueError(
                f"Parent key {parent_key} is the same as the current key {key}"
            )
        if key in self.history:
            node = self.history[key]
            if node:
                return node.copy()
            return node
        return False

    def log_start(
        self,
        search: list[BuildConcept],
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ):
        key = self._concepts_to_lookup(
            search,
            accept_partial=accept_partial,
            conditions=conditions,
        )
        if key in self.started:
            self.started[key] += 1
        else:
            self.started[key] = 1
        if self.started[key] > 5:
            raise UnresolvableQueryException(
                f"Was unable to resolve datasources to serve this query from model; unresolvable set was {search}. You may be querying unrelated concepts."
            )

    def log_end(
        self,
        search: list[BuildConcept],
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ):
        key = self._concepts_to_lookup(
            search,
            accept_partial=accept_partial,
            conditions=conditions,
        )
        if key in self.started:
            del self.started[key]

    def check_started(
        self,
        search: list[BuildConcept],
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ):
        return (
            self._concepts_to_lookup(
                search,
                accept_partial,
                conditions=conditions,
            )
            in self.started
        )

    def gen_select_node(
        self,
        concepts: list[BuildConcept],
        environment: BuildEnvironment,
        g,
        depth: int,
        fail_if_not_found: bool = False,
        accept_partial: bool = False,
        conditions: BuildWhereClause | None = None,
    ) -> StrategyNode | None:
        from trilogy.core.processing.node_generators.select_node import gen_select_node

        fingerprint = self._concepts_to_lookup(
            concepts,
            accept_partial,
            conditions=conditions,
        )
        if fingerprint in self.select_history:
            rval = self.select_history[fingerprint]
            if rval:
                # all nodes must be copied before returning
                return rval.copy()
            return rval
        gen = gen_select_node(
            concepts,
            environment,
            g,
            depth + 1,
            fail_if_not_found=fail_if_not_found,
            accept_partial=accept_partial,
            conditions=conditions,
        )
        self.select_history[fingerprint] = gen
        if gen:
            return gen.copy()
        return gen


__all__ = [
    "FilterNode",
    "GroupNode",
    "MergeNode",
    "SelectNode",
    "WindowNode",
    "StrategyNode",
    "NodeJoin",
    "UnnestNode",
    "ConstantNode",
    "UnionNode",
    "History",
    "WhereSafetyNode",
    "RecursiveNode",
]
