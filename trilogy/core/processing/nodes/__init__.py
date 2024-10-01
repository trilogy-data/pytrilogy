from .filter_node import FilterNode
from .group_node import GroupNode
from .merge_node import MergeNode
from .select_node_v2 import SelectNode, ConstantNode
from .window_node import WindowNode
from .base_node import StrategyNode, NodeJoin
from .unnest_node import UnnestNode
from pydantic import BaseModel, Field, ConfigDict
from trilogy.core.models import Concept, Environment, WhereClause


class History(BaseModel):
    history: dict[str, StrategyNode | None] = Field(default_factory=dict)
    select_history: dict[str, StrategyNode | None] = Field(default_factory=dict)
    started: set[str] = Field(default_factory=set)
    model_config = ConfigDict(arbitrary_types_allowed=True)

    def _concepts_to_lookup(
        self,
        search: list[Concept],
        accept_partial: bool,
        conditions: WhereClause | None = None,
    ) -> str:
        if conditions:
            return (
                "-".join([c.address for c in search])
                + str(accept_partial)
                + str(conditions)
            )
        return "-".join([c.address for c in search]) + str(accept_partial)

    def search_to_history(
        self,
        search: list[Concept],
        accept_partial: bool,
        output: StrategyNode | None,
        conditions: WhereClause | None = None,
    ):
        self.history[
            self._concepts_to_lookup(search, accept_partial, conditions=conditions)
        ] = output

    def get_history(
        self,
        search: list[Concept],
        conditions: WhereClause | None = None,
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
        search: list[Concept],
        accept_partial: bool = False,
        conditions: WhereClause | None = None,
    ):
        self.started.add(
            self._concepts_to_lookup(
                search,
                accept_partial=accept_partial,
                conditions=conditions,
            )
        )

    def check_started(
        self,
        search: list[Concept],
        accept_partial: bool = False,
        conditions: WhereClause | None = None,
    ):
        return (
            self._concepts_to_lookup(
                search,
                accept_partial,
                conditions=conditions,
            )
            in self.started
        )

    def _select_concepts_to_lookup(
        self,
        main: Concept,
        search: list[Concept],
        accept_partial: bool,
        fail_if_not_found: bool,
        accept_partial_optional: bool,
        conditions: WhereClause | None = None,
    ) -> str:
        return (
            str(main.address)
            + "|"
            + "-".join([c.address for c in search])
            + str(accept_partial)
            + str(fail_if_not_found)
            + str(accept_partial_optional)
            + str(conditions)
        )

    def gen_select_node(
        self,
        concept: Concept,
        local_optional: list[Concept],
        environment: Environment,
        g,
        depth: int,
        source_concepts,
        fail_if_not_found: bool = False,
        accept_partial: bool = False,
        accept_partial_optional: bool = False,
        conditions: WhereClause | None = None,
    ) -> StrategyNode | None:
        from trilogy.core.processing.node_generators.select_node import gen_select_node

        fingerprint = self._select_concepts_to_lookup(
            concept,
            local_optional,
            accept_partial,
            fail_if_not_found,
            accept_partial_optional=accept_partial_optional,
            conditions=conditions,
        )
        if fingerprint in self.select_history:
            return self.select_history[fingerprint]
        gen = gen_select_node(
            concept,
            local_optional,
            environment,
            g,
            depth + 1,
            fail_if_not_found=fail_if_not_found,
            accept_partial=accept_partial,
            conditions=conditions,
        )
        self.select_history[fingerprint] = gen
        return gen


__all__ = [
    "FilterNode",
    "GroupNode",
    "MergeNode",
    "SelectNode",
    "WindowNode",
    "StrategyNode",
    "NodeJoin",
    "ConstantNode",
    "UnnestNode",
    "History",
]
