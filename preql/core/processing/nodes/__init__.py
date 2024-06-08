from .filter_node import FilterNode
from .group_node import GroupNode
from .merge_node import MergeNode
from .select_node_v2 import SelectNode, StaticSelectNode, ConstantNode
from .window_node import WindowNode
from .base_node import StrategyNode, NodeJoin
from .unnest_node import UnnestNode
from pydantic import BaseModel, Field, ConfigDict
from preql.core.models import Concept

class History(BaseModel):
    history: dict[str, StrategyNode | None] = Field(default_factory=dict)

    model_config = ConfigDict(arbitrary_types_allowed=True) 

    def _concepts_to_lookup(self, search: list[Concept], accept_partial: bool):
        return "-".join([c.address for c in search])+ str(accept_partial)

    def search_to_history(self, search: list[Concept], accept_partial, output: StrategyNode | None):
        self.history[self._concepts_to_lookup(search,  accept_partial)] = output

    def get_history(self, search: list[Concept], accept_partial:bool = False):
        return self.history.get(self._concepts_to_lookup(search, accept_partial), False)

__all__ = [
    "FilterNode",
    "GroupNode",
    "MergeNode",
    "SelectNode",
    "StaticSelectNode",
    "WindowNode",
    "StrategyNode",
    "NodeJoin",
    "ConstantNode",
    "UnnestNode",
    "History"
]
