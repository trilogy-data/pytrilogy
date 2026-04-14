from __future__ import annotations

from trilogy.parsing.v2.multiselect_rules import MULTISELECT_NODE_HYDRATORS
from trilogy.parsing.v2.order_rules import ORDER_NODE_HYDRATORS
from trilogy.parsing.v2.rules_context import NodeHydrator
from trilogy.parsing.v2.select_statement_rules import SELECT_STATEMENT_NODE_HYDRATORS
from trilogy.parsing.v2.subselect_rules import SUBSELECT_NODE_HYDRATORS
from trilogy.parsing.v2.syntax import SyntaxNodeKind

SELECT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = (
    SELECT_STATEMENT_NODE_HYDRATORS
    | MULTISELECT_NODE_HYDRATORS
    | ORDER_NODE_HYDRATORS
    | SUBSELECT_NODE_HYDRATORS
)
