from __future__ import annotations

from trilogy.parsing.v2.rules.chart_rules import CHART_NODE_HYDRATORS
from trilogy.parsing.v2.rules.datasource_rules import DATASOURCE_NODE_HYDRATORS
from trilogy.parsing.v2.rules.function_definition_rules import (
    FUNCTION_DEFINITION_NODE_HYDRATORS,
)
from trilogy.parsing.v2.rules.merge_rules import MERGE_NODE_HYDRATORS
from trilogy.parsing.v2.rules.operational_rules import OPERATIONAL_NODE_HYDRATORS
from trilogy.parsing.v2.rules.persist_rules import PERSIST_NODE_HYDRATORS
from trilogy.parsing.v2.rules.rawsql_rules import RAWSQL_NODE_HYDRATORS
from trilogy.parsing.v2.rules.rowset_rules import ROWSET_NODE_HYDRATORS
from trilogy.parsing.v2.rules.type_rules import TYPE_NODE_HYDRATORS
from trilogy.parsing.v2.rules_context import NodeHydrator
from trilogy.parsing.v2.syntax import SyntaxNodeKind

STATEMENT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = (
    CHART_NODE_HYDRATORS
    | DATASOURCE_NODE_HYDRATORS
    | FUNCTION_DEFINITION_NODE_HYDRATORS
    | MERGE_NODE_HYDRATORS
    | OPERATIONAL_NODE_HYDRATORS
    | PERSIST_NODE_HYDRATORS
    | RAWSQL_NODE_HYDRATORS
    | ROWSET_NODE_HYDRATORS
    | TYPE_NODE_HYDRATORS
)
