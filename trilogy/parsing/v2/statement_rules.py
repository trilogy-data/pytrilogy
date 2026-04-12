from __future__ import annotations

from trilogy.parsing.v2.datasource_rules import DATASOURCE_NODE_HYDRATORS
from trilogy.parsing.v2.function_definition_rules import (
    FUNCTION_DEFINITION_NODE_HYDRATORS,
)
from trilogy.parsing.v2.merge_rules import MERGE_NODE_HYDRATORS
from trilogy.parsing.v2.persist_rules import PERSIST_NODE_HYDRATORS
from trilogy.parsing.v2.rawsql_rules import RAWSQL_NODE_HYDRATORS
from trilogy.parsing.v2.rowset_rules import ROWSET_NODE_HYDRATORS
from trilogy.parsing.v2.rules_context import NodeHydrator
from trilogy.parsing.v2.syntax import SyntaxNodeKind

STATEMENT_NODE_HYDRATORS: dict[SyntaxNodeKind, NodeHydrator] = (
    DATASOURCE_NODE_HYDRATORS
    | FUNCTION_DEFINITION_NODE_HYDRATORS
    | MERGE_NODE_HYDRATORS
    | PERSIST_NODE_HYDRATORS
    | RAWSQL_NODE_HYDRATORS
    | ROWSET_NODE_HYDRATORS
)
