from enum import Enum


class StatementType(Enum):
    QUERY = "query"


class Purpose(Enum):
    KEY = "key"
    PROPERTY = "property"
    METRIC = "metric"


class Modifier(Enum):
    PARTIAL = "Partial"
    OPTIONAL = "Optional"


class DataType(Enum):
    STRING = "string"
    BOOL = "bool"
    MAP = "map"
    LIST = "list"
    NUMBER = "number"
    FLOAT = "float"
    INTEGER = "int"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    ARRAY = "array"


class JoinType(Enum):
    INNER = "inner"
    OUTER = "outer"
    FULL = "full"
    LEFT = "left"
    RIGHT = "right"


class Ordering(Enum):
    ASCENDING = "asc"
    DESCENDING = "desc"


class FunctionType(Enum):
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    LENGTH = "len"
    LIKE = "like"
    NOT_LIKE = "not_like"


class Boolean(Enum):
    TRUE = "true"
    FALSE = "false"


class BooleanOperator(Enum):
    AND = "and"
    OR = "or"


class ComparisonOperator(Enum):
    LT = "<"
    GT = ">"
    EQ = "="
    GTE = ">="
    LTE = "<="
    NE = "!="


class LogicalOperator(Enum):
    AND = "and"
    OR = "or"
