from enum import Enum


class StatementType(Enum):
    QUERY = "query"


class Purpose(Enum):
    KEY = "key"
    PROPERTY = "property"
    METRIC = "metric"


class PurposeLineage(Enum):
    BASIC = "basic"
    WINDOW = "window"
    AGGREGATE = "aggregate"


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
    LEFT_OUTER = "left outer"
    FULL = "full"
    RIGHT_OUTER = "right outer"
    CROSS = "cross"


class Ordering(Enum):
    ASCENDING = "asc"
    DESCENDING = "desc"


class WindowType(Enum):
    ROW_NUMBER = "row_number"


class WindowOrder(Enum):
    ASCENDING = "top"
    DESCENDING = "bottom"


class FunctionType(Enum):
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    LENGTH = "len"
    LIKE = "like"
    CONCAT = "concat"
    NOT_LIKE = "not_like"


class FunctionClass(Enum):
    AGGREGATE_FUNCTIONS = [FunctionType.SUM, FunctionType.AVG, FunctionType.COUNT]


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
