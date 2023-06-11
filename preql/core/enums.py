from enum import Enum

InfiniteFunctionArgs = -1


class ConceptSource(Enum):
    MANUAL = "manual"
    AUTO_DERIVED = "auto_derived"


class StatementType(Enum):
    QUERY = "query"


class Purpose(Enum):
    CONSTANT = "const"
    KEY = "key"
    PROPERTY = "property"
    METRIC = "metric"


class PurposeLineage(Enum):
    BASIC = "basic"
    WINDOW = "window"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    CONSTANT = "constant"


class Modifier(Enum):
    PARTIAL = "Partial"
    OPTIONAL = "Optional"
    HIDDEN = "Hidden"


class DataType(Enum):
    # PRIMITIVES
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

    # GRANULAR
    UNIX_SECONDS = "unix_seconds"


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
    RANK = "rank"
    LAG = "lag"
    LEAD = "lead"


class WindowOrder(Enum):
    ASCENDING = "top"
    DESCENDING = "bottom"


class FunctionType(Enum):
    # Generic
    CASE = "case"
    CAST = "cast"
    CONCAT = "concat"
    CONSTANT = "constant"

    # Math
    DIVIDE = "divide"
    MULTIPLY = "multiply"
    ADD = "add"
    SUBTRACT = "subtract"
    ROUND = "round"

    # Aggregates
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM = "sum"
    MAX = "max"
    MIN = "min"
    AVG = "avg"
    LENGTH = "len"

    # String
    LIKE = "like"
    ILIKE = "ilike"
    LOWER = "lower"
    UPPER = "upper"

    # Dates
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"

    # time
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    DAY_OF_WEEK = "day_of_week"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"

    DATE_PART = "date_part"
    DATE_TRUNCATE = "date_truncate"

    # UNIX
    UNIX_TO_TIMESTAMP = "unix_to_timestamp"


class FunctionClass(Enum):
    AGGREGATE_FUNCTIONS = [
        FunctionType.MAX,
        FunctionType.MIN,
        FunctionType.SUM,
        FunctionType.AVG,
        FunctionType.COUNT,
        FunctionType.COUNT_DISTINCT,
    ]


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
    IN = "in"
    # TODO: deprecate for contains?
    LIKE = "like"
    ILIKE = "ilike"
    CONTAINS = "contains"

    @classmethod
    def _missing_(cls, value):
        if value == "is":
            return ComparisonOperator.EQ
        if str(value).lower() == "like":
            return ComparisonOperator.LIKE
        if str(value).lower() == "ilike":
            return ComparisonOperator.ILIKE
        if str(value).lower() == "contains":
            return ComparisonOperator.CONTAINS
        return super()._missing_(value)


class SourceType(Enum):
    FILTER = "filter"
    SELECT = "select"
    GROUP = "group"
    WINDOW = "window"
