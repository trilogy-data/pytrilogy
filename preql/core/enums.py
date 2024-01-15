from enum import Enum
from dataclasses import dataclass
from typing import Union

InfiniteFunctionArgs = -1


class UnnestMode(Enum):
    DIRECT = "direct"
    CROSS_APPLY = "cross_apply"


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
    AUTO = "auto"


class PurposeLineage(Enum):
    BASIC = "basic"
    WINDOW = "window"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    CONSTANT = "constant"
    UNNEST = "unnest"


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
    DATE_PART = "date_part"

    # GRANULAR
    UNIX_SECONDS = "unix_seconds"

    # PARSING
    UNKNOWN = "unknown"


@dataclass
class ArrayType:
    type: Union["ArrayType", "MapType", DataType]


@dataclass
class MapType:
    key_type: DataType
    content_type: Union["ArrayType", DataType]


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
    # structural
    UNNEST = "unnest"

    # Generic
    CASE = "case"
    CAST = "cast"
    CONCAT = "concat"
    CONSTANT = "constant"
    COALESCE = "coalesce"

    # COMPLEX
    INDEX_ACCESS = "index_access"

    # TEXT AND MAYBE MORE
    SPLIT = "split"

    # Math
    DIVIDE = "divide"
    MULTIPLY = "multiply"
    ADD = "add"
    SUBTRACT = "subtract"
    ROUND = "round"
    ABS = "abs"

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

    # CONSTANTS
    CURRENT_DATE = "current_date"
    CURRENT_DATETIME = "current_datetime"


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
    IS = "is"
    IS_NOT = "is not"
    GTE = ">="
    LTE = "<="
    NE = "!="
    IN = "in"
    NOT_IN = "not in"
    # TODO: deprecate for contains?
    LIKE = "like"
    ILIKE = "ilike"
    CONTAINS = "contains"

    @classmethod
    def _missing_(cls, value):
        if not isinstance(value, list) and " " in str(value):
            value = str(value).split()
        if isinstance(value, list):
            processed = [str(v).lower() for v in value]
            if processed == ["not", "in"]:
                return ComparisonOperator.NOT_IN
            if processed == ["is", "not"]:
                return ComparisonOperator.IS_NOT
            if value == ["in"]:
                return ComparisonOperator.IN
        return super()._missing_(str(value).lower())


class DatePart(Enum):
    MONTH = "month"
    YEAR = "year"
    WEEK = "week"
    DAY = "day"
    QUARTER = "quarter"


class SourceType(Enum):
    FILTER = "filter"
    SELECT = "select"
    GROUP = "group"
    WINDOW = "window"
    UNNEST = "unnest"
