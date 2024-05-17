from enum import Enum

InfiniteFunctionArgs = -1


class UnnestMode(Enum):
    DIRECT = "direct"
    CROSS_APPLY = "cross_apply"
    CROSS_JOIN = "cross_join"


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

    @classmethod
    def _missing_(cls, value):
        if value == "constant":
            return Purpose.CONSTANT
        return super()._missing_(value)


class PurposeLineage(Enum):
    BASIC = "basic"
    WINDOW = "window"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    CONSTANT = "constant"
    UNNEST = "unnest"
    ROOT = "root"


class Granularity(Enum):
    SINGLE_ROW = "single_row"
    MULTI_ROW = "multi_row"


class Modifier(Enum):
    PARTIAL = "Partial"
    OPTIONAL = "Optional"
    HIDDEN = "Hidden"

    @classmethod
    def _missing_(cls, value):
        strval = str(value)
        if strval == "~":
            return Modifier.PARTIAL
        return super()._missing_(value=strval.capitalize())


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
    SUM = "sum"
    MAX = "max"
    MIN = "min"
    AVG = "avg"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"


class WindowOrder(Enum):
    ASCENDING = "top"
    DESCENDING = "bottom"


class FunctionType(Enum):
    # custom
    CUSTOM = "custom"

    # structural
    UNNEST = "unnest"
    ALIAS = "alias"

    # Generic
    CASE = "case"
    CAST = "cast"
    CONCAT = "concat"
    CONSTANT = "constant"
    COALESCE = "coalesce"
    IS_NULL = "isnull"

    # COMPLEX
    INDEX_ACCESS = "index_access"
    ATTR_ACCESS = "attr_access"

    # TEXT AND MAYBE MORE
    SPLIT = "split"

    # Math
    DIVIDE = "divide"
    MULTIPLY = "multiply"
    ADD = "add"
    SUBTRACT = "subtract"
    MOD = "mod"
    ROUND = "round"
    ABS = "abs"

    # Aggregates
    ## group is not a real aggregate - it just means group by this + some other set of fields
    ## but is here as syntax is identical
    GROUP = "group"

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
    SUBSTRING = "substring"
    STRPOS = "strpos"

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
    DATE_ADD = "date_add"
    DATE_DIFF = "date_diff"

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
    ELSE = "else"

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
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str) and value.lower() != value:
            return DatePart(value.lower())
        return super()._missing_(value)


class SourceType(Enum):
    FILTER = "filter"
    SELECT = "select"
    MERGE = "merge"
    ABSTRACT = "abstract"
    DIRECT_SELECT = "direct_select"
    GROUP = "group"
    WINDOW = "window"
    UNNEST = "unnest"
    CONSTANT = "constant"


class ShowCategory(Enum):
    MODELS = "models"
    CONCEPTS = "concepts"
