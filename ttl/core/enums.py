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
    INTEGER = "int"

class JoinType(Enum):
    INNER = "inner"
    OUTER = "outer"
    FULL = "full"
    LEFT= "left"
    RIGHT = "right"


class Ordering(Enum):
    ASCENDING = 'asc'
    DESCENDING = 'desc'