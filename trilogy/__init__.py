from trilogy.constants import CONFIG
from trilogy.core.models import Environment
from trilogy.dialect.enums import Dialects
from trilogy.executor import Executor
from trilogy.parser import parse

__version__ = "0.0.2.55"

__all__ = ["parse", "Executor", "Dialects", "Environment", "CONFIG"]
