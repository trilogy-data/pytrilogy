from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from trilogy.constants import CONFIG
    from trilogy.core.models.environment import Environment, EnvironmentConfig
    from trilogy.dialect.enums import Dialects
    from trilogy.executor import Executor
    from trilogy.parser import parse

__version__ = "0.3.331"

__all__ = [
    "CONFIG",
    "Dialects",
    "Environment",
    "EnvironmentConfig",
    "Executor",
    "parse",
]

# Importing the engine (environment, executor, parser) costs ~half a second,
# and every `trilogy.<anything>` import runs this file first — so CLI commands
# that never touch the engine (init, --version, file, cloud) pay if not lazy
# PEP 562 defers each name to first attribute access; `from trilogy import
# Executor` is unchanged, it just resolves on use.
_LAZY_ATTRS: dict[str, str] = {
    "CONFIG": "trilogy.constants",
    "Dialects": "trilogy.dialect.enums",
    "Environment": "trilogy.core.models.environment",
    "EnvironmentConfig": "trilogy.core.models.environment",
    "Executor": "trilogy.executor",
    "parse": "trilogy.parser",
}


def __getattr__(name: str) -> Any:
    module_path = _LAZY_ATTRS.get(name)
    if module_path is None:
        raise AttributeError(f"module 'trilogy' has no attribute '{name}'")
    from importlib import import_module

    value = getattr(import_module(module_path), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted([*__all__, "__version__"])
