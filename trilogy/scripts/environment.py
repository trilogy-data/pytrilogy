from pathlib import Path
from typing import Any, Iterable


def smart_convert(value: str):
    """Convert string to appropriate Python type."""
    if not value:
        return value

    # Handle booleans
    if value.lower() in ("true", "false"):
        return value.lower() == "true"

    # Try numeric conversion
    try:
        if "." not in value and "e" not in value.lower():
            return int(value)
        return float(value)
    except ValueError:
        return value


def extra_to_kwargs(arg_list: Iterable[str]) -> dict[str, Any]:
    """Parse connection args given as ``key value`` pairs or ``key=value`` tokens."""
    args = list(arg_list)
    final: dict[str, Any] = {}
    i = 0
    while i < len(args):
        key = args[i].lstrip("-")
        if "=" in key:
            key, value = key.split("=", 1)
            i += 1
        elif i + 1 < len(args):
            value = args[i + 1]
            i += 2
        else:
            raise ValueError(
                f"Connection argument '{args[i]}' has no value; pass connection "
                "arguments as 'key value' pairs or 'key=value'"
            )
        final[key] = smart_convert(value)
    return final


def parse_env_params(env_param_list: tuple[str, ...]) -> dict[str, Any]:
    """Parse environment parameters from key=value format with type conversion."""
    env_params: dict[str, Any] = {}
    for param in env_param_list:
        if "=" not in param:
            raise ValueError(
                f"Environment parameter must be in key=value format: {param}"
            )
        key, value = param.split("=", 1)  # Split on first = only
        env_params[key] = smart_convert(value)
    return env_params


def parse_env_vars(env_var_list: tuple[str, ...]) -> dict[str, str]:
    """Parse env values from KEY=VALUE entries or env-file paths."""
    from trilogy.execution.config import load_env_file

    env_vars: dict[str, str] = {}
    for param in env_var_list:
        if "=" in param:
            key, value = param.split("=", 1)
            env_vars[key] = value
            continue

        env_file = Path(param)
        if not env_file.exists():
            raise ValueError(
                "Environment variable must be in KEY=VALUE format or be a path "
                f"to an existing env file: {param}"
            )

        if not env_file.is_file():
            raise ValueError(
                "Environment variable path must point to a file when using "
                f"--env FILE: {param}"
            )

        file_vars = load_env_file(env_file)
        if file_vars:
            env_vars.update(file_vars)

    return env_vars
