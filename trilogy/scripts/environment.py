from typing import Any, Iterable


def pairwise(t: Iterable[Any]) -> Iterable[tuple[Any, Any]]:
    it = iter(t)
    return zip(it, it)


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
    pairs = pairwise(arg_list)
    final = {}
    for k, v in pairs:
        k = k.lstrip("--")
        final[k] = smart_convert(v)
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
    """Parse environment variables from KEY=VALUE format (keeps values as strings)."""
    env_vars: dict[str, str] = {}
    for param in env_var_list:
        if "=" not in param:
            raise ValueError(
                f"Environment variable must be in KEY=VALUE format: {param}"
            )
        key, value = param.split("=", 1)
        env_vars[key] = value
    return env_vars
