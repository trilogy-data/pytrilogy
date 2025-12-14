import re


def canonicolize_name(name: str) -> str:
    """Convert a string to snake_case.

    Handles CamelCase, PascalCase, and names with spaces/special chars.
    """
    # Handle spaces and special characters first
    name = re.sub(r"[^\w\s]", "_", name)
    name = re.sub(r"\s+", "_", name)

    # Insert underscores before uppercase letters (for CamelCase)
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)

    # Convert to lowercase and remove duplicate underscores
    name = name.lower()
    name = re.sub(r"_+", "_", name)

    # Remove leading/trailing underscores
    return name.strip("_")


def find_common_prefix(names: list[str]) -> str:
    """Find the common prefix shared by all names in a list.

    The prefix is determined by finding the longest common substring
    that ends with an underscore (or is followed by an underscore in all names).

    Args:
        names: List of names to analyze

    Returns:
        The common prefix (including trailing underscore), or empty string if none found
    """
    if not names or len(names) < 2:
        return ""

    # Normalize all to lowercase for comparison
    normalized = [name.lower() for name in names]

    # Start with the first name as potential prefix
    prefix = normalized[0]

    # Find common prefix across all names
    for name in normalized[1:]:
        # Find where they start to differ
        i = 0
        while i < len(prefix) and i < len(name) and prefix[i] == name[i]:
            i += 1
        prefix = prefix[:i]

        if not prefix:
            return ""

    # Find the last underscore in the common prefix
    last_underscore = prefix.rfind("_")

    # Only consider it a valid prefix if:
    # 1. There's an underscore
    # 2. The prefix is at least 2 characters (excluding the underscore)
    # 3. All names have content after the prefix
    if last_underscore > 0:
        candidate_prefix = prefix[: last_underscore + 1]
        # Check that all names have content after this prefix
        if all(len(name) > len(candidate_prefix) for name in normalized):
            return candidate_prefix

    return ""


def canonicalize_names(names: list[str]) -> dict[str, str]:
    if not names:
        return {}

    common_prefix = find_common_prefix(names)

    if not common_prefix:
        # No common prefix, return names as-is
        return {name: canonicolize_name(name) for name in names}

    # Strip the prefix and normalize to snake_case
    result = {}
    for name in names:
        # Remove the prefix (case-insensitive)
        if name.lower().startswith(common_prefix):
            stripped = name[len(common_prefix) :]
        else:
            stripped = name
        result[name] = canonicolize_name(stripped)

    return result
