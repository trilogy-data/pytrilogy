def safe_quote(string: str, quote_char: str):
    # split dotted identifiers
    # TODO: evaluate if we need smarter parsing for strings that could actually include .
    if string.startswith("https://"):
        # it's a url, no splitting
        return f"{quote_char}{string}{quote_char}"
    components = string.split(".")
    return ".".join([f"{quote_char}{string}{quote_char}" for string in components])
