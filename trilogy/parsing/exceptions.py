class ParseError(Exception):
    pass


class NameShadowError(ParseError):
    """
    Raised when a name shadows another name in the same scope.
    """
