from typing import List


class UndefinedConceptException(Exception):
    def __init__(self, message, suggestions: List[str]):
        super().__init__(self, message)
        self.message = message
        self.suggestions = suggestions


class FrozenEnvironmentException(Exception):
    pass


class InvalidSyntaxException(Exception):
    pass


class UnresolvableQueryException(Exception):
    pass


class NoDatasourceException(UnresolvableQueryException):
    pass


class AmbiguousRelationshipResolutionException(UnresolvableQueryException):
    def __init__(self, message, parents: List[set[str]]):
        super().__init__(self, message)
        self.message = message
        self.parents = parents
