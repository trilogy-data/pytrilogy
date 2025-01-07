from typing import List


class UndefinedConceptException(Exception):
    def __init__(self, message, suggestions: List[str]):
        super().__init__(self, message)
        self.message = message
        self.suggestions = suggestions


class UnresolvableQueryException(Exception):
    pass


class InvalidSyntaxException(Exception):
    pass


class NoDatasourceException(Exception):
    pass


class FrozenEnvironmentException(Exception):
    pass


class AmbiguousRelationshipResolutionException(Exception):
    def __init__(self, message, parents: List[set[str]]):
        super().__init__(self, message)
        self.message = message
        self.parents = parents
