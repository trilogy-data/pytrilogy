from typing import List


class UndefinedConceptException(Exception):
    def __init__(self, message, suggestions: List[str]):
        super().__init__(self, message)
        self.message = message
        self.suggestions = suggestions


class InvalidSyntaxException(Exception):
    pass
