from pydantic import BaseModel


class ParseConfig(BaseModel):
    duplicate_declarations_allowed: bool = True
