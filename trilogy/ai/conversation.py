from dataclasses import dataclass
from typing import Literal, Union

from trilogy import Environment
from trilogy.ai.models import LLMMessage, LLMRequestOptions, LLMResponse
from trilogy.ai.prompts import (
    TRILOGY_CREATE_QUERY_TOOL,
    TRILOGY_LEAD_IN,
    TRILOGY_QUERY_TOOL,
    create_query_prompt,
    create_query_request_options,
)
from trilogy.ai.providers.base import LLMProvider
from trilogy.core.exceptions import (
    InvalidSyntaxException,
    NoDatasourceException,
    UndefinedConceptException,
    UnresolvableQueryException,
)
from trilogy.core.query_processor import process_query


@dataclass
class Conversation:

    messages: list[LLMMessage]
    provider: LLMProvider
    id: str | None = None

    @classmethod
    def create(
        cls,
        provider: LLMProvider,
        model_prompt: str = TRILOGY_LEAD_IN,
        id: str | None = None,
    ) -> "Conversation":
        system_message = LLMMessage(role="system", content=model_prompt)
        messages = [system_message]
        return cls(id=id, messages=messages, provider=provider)

    def add_message(
        self,
        message: Union[LLMMessage, str],
        role: Literal["user", "assistant"] = "user",
    ) -> None:
        """
        Add a message to the conversation.

        Args:
            message: Either an LLMMessage object or a string content
            role: The role for the message if a string is provided (default: 'user')
        """
        if isinstance(message, str):
            message = LLMMessage(role=role, content=message)
        self.messages.append(message)

    def get_response(self, options: LLMRequestOptions | None = None) -> LLMResponse:
        response = self.provider.generate_completion(
            options or LLMRequestOptions(), history=self.messages
        )
        model_info = {}
        if response.tool_calls:
            model_info["tool_calls"] = [
                {"name": tool_call.name, "arguments": tool_call.arguments}
                for tool_call in response.tool_calls
            ]
        response_message = LLMMessage(
            role="assistant", content=response.text, model_info=model_info
        )
        self.add_message(response_message)
        return response

    def _normalize_query(self, query: str) -> str:
        normalized = query.strip()
        if normalized and not normalized.endswith(";"):
            normalized += ";"
        return normalized

    def _validate_query(
        self, query: str, environment: Environment
    ) -> tuple[str, str | None]:
        normalized = self._normalize_query(query)
        try:
            _, raw = environment.parse(normalized)
            processed = process_query(statement=raw[-1], environment=environment)
            return (
                f"environment.parse succeeded and process_query returned: {processed}",
                None,
            )
        except (
            InvalidSyntaxException,
            NoDatasourceException,
            UnresolvableQueryException,
            UndefinedConceptException,
            SyntaxError,
        ) as error:
            return ("", str(error))

    def generate_query(
        self, user_input: str, environment: Environment, attempts: int = 4
    ) -> str:
        self.add_message(create_query_prompt(user_input, environment), role="user")
        last_error: str | None = None
        for _ in range(attempts):
            response_message = self.get_response(create_query_request_options())
            if not response_message.tool_calls:
                self.add_message(
                    f"You must call either {TRILOGY_CREATE_QUERY_TOOL.name} to validate a draft query or {TRILOGY_QUERY_TOOL.name} to submit the final query.",
                    role="user",
                )
                continue

            for tool_call in response_message.tool_calls:
                query = tool_call.arguments.get("query")
                if not isinstance(query, str) or not query.strip():
                    last_error = (
                        f"Tool {tool_call.name} was called without a non-empty query."
                    )
                    self.add_message(
                        f"{last_error} Call {TRILOGY_CREATE_QUERY_TOOL.name} or {TRILOGY_QUERY_TOOL.name} with a query string.",
                        role="user",
                    )
                    continue

                result, validation_error = self._validate_query(query, environment)
                normalized = self._normalize_query(query)

                if tool_call.name == TRILOGY_CREATE_QUERY_TOOL.name:
                    last_error = validation_error
                    if validation_error:
                        self.add_message(
                            f"{TRILOGY_CREATE_QUERY_TOOL.name} failed for query {normalized} with validation error: {validation_error}",
                            role="user",
                        )
                    else:
                        self.add_message(
                            f"{TRILOGY_CREATE_QUERY_TOOL.name} succeeded for query {normalized}. {result}",
                            role="user",
                        )
                    continue

                if tool_call.name == TRILOGY_QUERY_TOOL.name:
                    if validation_error is None:
                        return normalized
                    last_error = validation_error
                    self.add_message(
                        f"{TRILOGY_QUERY_TOOL.name} failed for query {normalized} with validation error: {validation_error}. Use {TRILOGY_CREATE_QUERY_TOOL.name} to validate the next draft before submitting it again.",
                        role="user",
                    )
                    continue

                last_error = f"Unknown tool call: {tool_call.name}."
                self.add_message(
                    f"Unknown tool call: {tool_call.name}. Use only {TRILOGY_CREATE_QUERY_TOOL.name} and {TRILOGY_QUERY_TOOL.name}.",
                    role="user",
                )
        raise Exception(
            f"Failed to generate a valid query after {attempts} attempts. Last error: {last_error}. Full conversation: {self.messages}"
        )
