from dataclasses import dataclass
from typing import Literal, Union

from trilogy import Environment
from trilogy.ai.models import LLMMessage, LLMRequestOptions
from trilogy.ai.prompts import TRILOGY_LEAD_IN, create_query_prompt
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

    def get_response(self) -> LLMMessage:
        options = LLMRequestOptions()
        response = self.provider.generate_completion(options, history=self.messages)
        response_message = LLMMessage(role="assistant", content=response.text)
        self.add_message(response_message)
        return response_message

    def extract_response(self, content: str) -> str:
        # get contents in triple backticks
        content = content.replace('"""', "```")
        if "```" in content:
            parts = content.split("```")
            if len(parts) >= 3:
                return parts[1].strip()
        return content

    def generate_query(
        self, user_input: str, environment: Environment, attempts: int = 4
    ) -> str:
        attempts = 0
        self.add_message(create_query_prompt(user_input, environment), role="user")
        e = None
        while attempts < 4:
            attempts += 1

            response_message = self.get_response()
            response = self.extract_response(response_message.content)
            if not response.strip()[-1] == ";":
                response += ";"
            try:
                env, raw = environment.parse(response)
                process_query(statement=raw[-1], environment=environment)
                return response
            except (
                InvalidSyntaxException,
                NoDatasourceException,
                UnresolvableQueryException,
                UndefinedConceptException,
                SyntaxError,
            ) as e2:
                e = e2
                self.add_message(
                    f"The previous response could not be parsed due to the error: {str(e)}. Please generate a new query with the issues fixed. Use the same response format.",
                    role="user",
                )

        raise Exception(
            f"Failed to generate a valid query after {attempts} attempts. Last error: {str(e)}. Full conversation: {self.messages}"
        )
