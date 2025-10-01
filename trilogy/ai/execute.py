from trilogy import Environment, Executor


def generate_llm_query(
    environment: Environment,
    user_input: str,
    max_retries: int = 3,
    model: str = "gpt-4",
    temperature: float = 0.0,
) -> str:
    executor = Executor(environment=environment)
    return executor.generate_llm_query(
        user_input=user_input,
        max_retries=max_retries,
        model=model,
        temperature=temperature,
    )
