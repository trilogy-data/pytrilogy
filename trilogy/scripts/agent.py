"""Agent command for Trilogy CLI - AI-powered orchestration tasks."""

from click import argument, option, pass_context


@argument("command", type=str)
@option(
    "--context",
    "-c",
    multiple=True,
    help="Additional context files or paths for the agent",
)
@option("--model", "-m", type=str, help="AI model to use (if configured)")
@option(
    "--interactive", "-i", is_flag=True, help="Run in interactive mode with feedback"
)
@pass_context
def agent(
    ctx, command: str, context: tuple[str, ...], model: str | None, interactive: bool
):
    """Pass off a multi-step orchestration task to an AI agent.

    This command allows you to delegate complex, multi-step tasks to a configured
    AI agent. The agent can understand natural language commands and execute
    a series of Trilogy operations to accomplish the goal.

    Examples:
        trilogy agent "analyze sales trends and create a dashboard"
        trilogy agent "ingest new data and run validation tests"
        trilogy agent "optimize query performance for customer reports"

    Args:
        command: Natural language command describing the task
        context: Additional context files or paths to inform the agent
        model: Specific AI model to use (requires configuration)
        interactive: Enable interactive mode for step-by-step feedback
    """
    raise NotImplementedError(
        "The 'agent' command is not yet implemented. "
        "Configure an AI agent in your trilogy.toml to use this feature."
    )
