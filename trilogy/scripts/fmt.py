"""Format command for Trilogy CLI."""

from datetime import datetime

from click import Path, argument, pass_context

from trilogy import parse
from trilogy.parsing.render import Renderer
from trilogy.scripts.common import handle_execution_exception
from trilogy.scripts.display import print_success, show_formatting_result, with_status


@argument("input", type=Path(exists=True))
@pass_context
def fmt(ctx, input):
    """Format a Trilogy script file."""
    with with_status("Formatting script"):
        start = datetime.now()
        try:
            with open(input, "r") as f:
                script = f.read()
            _, queries = parse(script)
            r = Renderer()
            with open(input, "w") as f:
                f.write("\n".join([r.to_string(x) for x in queries]))
            duration = datetime.now() - start

            print_success("Script formatted successfully")
            show_formatting_result(input, len(queries), duration)

        except Exception as e:
            handle_execution_exception(e, debug=ctx.obj["DEBUG"])
