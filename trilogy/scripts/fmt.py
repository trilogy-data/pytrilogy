"""Format command for Trilogy CLI."""

from datetime import datetime
from pathlib import Path as PathLib

from click import Path, argument, pass_context

from trilogy.core.models.environment import Environment
from trilogy.parsing.render import Renderer
from trilogy.scripts.common import handle_execution_exception
from trilogy.scripts.display import print_success, show_formatting_result, with_status
from trilogy.utility import safe_open


def format_file(file_path: str) -> tuple[str, int, bool, str | None]:
    """Format a single file and return results."""
    try:
        path = PathLib(file_path)
        with safe_open(file_path) as f:
            script = f.read()
        # Imports resolve relative to the file's directory, not the caller's CWD.
        env = Environment(working_path=path.parent)
        _, queries = env.parse(script)
        # Pass the environment so the renderer can resolve virtual
        # (``_virt_...``) concept refs back into their inline lineage.
        r = Renderer(environment=env)
        with safe_open(file_path, "w", newline="\n") as f:
            f.write(r.render_statement_string(queries) + "\n")
        return (file_path, len(queries), True, None)
    except Exception as e:
        return (file_path, 0, False, str(e))


def find_preql_files(path: PathLib) -> list[str]:
    """Recursively find all .preql files in a directory."""
    return [str(p) for p in path.rglob("*.preql")]


@argument("input", type=Path(exists=True))
@pass_context
def fmt(ctx, input):
    """Format a Trilogy script file or directory."""
    start = datetime.now()
    input_path = PathLib(input)

    # Determine files to format
    if input_path.is_file():
        files = [str(input_path)]
    elif input_path.is_dir():
        files = find_preql_files(input_path)
        if not files:
            print_success(f"No .preql files found in {input}")
            return
    else:
        print_success(f"Invalid path: {input}")
        return

    total_files = len(files)
    total_queries = 0
    failed_files = []

    with with_status(f"Formatting {total_files} file(s)"):
        try:
            # Sequential processing — files import each other, so a parallel
            # write-during-read leaves importers reading partial state and
            # failing with "Unable to import" against in-flight neighbours.
            for f in files:
                file_path, query_count, success, error = format_file(f)
                if success:
                    total_queries += query_count
                else:
                    failed_files.append((file_path, error))

            duration = datetime.now() - start

            if failed_files:
                for file_path, error in failed_files:
                    print_success(f"Failed to format {file_path}: {error}")
                print_success(
                    f"Formatted {total_files - len(failed_files)}/{total_files} files "
                    f"with {total_queries} total queries in {duration}"
                )
            else:
                print_success(
                    f"Successfully formatted {total_files} file(s) "
                    f"with {total_queries} total queries"
                )
                if total_files == 1:
                    show_formatting_result(files[0], total_queries, duration)
                else:
                    print_success(f"Duration: {duration}")

        except Exception as e:
            handle_execution_exception(e, debug=ctx.obj["DEBUG"])
