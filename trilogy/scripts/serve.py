"""Serve command for Trilogy CLI."""

import os
import secrets
import shutil
import sys
from collections import defaultdict
from pathlib import Path as PathlibPath
from urllib.parse import quote

from click import Path, argument, option, pass_context

from trilogy.execution.config import load_config_file
from trilogy.scripts.common import find_trilogy_config
from trilogy.scripts.serve_helpers import (
    find_all_model_files,
    find_trilogy_files,
    get_relative_model_name,
    get_safe_model_name,
)

TOKEN_BYTES = 16  # 128-bit random token


def check_fastapi_available() -> bool:
    """Check if FastAPI and uvicorn are available."""
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401

        return True
    except ImportError:
        return False


def get_trilogy_cmd() -> list[str]:
    """Return the command prefix to invoke the trilogy CLI."""
    exe = shutil.which("trilogy")
    if exe:
        return [exe]
    # Fall back to running trilogy.py directly with the current interpreter
    return [sys.executable, str(PathlibPath(__file__).parent / "trilogy.py")]


def _validate_target(target: str, directory_path: PathlibPath) -> PathlibPath:
    """Resolve target and ensure it stays within the served directory."""
    from fastapi import HTTPException

    target_path = (directory_path / target).resolve()
    try:
        target_path.relative_to(directory_path)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Target must be within served directory"
        )
    if not target_path.exists():
        raise HTTPException(status_code=404, detail="Target not found")
    return target_path


def _build_cmd(
    command: str, target_path: PathlibPath, config_path: PathlibPath | None, engine: str
) -> list[str]:
    cmd = get_trilogy_cmd() + [command, str(target_path)]
    if config_path:
        cmd.extend(["--config", str(config_path)])
    elif engine != "generic":
        cmd.append(engine)
    return cmd


def _get_file_listing(directory_path: PathlibPath):  # type: ignore[return]
    from trilogy.scripts.serve_helpers.models import DirectoryListing, FileListResponse

    dirs: dict[str, list[str]] = defaultdict(list)
    for f in find_all_model_files(directory_path):
        rel = f.relative_to(directory_path)
        parent = str(rel.parent).replace("\\", "/")
        if parent == ".":
            parent = ""
        dirs[parent].append(f.name)
    return FileListResponse(
        directories=[
            DirectoryListing(directory=d, files=sorted(fs))
            for d, fs in sorted(dirs.items())
        ]
    )


def create_app(
    app,
    engine: str,
    directory_path: PathlibPath,
    host: str,
    port: int,
    token: str | None = None,
    config_path: PathlibPath | None = None,
):
    # Normalize once so every closure (including compute_state_sync) sees the
    # same representation. Avoids Windows short-name vs full-name mismatches
    # (e.g. RUNNER~1 vs runneradmin) when doing relative_to() comparisons.
    directory_path = PathlibPath(os.path.realpath(directory_path))

    from fastapi import BackgroundTasks, Depends, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import PlainTextResponse
    from fastapi.routing import APIRouter
    from fastapi.security import APIKeyHeader

    from trilogy.scripts.serve_helpers import (
        FileListResponse,
        JobRequest,
        JobStatus,
        ModelImport,
        StateResponse,
        StoreIndex,
        cancel_job,
        compute_state_sync,
        create_job,
        find_file_content_by_name,
        find_model_by_name,
        generate_model_index,
        get_job,
        run_subprocess,
    )

    url_host = "localhost" if host == "0.0.0.0" else host
    if port in (80, 443):
        base_url = f"http://{url_host}"
    else:
        base_url = f"http://{url_host}:{port}"

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # --- Token auth dependency (skipped when token is None) ---
    if token is not None:
        _api_key_header = APIKeyHeader(name="X-Trilogy-Token", auto_error=False)

        async def _require_token(
            api_key: str | None = Depends(_api_key_header),
        ) -> None:
            if api_key != token:
                raise HTTPException(
                    status_code=401, detail="Invalid or missing X-Trilogy-Token header"
                )

        router = APIRouter(dependencies=[Depends(_require_token)])
    else:
        router = APIRouter()

    # --- Existing model endpoints ---

    @router.get("/")
    async def root():
        file_count = len(find_all_model_files(directory_path))
        return {
            "message": "Trilogy Model Server",
            "description": f"Serving model '{directory_path.name}' with {file_count} files from {directory_path}",
            "endpoints": {
                "/index.json": "Get list of available models",
                "/models/<model-name>.json": "Get specific model details",
                "/files": "List all trilogy files by directory",
                "/run": "Run a target file or directory (POST)",
                "/refresh": "Refresh a target file or directory (POST)",
                "/jobs/<job-id>": "Poll background job status",
            },
        }

    @router.get("/index.json", response_model=StoreIndex)
    async def get_index() -> StoreIndex:
        return StoreIndex(
            name=f"Trilogy Models - {directory_path.name}",
            models=generate_model_index(directory_path, base_url, engine),
        )

    @router.get("/models/{model_name}.json", response_model=ModelImport)
    async def get_model(model_name: str) -> ModelImport:
        model = find_model_by_name(model_name, directory_path, base_url, engine)
        if model is None:
            raise HTTPException(status_code=404, detail="Model not found")
        return model

    @router.get("/files/{file_name}")
    async def get_file(file_name: str):
        content = find_file_content_by_name(file_name, directory_path)
        if content is None:
            raise HTTPException(status_code=404, detail="File not found")
        return PlainTextResponse(content=content)

    # --- New endpoints ---

    @router.get("/files", response_model=FileListResponse)
    async def list_files() -> FileListResponse:
        """List all trilogy/sql/csv files grouped by directory."""
        return _get_file_listing(directory_path)

    @router.post("/run", response_model=JobStatus)
    async def run_target(
        request: JobRequest, background_tasks: BackgroundTasks
    ) -> JobStatus:
        """Run a trilogy file or directory in a background subprocess."""
        target_path = _validate_target(request.target, directory_path)
        job = create_job()
        cmd = _build_cmd("run", target_path, config_path, engine)
        background_tasks.add_task(run_subprocess, job, cmd, str(directory_path))
        return JobStatus(job_id=job.job_id, status=job.status, output=job.output, error=job.error)  # type: ignore[arg-type]

    @router.post("/refresh", response_model=JobStatus)
    async def refresh_target(
        request: JobRequest, background_tasks: BackgroundTasks
    ) -> JobStatus:
        """Refresh stale assets in a trilogy file or directory in a background subprocess."""
        target_path = _validate_target(request.target, directory_path)
        job = create_job()
        cmd = _build_cmd("refresh", target_path, config_path, engine)
        background_tasks.add_task(run_subprocess, job, cmd, str(directory_path))
        return JobStatus(job_id=job.job_id, status=job.status, output=job.output, error=job.error)  # type: ignore[arg-type]

    @router.get("/jobs/{job_id}", response_model=JobStatus)
    async def get_job_status(job_id: str) -> JobStatus:
        """Poll the status of a background run or refresh job."""
        job = get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobStatus(
            job_id=job.job_id,
            status=job.status,  # type: ignore[arg-type]
            output=job.output,
            error=job.error,
            return_code=job.return_code,
        )

    @router.post("/jobs/{job_id}/cancel", response_model=JobStatus)
    async def cancel_job_endpoint(job_id: str) -> JobStatus:
        """Cancel a running background job. No-op if already finished."""
        job = cancel_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return JobStatus(
            job_id=job.job_id,
            status=job.status,  # type: ignore[arg-type]
            output=job.output,
            error=job.error,
            return_code=job.return_code,
        )

    @router.get("/state", response_model=StateResponse)
    async def get_state(target: str) -> StateResponse:
        """Return watermark and staleness state for all datasources in a trilogy file."""
        import asyncio

        target_path = _validate_target(target, directory_path)
        if not target_path.is_file():
            raise HTTPException(
                status_code=400, detail="Target must be a file, not a directory"
            )
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(
                None,
                compute_state_sync,
                target_path,
                engine,
                config_path,
                directory_path,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"State computation failed: {e}"
            )

    app.include_router(router)

    print(f"Starting Trilogy Model Server on http://{host}:{port}")
    print(f"Serving model '{directory_path.name}' from: {directory_path}")
    print(f"Engine: {engine}")
    print(f"Access the index at: http://{host}:{port}/index.json")
    print(
        f"Found {len(find_all_model_files(directory_path))} model files (.preql, .sql, .csv)"
    )
    return app


@argument("path", type=Path(exists=True, file_okay=True, dir_okay=True), default=".")
@argument("engine", type=str, required=False, default="generic")
@option("--port", "-p", default=8100, help="Port to run the server on")
@option("--host", "-h", default="0.0.0.0", help="Host to bind the server to")
@option("--timeout", "-t", default=None, type=float, help="Shutdown after N seconds")
@option(
    "--no-browser",
    is_flag=True,
    default=False,
    help="Do not open the browser automatically on startup",
)
@option(
    "--no-auth",
    is_flag=True,
    default=False,
    help="Disable token authentication (for local development only)",
)
@pass_context
def serve(
    ctx,
    path: str,
    engine: str,
    port: int,
    host: str,
    timeout: float | None,
    no_browser: bool,
    no_auth: bool,
):
    """Start a FastAPI server to expose Trilogy models from a directory or file."""
    if not check_fastapi_available():
        print(
            "Error: FastAPI and uvicorn are required for the serve command.\n"
            "Please install with: pip install pytrilogy[serve]",
            file=sys.stderr,
        )
        sys.exit(1)

    import uvicorn
    from fastapi import FastAPI

    from trilogy import __version__

    path_obj = PathlibPath(path).resolve()

    # Determine directory and target file
    if path_obj.is_file():
        directory_path = path_obj.parent
        target_file = path_obj
    else:
        directory_path = path_obj
        target_file = None

    # Load trilogy.toml for engine dialect and serve settings
    config_path = find_trilogy_config(directory_path)
    studio_url = "https://trilogydata.dev/trilogy-studio-core"
    if config_path:
        try:
            runtime_config = load_config_file(config_path)
            if runtime_config.engine_dialect and engine == "generic":
                engine = runtime_config.engine_dialect.value
            studio_url = runtime_config.serve_studio_url
        except Exception:
            pass

    token = None if no_auth else secrets.token_urlsafe(TOKEN_BYTES)

    app = FastAPI(title="Trilogy Model Server", version=__version__)
    create_app(
        app, engine, directory_path, host, port, token=token, config_path=config_path
    )

    # Generate Trilogy Studio URL
    url_host = "localhost" if host == "0.0.0.0" else host
    base_url = (
        f"http://{url_host}:{port}" if port not in (80, 443) else f"http://{url_host}"
    )

    # Find target file if not specified
    if target_file is None:
        trilogy_files = find_trilogy_files(directory_path)
        if trilogy_files:
            target_file = trilogy_files[0]

    if target_file:
        model_safe_name = get_safe_model_name(directory_path.name)
        model_url = f"{base_url}/models/{model_safe_name}.json"
        store_url = base_url
        asset_name = get_relative_model_name(target_file, directory_path)
        engine_url = "duckdb" if engine == "duck_db" else engine

        studio_link = (
            f"{studio_url}#"
            f"import={quote(model_url)}&"
            f"assetType=trilogy&"
            f"assetName={quote(asset_name)}&"
            f"modelName={quote(directory_path.name)}&"
            f"connection={quote(engine_url)}&"
            f"store={quote(store_url)}" + (f"&token={token}" if token else "")
        )

        print("\n" + "=" * 80)
        print("Trilogy Studio Link:")
        print(studio_link)
        print("=" * 80 + "\n")

        if not no_browser:
            import threading
            import webbrowser

            threading.Timer(1.0, webbrowser.open, args=[studio_link]).start()

    if timeout is not None:
        import threading

        config = uvicorn.Config(app, host=host, port=port)
        server = uvicorn.Server(config)

        def shutdown_after_timeout():
            import time

            time.sleep(timeout)
            server.should_exit = True

        timer = threading.Thread(target=shutdown_after_timeout, daemon=True)
        timer.start()
        server.run()
    else:
        uvicorn.run(app, host=host, port=port)
