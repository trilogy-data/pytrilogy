"""Serve command for Trilogy CLI."""

import os
import secrets
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path as PathlibPath
from urllib.parse import quote, urlparse

from click import Path, argument, option, pass_context

from trilogy.dialect.config import DialectConfig
from trilogy.dialect.enums import Dialects
from trilogy.execution.config import DEFAULT_STUDIO_URL, load_config_file
from trilogy.execution.state.snapshot import StateSnapshot
from trilogy.scripts.common import find_trilogy_config
from trilogy.scripts.serve_helpers import (
    REMOTE_STORE_CONTRACT_VERSION,
    StudioBundle,
    StudioManifest,
    find_all_model_files,
    find_trilogy_files,
    get_relative_model_name,
    get_safe_model_name,
)
from trilogy.scripts.source_identity import path_token
from trilogy.utility import utc_now_iso

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
    command: str,
    target_path: PathlibPath,
    config_path: PathlibPath | None,
    engine: str,
    options: list[str] | None = None,
) -> list[str]:
    # Options go before the trailing dialect, which is positional.
    cmd = get_trilogy_cmd() + [command, str(target_path)]
    cmd.extend(options or [])
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


ALLOWED_WRITE_EXTENSIONS = {".preql", ".sql", ".csv", ".py"}


def _validate_write_path(path: str, directory_path: PathlibPath) -> PathlibPath:
    """Resolve path and ensure it stays within directory and has an allowed extension."""
    from fastapi import HTTPException

    target_path = (directory_path / path).resolve()
    try:
        target_path.relative_to(directory_path)
    except ValueError:
        raise HTTPException(
            status_code=400, detail="Path must be within served directory"
        )
    if target_path.suffix not in ALLOWED_WRITE_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Only {', '.join(sorted(ALLOWED_WRITE_EXTENSIONS))} files are allowed",
        )
    return target_path


def build_store_id(directory_path: PathlibPath, project_name: str | None) -> str:
    """Stable, collision-resistant id for the studio's store registration.

    The client namespaces every remote entity under this (`remote:<id>:<path>`)
    and matches stores by it, so two projects sharing an id merge into one
    store — same-named files collide, and because the store record holds the
    base URL, saves start routing to whichever server registered last. The id
    the client derives on its own is the base URL (`localhost:8100`), which
    every project served on that port shares and which moves with the port.

    Hence a label plus a digest of the served path: the path is what actually
    distinguishes two projects, and it doesn't change when the port does. The
    digest rather than the path itself keeps the filesystem layout out of the
    client's storage keys.

    That construction is `source_identity.path_token`, shared with the local
    half of `trilogy cloud`'s source fingerprint — the same directory names
    itself the same way to a studio store and to a pushed job. Deliberately
    *not* the git-aware `resolve_origin`: two checkouts of one repository are
    two served projects and must not merge into one store.
    """
    return path_token(directory_path, project_name)


def announce_studio_download(manifest: StudioManifest) -> None:
    megabytes = manifest.tarball.bytes / 1_000_000
    print(
        f"Fetching Trilogy Studio {manifest.version} ({megabytes:.1f} MB) "
        "— cached for future runs..."
    )


def build_hosted_studio_link(
    studio_url: str,
    model_url: str,
    asset_name: str,
    model_name: str,
    engine_url: str,
    store_url: str,
    token: str | None,
) -> str:
    """Deep link into the studio hosted on trilogydata.dev.

    Fallback only. A public origin fetching a loopback store is gated by Local
    Network Access, which the browser auto-denies when the fetch fires on page
    load — so this link needs the user to grant a permission prompt that may
    never appear. Prefer `build_local_studio_link`.
    """
    return (
        f"{studio_url}#"
        f"import={quote(model_url)}&"
        f"assetType=trilogy&"
        f"assetName={quote(asset_name)}&"
        f"modelName={quote(model_name)}&"
        f"connection={quote(engine_url)}&"
        f"store={quote(store_url)}&"
        f"remote={quote('true')}" + (f"&token={token}" if token else "")
    )


def is_loopback_studio(studio_url: str) -> bool:
    """Is this studio address on the same machine as the store we're serving?

    Only used to decide whether to print the Local Network Access caveat: that
    block applies to a *public* origin reaching a loopback store, so a studio
    already running on localhost (a dev server, another `trilogy serve`) is not
    subject to it and should not be warned about.
    """
    host = (urlparse(studio_url).hostname or "").lower()
    return host in ("localhost", "127.0.0.1", "::1", "0.0.0.0")


def build_local_studio_link(
    base_url: str,
    base_path: str,
    asset_name: str,
    model_name: str,
    store_id: str,
    token: str | None,
) -> str:
    """Deep link into the studio this server hosts — same origin as the store.

    No `import` or `connection`: the client ignores both when `remote=true` and
    reads them from /index.json instead. `storeId` is pinned so the studio's
    saved state survives a port change, which would otherwise mint a new id.
    """
    return (
        f"{base_url}{base_path}#"
        f"store={quote(base_url)}&"
        f"storeId={quote(store_id)}&"
        f"remote=true&"
        f"assetType=trilogy&"
        f"assetName={quote(asset_name)}&"
        f"modelName={quote(model_name)}" + (f"&token={token}" if token else "")
    )


def create_app(
    app,
    engine: str,
    directory_path: PathlibPath,
    host: str,
    port: int,
    token: str | None = None,
    config_path: PathlibPath | None = None,
    project_name: str | None = None,
    connection_type: Dialects | str | None = None,
    connection_options: dict[str, str] | None = None,
    engine_config: DialectConfig | None = None,
    startup_scripts: list[PathlibPath] | None = None,
    enable_state_cache: bool = True,
    studio_bundle: StudioBundle | None = None,
):
    # Normalize once so every closure (including the state probe) sees the
    # same representation. Avoids Windows short-name vs full-name mismatches
    # (e.g. RUNNER~1 vs runneradmin) when doing relative_to() comparisons.
    directory_path = PathlibPath(os.path.realpath(directory_path))

    from fastapi import BackgroundTasks, Depends, HTTPException, Response
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import PlainTextResponse
    from fastapi.routing import APIRouter
    from fastapi.security import APIKeyHeader

    from trilogy.scripts.serve_helpers import (
        ConnectionSpec,
        FileCreateRequest,
        FileListResponse,
        FileWriteRequest,
        JobRequest,
        JobStatus,
        ModelImport,
        StateSnapshotCache,
        StoreIndex,
        build_connection_spec,
        cancel_job,
        compute_state_snapshot_sync,
        create_job,
        find_model_by_name,
        fingerprint_directory,
        generate_model_index,
        get_job,
        relative_target,
        run_subprocess,
    )

    state_cache = StateSnapshotCache(directory_path) if enable_state_cache else None

    def _job_state_options(cache_key: str) -> tuple[list[str], PathlibPath | None]:
        """State-store flags for a job, and the file to adopt when it finishes.

        The cache doubles as the job's state store, in both directions:
        ``--state-input`` hands the job the observations the server already
        holds instead of making it re-probe, and ``--state-file`` has it write
        a fresh snapshot back. Nothing has to be kept in sync by hand.

        The seeding half means a job trusts recorded state rather than reading
        the warehouse for it, which is the documented meaning of
        ``--state-input``. A table loaded outside trilogy is therefore invisible
        to it, exactly as it is to a cached ``/state``. ``--no-state-cache``
        turns off both halves together, since they are the same trust decision.
        """
        if state_cache is None:
            return [], None
        options: list[str] = []
        seed = state_cache.state_input_path(
            cache_key, fingerprint_directory(directory_path)
        )
        if seed is not None:
            options.extend(["--state-input", str(seed)])
        handle, written = tempfile.mkstemp(suffix=".trilogy-state.json")
        os.close(handle)
        options.extend(["--state-file", written])
        return options, PathlibPath(written)

    def _finish_job(cache_key: str, written: PathlibPath | None) -> None:
        """Refresh the cache from a finished job.

        Only the job's own target gets a new snapshot — that is the only one it
        wrote a probe for. Every other entry is dropped rather than reasoned
        about: a job rewrites assets, targets overlap (a directory contains its
        files), and state flows downstream, so anything narrower would have to
        model the dependency graph to be sound. Getting that wrong shows a stale
        "fresh", which is the one answer this must never invent.
        """
        if state_cache is None:
            return
        state_cache.clear()
        if written is None:
            return
        if written.exists():
            state_cache.adopt(cache_key, written, fingerprint_directory(directory_path))
        written.unlink(missing_ok=True)

    url_host = "localhost" if host == "0.0.0.0" else host
    if port in (80, 443):
        base_url = f"http://{url_host}"
    else:
        base_url = f"http://{url_host}:{port}"

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        # A browser hides every non-safelisted response header from JS unless it
        # is named here, so the studio could not read its own cache status.
        expose_headers=["X-Trilogy-Cached", "X-Trilogy-Computed-At"],
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
            "contract_version": REMOTE_STORE_CONTRACT_VERSION,
            "endpoints": {
                "/index.json": "Get list of available models",
                "/models/<model-name>.json": "Get specific model details",
                "/files": "List all trilogy files by directory",
                "/run": "Run a target file or directory (POST)",
                "/refresh": "Refresh a target file or directory (POST)",
                "/jobs/<job-id>": "Poll background job status",
            },
        }

    connection_spec: ConnectionSpec | None = build_connection_spec(
        connection_type, connection_options, engine, engine_config
    )

    # Resolve startup script paths to posix paths relative to the served
    # directory. A script the client can't reach is dropped rather than
    # advertised: an entry only means anything if it also shows up in /files,
    # which requires it to exist, live under `directory_path`, and be one of
    # the served extensions.
    resolved_startup_scripts: list[str] = []
    for script in startup_scripts or []:
        script_abs = script if script.is_absolute() else directory_path / script
        try:
            script_real = PathlibPath(os.path.realpath(script_abs))
            rel = script_real.relative_to(directory_path)
        except ValueError:
            continue
        if (
            script_real.suffix not in ALLOWED_WRITE_EXTENSIONS
            or not script_real.is_file()
        ):
            continue
        resolved_startup_scripts.append(rel.as_posix())

    @router.get("/index.json", response_model=StoreIndex)
    async def get_index() -> StoreIndex:
        return StoreIndex(
            name=project_name or f"Trilogy Models - {directory_path.name}",
            models=generate_model_index(directory_path, base_url, engine),
            project_name=project_name,
            connection=connection_spec,
            startup_scripts=list(resolved_startup_scripts),
        )

    @router.get("/models/{model_name}.json", response_model=ModelImport)
    async def get_model(model_name: str) -> ModelImport:
        model = find_model_by_name(model_name, directory_path, base_url, engine)
        if model is None:
            raise HTTPException(status_code=404, detail="Model not found")
        return model

    @router.get("/files/{path:path}")
    async def get_file(path: str):
        target_path = _validate_write_path(path, directory_path)
        if not target_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        return PlainTextResponse(content=target_path.read_text(encoding="utf-8"))

    @router.post("/files", status_code=201)
    async def create_file(request: FileCreateRequest):
        """Create a new file within the served directory."""
        target_path = _validate_write_path(request.path, directory_path)
        if target_path.exists():
            from fastapi import HTTPException

            raise HTTPException(status_code=409, detail="File already exists")
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(request.content, encoding="utf-8")
        return {"path": request.path}

    @router.put("/files/{path:path}")
    async def update_file(path: str, request: FileWriteRequest):
        """Update the content of an existing file."""
        target_path = _validate_write_path(path, directory_path)
        if not target_path.exists():
            from fastapi import HTTPException

            raise HTTPException(status_code=404, detail="File not found")
        target_path.write_text(request.content, encoding="utf-8")
        return {"path": path}

    @router.delete("/files/{path:path}", status_code=204)
    async def delete_file(path: str):
        """Delete a file within the served directory."""
        target_path = _validate_write_path(path, directory_path)
        if not target_path.exists():
            from fastapi import HTTPException

            raise HTTPException(status_code=404, detail="File not found")
        target_path.unlink()

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
        cache_key = relative_target(target_path, directory_path)
        options, written = _job_state_options(cache_key)
        cmd = _build_cmd("run", target_path, config_path, engine, options)
        background_tasks.add_task(
            run_subprocess,
            job,
            cmd,
            str(directory_path),
            lambda: _finish_job(cache_key, written),
        )
        return JobStatus(job_id=job.job_id, status=job.status, output=job.output, error=job.error)  # type: ignore[arg-type]

    @router.post("/refresh", response_model=JobStatus)
    async def refresh_target(
        request: JobRequest, background_tasks: BackgroundTasks
    ) -> JobStatus:
        """Refresh stale assets in a trilogy file or directory in a background subprocess."""
        target_path = _validate_target(request.target, directory_path)
        job = create_job()
        cache_key = relative_target(target_path, directory_path)
        options, written = _job_state_options(cache_key)
        cmd = _build_cmd("refresh", target_path, config_path, engine, options)
        background_tasks.add_task(
            run_subprocess,
            job,
            cmd,
            str(directory_path),
            lambda: _finish_job(cache_key, written),
        )
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

    @router.get("/state")
    async def get_state(
        target: str, response: Response, refresh: bool = False
    ) -> StateSnapshot:
        """Watermark, staleness and partition state for a trilogy file or directory.

        Returns a ``StateSnapshot`` — the interchange format shared with
        ``trilogy state -o``, ``run/refresh --state-file``, and the cloud
        service. A directory target is probed as a whole project, resolving
        cross-script ownership and deduplicating by physical address.

        **Served from an on-disk cache by default.** Computing this re-parses
        the target, builds an executor and re-probes the warehouse — seconds per
        call, and real money on a billed warehouse — so a cached snapshot is
        reused until a model file changes or a run/refresh job finishes. The
        cache lives in ``.trilogy/state`` under the served directory and so
        survives a server restart.

        That makes ``snapshot_ts`` load-bearing rather than informational: it is
        when the warehouse was actually observed, and a client showing state
        passively should surface it. The deliberate gap is a table loaded
        *outside* trilogy, which no server-side event can catch — pass
        ``refresh=true`` to force a re-probe.

        Cache status is reported in headers rather than in the body, so the body
        stays a verbatim ``StateSnapshot``:

        - ``X-Trilogy-Cached``: ``true`` / ``false``
        - ``X-Trilogy-Computed-At``: when the returned probe actually ran
        """
        import asyncio

        from click.exceptions import Exit

        target_path = _validate_target(target, directory_path)
        cache_key = relative_target(target_path, directory_path)

        fingerprint = ""
        if state_cache is not None:
            fingerprint = fingerprint_directory(directory_path)
            if not refresh:
                hit = state_cache.get(cache_key, fingerprint)
                if hit is not None:
                    response.headers["X-Trilogy-Cached"] = "true"
                    response.headers["X-Trilogy-Computed-At"] = hit.computed_at
                    return hit.snapshot

        loop = asyncio.get_event_loop()
        try:
            snapshot = await loop.run_in_executor(
                None,
                compute_state_snapshot_sync,
                target_path,
                engine,
                config_path,
                directory_path,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exit as e:
            # The directory probe exits the process on an unparseable script;
            # for a server that is a bad request, not a crash.
            raise HTTPException(
                status_code=400,
                detail=f"State probe failed for '{target}' (exit {e.exit_code}); "
                "check the scripts parse and their dialect is configured.",
            )
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"State computation failed: {e}"
            )
        computed_at = utc_now_iso()
        if state_cache is not None:
            state_cache.put(cache_key, snapshot, fingerprint, computed_at)
        response.headers["X-Trilogy-Cached"] = "false"
        response.headers["X-Trilogy-Computed-At"] = computed_at
        return snapshot

    app.include_router(router)

    if studio_bundle is not None:
        import mimetypes

        from fastapi.staticfiles import StaticFiles

        # DuckDB instantiates wasm by streaming, which rejects any other content
        # type; `mimetypes` doesn't register .wasm on every platform.
        mimetypes.add_type("application/wasm", ".wasm")

        # The bundle is built with an absolute vite base, so it only works
        # mounted at the path its manifest names. Unauthenticated on purpose:
        # a <script src> can't carry X-Trilogy-Token, and the token is there to
        # protect the store's files, not the studio's own assets.
        app.mount(
            studio_bundle.base_path.rstrip("/"),
            StaticFiles(directory=studio_bundle.directory, html=True),
            name="studio",
        )

    print(f"Starting Trilogy Model Server on http://{host}:{port}")
    print(f"Serving model '{directory_path.name}' from: {directory_path}")
    print(f"Engine: {engine}")
    print(f"Access the index at: http://{host}:{port}/index.json")
    print(
        f"Found {len(find_all_model_files(directory_path))} model files (.preql, .sql, .csv, .py)"
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
@option(
    "--auth-token",
    default=None,
    type=str,
    help="Use a specific auth token instead of generating one randomly",
)
@option(
    "--no-state-cache",
    is_flag=True,
    default=False,
    help="Recompute /state on every request instead of caching it under .trilogy/state",
)
@option(
    "--no-local-studio",
    is_flag=True,
    default=False,
    help="Link to the hosted studio instead of serving a local bundle",
)
@option(
    "--studio-bundle",
    default=None,
    type=Path(exists=True, file_okay=False, dir_okay=True),
    help="Serve an already-extracted studio bundle from this directory",
)
@option(
    "--studio-url",
    default=None,
    type=str,
    help=(
        "Link back to the studio already running at this address instead of "
        "serving a local bundle. Overrides [serve] studio_url in trilogy.toml."
    ),
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
    auth_token: str | None,
    no_state_cache: bool,
    no_local_studio: bool,
    studio_bundle: str | None,
    studio_url: str | None,
):
    """Start a FastAPI server to expose Trilogy models from a directory or file."""
    if not check_fastapi_available():
        print(
            "Error: FastAPI and uvicorn are required for the serve command.\n"
            "Please install with: pip install pytrilogy[serve]",
            file=sys.stderr,
        )
        sys.exit(1)

    if studio_url and studio_bundle:
        print(
            "Error: --studio-url and --studio-bundle are mutually exclusive. "
            "--studio-url names a studio hosted elsewhere; --studio-bundle "
            "serves one from this process.",
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
    resolved_studio_url = DEFAULT_STUDIO_URL
    project_name: str | None = None
    connection_type: Dialects | str | None = None
    connection_options: dict[str, str] = {}
    engine_config: DialectConfig | None = None
    startup_scripts: list[PathlibPath] = []
    if config_path:
        runtime_config = load_config_file(config_path)
        if runtime_config.engine_dialect and engine == "generic":
            engine = runtime_config.engine_dialect.value
        resolved_studio_url = runtime_config.serve_studio_url
        project_name = runtime_config.project_name
        if runtime_config.serve_connection:
            connection_type = runtime_config.serve_connection.type
            connection_options = runtime_config.serve_connection.options
        # Only advertisable when it describes the engine actually being served
        # — an explicit `--engine` that disagrees with `[engine] dialect` makes
        # the config's fields belong to a different database.
        if (
            runtime_config.engine_dialect
            and runtime_config.engine_dialect.value == engine
        ):
            engine_config = runtime_config.engine_config
        startup_scripts = runtime_config.startup_sql + runtime_config.startup_trilogy

    # An explicit address names a studio that is already running, so there is
    # nothing to gain from downloading and mounting a bundle — and the link has
    # to point at that studio, which only the hosted form does. Config's
    # studio_url keeps its narrower meaning (a fallback for --no-local-studio):
    # a file that has sat in a repo for months shouldn't silently turn off the
    # local bundle for everyone who runs `trilogy serve` in it.
    if studio_url:
        resolved_studio_url = studio_url
        no_local_studio = True

    if no_auth:
        token = None
        if host != "localhost" and host != "127.0.0.1":
            print(
                "WARNING: Authentication is disabled and the server is bound to "
                f"{host}. File read and write endpoints are accessible to anyone "
                "who can reach this host. Use --host localhost for local-only access.",
                file=sys.stderr,
            )
    elif auth_token:
        token = auth_token
    else:
        token = secrets.token_urlsafe(TOKEN_BYTES)

    resolved_bundle: StudioBundle | None = None
    if not no_local_studio:
        from trilogy.scripts.serve_helpers import (
            StudioBundleError,
            resolve_studio_bundle,
        )

        try:
            resolved_bundle = resolve_studio_bundle(
                explicit_directory=(
                    PathlibPath(studio_bundle) if studio_bundle else None
                ),
                on_progress=announce_studio_download,
            )
        except StudioBundleError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    app = FastAPI(title="Trilogy Model Server", version=__version__)
    create_app(
        app,
        engine,
        directory_path,
        host,
        port,
        token=token,
        config_path=config_path,
        project_name=project_name,
        connection_type=connection_type,
        connection_options=connection_options,
        engine_config=engine_config,
        startup_scripts=startup_scripts,
        enable_state_cache=not no_state_cache,
        studio_bundle=resolved_bundle,
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
        asset_name = get_relative_model_name(target_file, directory_path)
        display_model_name = project_name if project_name else directory_path.name

        if resolved_bundle is not None:
            studio_link = build_local_studio_link(
                base_url,
                resolved_bundle.base_path,
                asset_name,
                display_model_name,
                build_store_id(directory_path, project_name),
                token,
            )
        else:
            studio_link = build_hosted_studio_link(
                resolved_studio_url,
                f"{base_url}/models/{model_safe_name}.json",
                asset_name,
                display_model_name,
                "duckdb" if engine == "duck_db" else engine,
                base_url,
                token,
            )

        print("\n" + "=" * 80)
        print("Trilogy Studio Link:")
        print(studio_link)
        if resolved_bundle is None and not is_loopback_studio(resolved_studio_url):
            print(
                "(hosted studio — your browser may block it from reaching this "
                "local server; run with a studio bundle to serve it locally)"
            )
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
