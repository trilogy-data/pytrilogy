"""Serve command for Trilogy CLI."""

import sys
from pathlib import Path as PathlibPath

from click import Path, argument, option, pass_context


def check_fastapi_available() -> bool:
    """Check if FastAPI and uvicorn are available."""
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401

        return True
    except ImportError:
        return False


def create_app(app, engine: str, directory_path: PathlibPath, host: str, port: int):
    from fastapi import HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import PlainTextResponse

    from trilogy.scripts.serve_helpers import (
        ModelImport,
        StoreIndex,
        find_all_model_files,
        find_file_content_by_name,
        find_model_by_name,
        generate_model_index,
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

    @app.get("/")
    async def root():
        """Root endpoint with server information."""
        file_count = len(find_all_model_files(directory_path))
        return {
            "message": "Trilogy Model Server",
            "description": f"Serving model '{directory_path.name}' with {file_count} files from {directory_path}",
            "endpoints": {
                "/index.json": "Get list of available models",
                "/models/<model-name>.json": "Get specific model details",
            },
        }

    @app.get("/index.json", response_model=StoreIndex)
    async def get_index() -> StoreIndex:
        """Return the store index with list of available models."""
        return StoreIndex(
            name=f"Trilogy Models - {directory_path.name}",
            models=generate_model_index(directory_path, base_url, engine),
        )

    @app.get("/models/{model_name}.json", response_model=ModelImport)
    async def get_model(model_name: str) -> ModelImport:
        """Return a specific model by name."""
        model = find_model_by_name(model_name, directory_path, base_url, engine)
        if model is None:
            raise HTTPException(status_code=404, detail="Model not found")
        return model

    @app.get("/files/{file_name}")
    async def get_file(file_name: str):
        """Return the raw .preql or .sql file content."""
        content = find_file_content_by_name(file_name, directory_path)
        if content is None:
            raise HTTPException(status_code=404, detail="File not found")
        return PlainTextResponse(content=content)

    print(f"Starting Trilogy Model Server on http://{host}:{port}")
    print(f"Serving model '{directory_path.name}' from: {directory_path}")
    print(f"Engine: {engine}")
    print(f"Access the index at: http://{host}:{port}/index.json")
    print(
        f"Found {len(find_all_model_files(directory_path))} model files (.preql, .sql, .csv)"
    )
    return app


@argument("path", type=Path(exists=True, file_okay=True, dir_okay=True))
@argument("engine", type=str, required=False, default="generic")
@option("--port", "-p", default=8100, help="Port to run the server on")
@option("--host", "-h", default="0.0.0.0", help="Host to bind the server to")
@option("--timeout", "-t", default=None, type=float, help="Shutdown after N seconds")
@pass_context
def serve(ctx, path: str, engine: str, port: int, host: str, timeout: float | None):
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

    # Load trilogy.toml to get engine dialect if not explicitly provided
    from trilogy.execution.config import load_config_file
    from trilogy.scripts.common import find_trilogy_config

    config_path = find_trilogy_config(directory_path)
    if config_path and engine == "generic":
        try:
            runtime_config = load_config_file(config_path)
            if runtime_config.engine_dialect:
                engine = runtime_config.engine_dialect.value
        except Exception:
            pass

    # Use localhost instead of 0.0.0.0 in URLs so they resolve properly
    from trilogy.scripts.serve_helpers import (
        find_trilogy_files,
        get_relative_model_name,
        get_safe_model_name,
    )

    app = FastAPI(title="Trilogy Model Server", version=__version__)

    create_app(app, engine, directory_path, host, port)

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
        # The model URL uses the directory name (not the file name)
        model_safe_name = get_safe_model_name(directory_path.name)
        model_url = f"{base_url}/models/{model_safe_name}.json"
        store_url = base_url

        # The asset name is the specific file within the model
        asset_name = get_relative_model_name(target_file, directory_path)

        # URL-encode the parameters
        from urllib.parse import quote

        studio_url = (
            f"https://trilogydata.dev/trilogy-studio-core/#"
            f"import={quote(model_url)}&"
            f"assetType=trilogy&"
            f"assetName={quote(asset_name)}&"
            f"modelName={quote(directory_path.name)}&"
            f"connection={quote(engine)}&"
            f"store={quote(store_url)}"
        )

        print("\n" + "=" * 80)
        print("Trilogy Studio Link:")
        print(studio_url)
        print("=" * 80 + "\n")

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
